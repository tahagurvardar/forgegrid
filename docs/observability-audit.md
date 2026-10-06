# Milestone 4: observability audit

This records Milestone 4 decisions. Later console support and M6 exporter/regression corrections are documented in [operational-console-audit.md](operational-console-audit.md) and [final-engineering-audit.md](final-engineering-audit.md). Current complete verification is in [release-audit.md](release-audit.md).

Source of truth: architecture-v0.1.md, implementation-v0.1.md, correctness-audit.md, pipeline-semantics-audit.md, and the existing protocol/execution/failure documents. Started from clean, pushed `9080fb96d84065bf79c7ee9d9e877aea58921597`.

**Observability is not part of the authoritative state machine.** PostgreSQL, ownership checks, pipeline → ordered jobs → attempts → worker-session locks, fencing, session incarnation, ACK-gated leases, deadlines, bounded retries, cancellation, and DAG decisions remain authoritative. No exporter acknowledgement is required for any transition.

## Findings before implementation

1. Both production binaries already used JSON slog. Registration, dispatch, result, connection loss, lease guard, and cleanup had records; correlation was incomplete. Workload output already had a separate sequenced PostgreSQL-backed stream.
2. Useful span boundaries are submission/cancellation, eligible claim/attempt creation, dispatch, receipt/acceptance, execution/Docker operations, authoritative completion, renewal, lease-expiry recovery/retry, and dependency propagation/aggregation. Instrumenting every query or every heartbeat would obscure these boundaries.
3. Scheduling is asynchronous and stream connections outlive jobs. Persist a small W3C traceparent with job/attempt diagnostic metadata; propagate it in assignment and individual control/result messages. Do not retain request contexts or make the connection the execution parent.
4. State gauges and committed lifecycle/history counters can come from read-only PostgreSQL snapshots; assignment latency and rejected stale messages are local observations. Worker physical executor outcomes are distinct from accepted authoritative results.
5. Pipeline/job/attempt/session/container IDs, worker IDs, argv, images, URLs, trace IDs, and error text must not become application metric labels. Fixed result/reason categories are sufficient.
6. Operational logs need full attempt/pipeline/job/session/fence correlation and trace/span IDs where available. Neither argv/environment nor stdout/stderr should be copied into operational metadata.
7. Existing transitions can carry diagnostic metadata and lightweight spans without changing decisions, transaction boundaries, or lock acquisition order. Metrics/export must be isolated from ownership paths. No architectural blocker or new product feature was required.

## Tracing and asynchronous propagation

The Go OpenTelemetry SDK exports HTTP/protobuf OTLP to the Collector, which forwards OTLP/gRPC to Jaeger. Resource service names are forgegrid-controlplane and forgegrid-worker. Local development samples all traces. Empty OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_TRACES_EXPORTER=none disables export; diagnostic context generation still works.

HTTP mutations extract W3C context, start http.request, return X-ForgeGrid-Trace-ID, and end at the response. Health, inspection, and scrape polling do not generate traces. Submission ends immediately after persistence, rather than holding a request/span open for execution.

Migration 004 adds optional diagnostic job/attempt trace_parent plus queue timing columns. Application-generated contexts contain only traceparent: no baggage, credentials, tracestate, or full request headers. Empty, oversized, or malformed propagated context is ignored; it cannot change cancellation/deadlines on the existing Go context. These columns are never consulted by ownership/readiness predicates. No new authoritative tables are added. Legacy rows receive empty context and nullable timing; the next transition can start a new trace.

Each scheduler claim resumes job context. scheduler.create_attempt creates and persists a distinct span context per immutable attempt, after eligible worker selection. grpc.dispatch_assignment resumes that context and sends trace_parent and pipeline_id in RunAttempt. Its queued_for_delivery attribute means enqueued to the stream, not proof of Send or worker acceptance. Worker receipt and acceptance are separate spans. Individual WorkerMessage and unary AttemptResult contexts correlate renewals/completions independently of the long-lived Connect stream; that stream has no umbrella job parent.

worker.execute_attempt surrounds the executor and log draining; Docker create/start/wait are child operations. Context decoration preserves the existing monotonic deadlines and cancellation. worker.report_completion records enqueue, not result-ACK latency. The Control Plane completion span checks ownership and commits or rejects exactly as before. Repeated report messages use cloned protobuf results, so diagnostic context updates cannot race a previous Send.

Recovery resumes the expired attempt context. recovery.retry_job identifies the bounded retry decision; the job carries that context to the next claim. Retry attempts share the trace ID while keeping separate span/attempt IDs, numbers, worker sessions, and fences. Final-parent dependency release passes its context to newly queued children, preserving a causal chain through the DAG. Concurrent parents already share the pipeline trace; fan-in uses the transition that made all dependencies successful, rather than fabricating one parent for every predecessor. Separate cancellation HTTP requests have separate traces correlated by pipeline/job attributes.

Named spans cover pipeline.submit, pipeline.cancel, job.cancel, scheduler.claim_job, scheduler.select_worker, scheduler.create_attempt, grpc.dispatch_assignment, worker.receive_assignment, worker.accept_assignment, worker.execute_attempt, docker.create/start/wait, worker.report_completion, controlplane.complete_attempt, lease.renew, recovery.expire_attempt, recovery.retry_job, dag.release_dependencies, dag.propagate_skip, and pipeline.finalize. Worker renewal enqueue also has worker.request_lease_renewal.

Ownership spans obtain persisted context/attributes after lock acquisition; they do not trace every query or claim to measure every lock wait. Assignment histogram includes the transaction/lock acquisition. Span attributes include correlation IDs, attempt number, fence, result/failure category, affected job count, and commit outcome. Nested DAG/retry spans describe tentative transaction work: consult the enclosing operation's committed flag. A rollback can produce diagnostic spans without a committed transition. pipeline.finalize may show RUNNING/CANCELLING aggregation as well as terminal results. A crashed worker may never export its execution span's end; Control Plane recovery spans provide the durable-state explanation.

## Metric catalog and cardinality

Control Plane samples committed PostgreSQL state every two seconds using a separate one-connection pool, read-only REPEATABLE READ transaction, one-second statement/sample timeout, and an immutable cached snapshot. Scraping never queries PostgreSQL or acquires ownership locks. On sampling failure the previous snapshot remains, snapshot_success becomes zero, and snapshot_timestamp_seconds exposes its age. No sampling error reaches the scheduler or completion path.

| Metric (forgegrid_ prefix) | Meaning / labels |
|---|---|
| workers_online / workers_offline | Latest incarnation per logical worker; superseded/historical sessions do not inflate current liveness. |
| jobs_queued / jobs_blocked / jobs_running | Current logical state counts; running excludes DISPATCHED/CANCELLING. |
| attempts_started_total | Persisted started_at, not inferred physical Docker activity. |
| attempts_completed_total{result} | Committed terminal attempts, including LOST/recovery, once per row. |
| attempt_retries_total{reason} | Created retry attempts classified by the immediately preceding attempt's failure. A pending retry decision is not yet a created retry. |
| lease_expirations_total | LEASE_EXPIRED infrastructure outcomes, including cancelled-job LOST attempts. Timeout-first recovery remains JOB_TIMEOUT, visible in TIMED_OUT totals. |
| stale_results_rejected_total | Rejected stale ownership result messages; process-local, may count repeated rejection deliveries. |
| pipeline_submissions_total | Persisted pipeline rows. |
| pipeline_completions_total{result} | Committed terminal pipelines. |
| scheduler_assignment_duration_seconds | Histogram of successful assignment transactions, including lock acquisition; process-local. |
| queue_wait_duration_seconds | Histogram from last QUEUED entry to attempt creation; excludes BLOCKED and RETRY_WAIT time. |
| submission_to_assignment_duration_seconds | Histogram including dependency/backoff/prior-attempt wait, for every attempt. |
| attempt_duration_seconds | Terminal attempt duration from started_at, or assigned_at when never started; includes reporting/recovery latency. |
| log_bytes_total | Persisted unique log chunk bytes; duplicate delivery adds zero. |
| metrics_snapshot_success / metrics_snapshot_timestamp_seconds | Cached database sample health/age. |
| worker_executions_total{result} | Worker-local physical executor outcomes, including cleanup/drain; not authoritative completion. |
| worker_lease_guard_expirations_total | Worker-local monotonic guard expirations. |

Result labels are closed: SUCCEEDED, FAILED, TIMED_OUT, CANCELLED, LOST; worker unknown outcomes map to OTHER. Pipeline result is SUCCEEDED/FAILED/CANCELLED. Retry reason is LEASE_EXPIRED/EXECUTOR_INFRA_ERROR/ASSIGNMENT_REJECTED/OTHER. Histogram buckets are fixed. No IDs/error strings become application labels. Prometheus adds its normal bounded local job/instance target labels for one Control Plane and three workers.

History-derived counters/histograms survive application restart and do not count rolled-back writes or duplicate delivery. They reflect retained database history, not an event bus: deliberate deletion/retention would reset/reduce them. Local counters reset on process restart. Metric snapshots are eventually visible, not transactional client observations. Queue timing is diagnostic and does not affect eligibility. Legacy claims without last_queued_at fall back to created_at; existing historical attempts with no queue timing are excluded from that histogram.

## Structured logging

Production binaries consistently use JSON slog with service.name. Attempt commit/dispatch/execution/accepted result/rejection/recovery/lease-guard records include job_id, attempt_id, attempt_number, worker_id, worker_session_id, fencing_token, pipeline_id when present, and trace_id/span_id when available. Pipeline submission and cancellation records include their IDs and trace fields. Lease renewals/heartbeats are not per-tick operational log spam. Exact duplicate winners do not produce another authoritative-finalization log.

Workload stdout/stderr remains only in the attempt log stream. No argv, environment dumps, payloads, or arbitrary failure_detail are added to operational logs or spans. Rejection and cleanup use bounded categories; Docker error span status is generic. Export errors produce a generic warning at most once per 30 seconds. Trace IDs and operational logs are diagnostic, never authority.

## Failure isolation and local stack

No core service has depends_on for telemetry. SDK exporter construction does not dial a backend. A non-blocking BatchSpanProcessor queue holds at most 1,024 spans, batches at most 256, flushes after 200ms, and uses one-second export timeout with exporter retries disabled. Full queues drop telemetry rather than blocking span producers. Shutdown is bounded to two seconds and occurs outside ownership handling.

Collector memory/queue/retry are bounded; Jaeger uses transient in-memory storage; Prometheus keeps at most one day/128MB locally. They have independent container memory limits. No telemetry data directory is mounted from the repository. Backend restarts can lose telemetry. There is no durable telemetry spool, HA, Grafana, or scale claim. Shared CPU/PostgreSQL still have instrumentation/sampling overhead; this is failure isolation, not a zero-overhead claim. History aggregation may need a later measured optimization as retention grows.

Jaeger UI: http://localhost:16686. Prometheus: http://localhost:9092. Control Plane metrics: http://localhost:8080/metrics. Worker metrics are internal Compose targets on 9091. OTLP is internal to Compose, with no public host binding.

```powershell
./scripts/demo-observability.ps1
./scripts/verify-observability.ps1
```

Run demos without concurrent submissions. The observability demo runs build → unit-test/lint → package, obtains its trace ID from the HTTP response, and queries Jaeger to require the expected spans and both services. It runs the existing fenced worker-b → worker-c DAG recovery proof, then requires expiry/retry/attempt-2/worker-c spans, persistent metric increments, and four healthy scrape targets. It prints exact clickable trace URLs. Inspect forgegrid_workers_offline around the kill in a Prometheus historical range; the gauge returns to online when the worker restarts.

The failure-isolation script starts/recreates core services with all three backends stopped, then separately stops Collector, Jaeger, and Prometheus during real running jobs. Each job must succeed with one attempt. It restores the telemetry stack. Run the demo afterward to leave inspectable traces in the transient Jaeger store.

## Test evidence and verification

- Propagation units preserve cancellation and reject malformed context; a Protobuf round-trip carries scheduling trace and attempt identity.
- Exporter units exercise connection refusal, a hanging HTTP backend, export timeout, and queue pressure without blocking span producers.
- PostgreSQL tests prove retry/dependency trace correlation after the submission span ended; successful/LOST attempts, retries, stale rejection, pipeline terminal counts, duplicate logs/completions, and closed label sets.
- Deferred commit faults do not increment committed lifecycle metrics. Scrape serves its cached snapshot even after the coordination pool closes. Queue timing excludes dependency/retry wait.
- An unavailable exporter and deliberately malformed diagnostic metadata leave fencing, slot accounting, and successful DAG finalization intact.
- Existing correctness tests retain their assertions. The legacy migration test now seeds via original-schema SQL because the current Submit API runs after startup migrations; upgrade/reapplication/data-preservation assertions remain.
- Real Docker demo and backend-loss script complement existing execution, pipeline, lease-guard, and process-crash suites.

Completed observability verification is recorded in the [final engineering audit](final-engineering-audit.md), and the release matrix is recorded in [release-audit.md](release-audit.md). Observability does not strengthen physical execution guarantees: crashes can leave Docker containers running, physical attempts may overlap, and fences protect PostgreSQL results rather than workload side effects. Frontend, authentication, integrations, brokers, artifacts, secrets, autoscaling, and unrelated infrastructure were deferred at Milestone 4.

Milestone 5 now adds the operational console and links valid persisted attempt trace IDs to local Jaeger. It does not depend on Jaeger or Prometheus availability. The preceding frontend-deferred statement records Milestone 4 scope; see [console semantics](operational-console-audit.md) for the implemented inspection layer.
