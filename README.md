# ForgeGrid

ForgeGrid is a Go distributed job execution engine with static DAG pipelines. The execution/recovery backbone, concurrency milestone, and Milestone 3 pipeline semantics follow [architecture-v0.1](docs/architecture-v0.1.md): PostgreSQL owns coordination state, outbound gRPC connects workers, and Docker executes trusted local jobs.

Execution is **at least once physically**, with a single authoritative terminal winner. Killing an agent can leave its Docker job container alive. Lease expiry and fencing prevent that old attempt from finalizing the logical job.

## Run locally

Requirements: Docker Desktop with its Linux engine running, Docker Compose, and PowerShell 7. Host Go and protoc are optional; development tools run in Docker. Ports 5432, 8080, and 9090 must be available. Initial builds require internet access.

From the repository root:

```powershell
./scripts/demo-normal.ps1
./scripts/demo-recovery.ps1
./scripts/demo-pipeline.ps1
./scripts/demo-pipeline-recovery.ps1
./scripts/demo-observability.ps1
```

The normal demo builds the stack, registers worker-a/b/c, submits an argv job, and asserts PostgreSQL-backed success and both log streams. The recovery demo temporarily stops a/c, starts a fresh worker-b session, kills worker-b with SIGKILL, starts c, observes the offline session while attempt #1 still owns the job, waits for lease expiry, verifies LOST -> attempt #2 on c -> SUCCEEDED, and replays the old result through gRPC. It must print `STALE_ATTEMPT_REJECTED` and `WORKER_B_TO_WORKER_C_RECOVERY_PASSED`. Both demos use the existing local database; run them when no other jobs are being submitted. The recovery demo restores all three workers afterward.

Start the stack without submitting a demo:

```powershell
docker compose up -d --build --wait postgres control-plane worker-a worker-b worker-c
```

Submit a job:

```powershell
$body = @{ image = 'alpine:3.22'; command = @('echo', 'hello ForgeGrid'); timeout_seconds = 30; max_attempts = 2 } | ConvertTo-Json
$job = Invoke-RestMethod http://localhost:8080/api/v1/jobs -Method Post -ContentType application/json -Body $body
Invoke-RestMethod "http://localhost:8080/api/v1/jobs/$($job.id)"
Invoke-RestMethod http://localhost:8080/api/v1/workers
```

Read attempt logs with `GET /api/v1/attempts/{attempt_id}/logs?after=0`. Chunks are ordered by sequence; JSON payloads are base64-encoded bytes. Responses contain at most 1,000 chunks; paginate using the last sequence. The executor directly uses argv with the image entrypoint overridden by argv[0]. A shell is used only when explicitly requested in argv, as in the demo's stdout/stderr command. Jobs have no network, host mounts, Docker socket, privileged mode, or host namespaces.

Stop services while retaining PostgreSQL data:

```powershell
docker compose down
```

## Verify

Pipeline demos show build → unit-test/lint in parallel → package, both success and permanent branch failure with package SKIPPED. The pipeline recovery demo kills worker-b during test, retains package BLOCKED through lease expiry/retry, rejects stale replay, and proves worker-c success releases package. Like the original recovery demo, it restores all workers and should run without concurrent submissions.

```powershell
./scripts/verify.ps1
./scripts/verify-e2e.ps1
./scripts/verify-recovery.ps1
./scripts/demo-recovery.ps1
./scripts/test-leaseguard.ps1
```

`verify.ps1` checks gofmt, go vet, compilation, unit tests, and race-enabled integration tests against real PostgreSQL. Integration tests use a unique schema per test and remove only that schema. Explicitly requested integration tests fail if `TEST_DATABASE_URL` is missing. `verify-e2e.ps1` tests real Docker execution, stdout/stderr persistence, duplicate result/log delivery, stale fencing, nonzero exit, invalid command, and timeout. The lease-guard script stops the Control Plane, proves the old job container stops without renewal ACKs, then checks recovery after restart.

`verify-recovery.ps1` mounts the Docker socket into the development test runner and tests real process deaths at exact assignment/start windows, surviving and overlapping Docker executions, orphan reconciliation, and internal cancellation. It uses isolated database schemas and unique worker/container labels. All verification scripts disable Go test result caching. See the [Gate C audit and coverage matrix](docs/correctness-audit.md) for deterministic transaction races and injected rollback cases.

With a local Go toolchain:

```powershell
go test ./...
$env:TEST_DATABASE_URL = 'postgres://forgegrid:forgegrid@localhost:5432/forgegrid?sslmode=disable'
go test -race -tags integration ./...
# Requires the running Compose stack:
go test -race -tags e2e ./tests/e2e
```

Regenerate the committed Protobuf Go bindings:

```powershell
./scripts/generate-proto.ps1
```

## Implementation

Milestone 5 adds a React/TypeScript/Vite operational console: worker/session capacity, pipeline DAG inspection, immutable attempt history and fencing/lease details, persisted recovery chronology, live resumable SSE output, existing cancellation and static pipeline submission. PostgreSQL remains authoritative. See [console APIs, semantics and verification](docs/operational-console-audit.md).

```powershell
docker compose --profile console up -d --build --wait postgres control-plane worker-a worker-b worker-c console
# Console: http://localhost:5173
./scripts/verify-frontend.ps1
```

The frontend verification script runs strict checks, production build, component/browser tests and real worker-b → worker-c browser recovery. Run it exclusively, without other submissions/recovery demos. For Vite development, stop the console service, then run `npm ci` and `npm run dev` in `frontend` with the backend running. State views poll; attempt output uses SSE and native sequence-based reconnect, with bounded browser memory. Exact worker offline-transition timestamps are not persisted and are not invented by the UI. DRAINING is explicitly unsupported.

Milestone 4 adds distributed OpenTelemetry tracing through a Collector to Jaeger, bounded-cardinality Prometheus metrics, and correlated JSON operational logs. Run ./scripts/demo-observability.ps1 for the verified normal/recovery trace URLs; inspect Jaeger at http://localhost:16686 and Prometheus at http://localhost:9092. ./scripts/verify-observability.ps1 checks execution with backends absent or stopped mid-job. Telemetry remains optional for execution. See [observability semantics and evidence](docs/observability-audit.md).

Pipeline HTTP submission uses POST /api/v1/pipelines with a jobs array; each entry adds key and dependencies to the existing image/argv/timeout/max_attempts spec. GET /api/v1/pipelines/{id} returns the DAG's jobs and attempt histories. POST /api/v1/jobs/{id}/cancel and POST /api/v1/pipelines/{id}/cancel persist cancellation before best-effort worker notification. See [pipeline semantics and verification](docs/pipeline-semantics-audit.md) for exact timeout, cancellation, retry, and final-state rules.

- `cmd/controlplane`, `internal/controlplane`: HTTP/gRPC gateway, transactional scheduling, heartbeat detection, and lease recovery in one process.
- `cmd/worker`, `internal/worker`: process session UUID, heartbeats, renewal requests, monotonic lease guard, Docker CLI executor, log collector, and startup reconciliation.
- `internal/domain`: ownership checks, specification validation, result classification, and capped infrastructure retry policy.
- `internal/store/postgres`, `db/migrations`: explicit pgx SQL, row locks, durable attempts, capacity, duplicate-safe logs, and transactional DAG coordination. Embedded migrations run under a PostgreSQL advisory lock; 002 adds cancellation and 003 adds pipelines/dependencies and execution deadlines. Seven tables; upgrade/reapplication tested.
- `api/proto/forgegrid/v1`, `gen/go`: versioned contracts and generated bindings.
- `tests`, `scripts`: Docker end-to-end checks, diagnostic result replay, and repeatable demos.

Development defaults: heartbeat 2s, offline threshold 6s, execution lease 10s, renewal interval 2s, retry base 1s. Set `HEARTBEAT_INTERVAL`, `OFFLINE_THRESHOLD`, `EXECUTION_LEASE`, `LEASE_RENEW_INTERVAL`, and `RETRY_BASE` before Compose startup to override them. Each worker session has capacity one. `max_attempts` defaults to two and is bounded to 1..10. Infrastructure retry delay doubles and caps at 30s. Timeout defaults to 30s and is bounded to 1..86,400 seconds.

See [execution semantics](docs/execution-semantics.md), [failure model](docs/failure-model.md), [protocol](docs/protocol.md), and [implementation decisions](docs/implementation-v0.1.md).

## Limits of this slice

One Control Plane, no HA; static DAGs are bounded to 128 jobs/2,048 edges. No authentication or TLS: console/HTTP/gRPC are for trusted local development with loopback host bindings. Workers require Docker daemon access; this is not a hostile multi-tenant sandbox. Workload containers are non-privileged. Pipeline coordination is deliberately serialized; no scale claim is made. Control Plane/workers upgrade together for execution-budget ACKs.

A process crash can leave a physical container running. Startup reconciliation removes older-session containers for the same worker identity on the accessible daemon; inaccessible machines cannot be remotely cleaned up. Local cancellation attempts Docker removal with a bounded timeout and logs failures, so physical shutdown cannot be guaranteed when the daemon is unavailable. A permanently lost worker's container may require manual removal using its ForgeGrid labels.

Log delivery has a bounded in-memory queue/retries with persistence ACKs. Crashes/cancellation can lose unacknowledged logs; there is no durable spool or total storage retention policy. Browser SSE reads persisted logs; old immutable attempt logs remain diagnostic. Tracing queues and local telemetry retention are bounded; backend outages/crashes may lose spans. Metrics history aggregation has overhead and no scale claim. Submission idempotency keys, DAG mutation, dynamic DAGs, matrix, expressions, fail-fast, artifacts, authentication, secrets, and integrations remain deferred. Public job/pipeline cancellation and the operational console are implemented.
