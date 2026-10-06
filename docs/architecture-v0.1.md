# ForgeGrid Architecture Specification v0.1

**Status:** Proposed → implementation-ready after architecture review
**Primary goal:** Distributed execution correctness under worker failure
**Non-goal:** Full GitHub Actions / GitLab CI replacement

**Release reading guide (v1.0.0 preparation):** This is the original design specification, preserved with its development gates and conceptual examples. Milestones 1–6 implement and audit the core execution, DAG, observation and console behavior. Runtime details are defined by committed migrations/contracts and [execution semantics](execution-semantics.md); current verified scope, deferred features and final checks are indexed in [release-audit.md](release-audit.md). Conceptual DRAINING, submission idempotency and later roadmap sections are not implemented API guarantees.

---

## 1. Product Definition

ForgeGrid is a distributed job execution engine with CI-style static DAG pipelines.

A client submits a pipeline consisting of one or more jobs. The Control Plane persists the pipeline, determines runnable jobs, selects eligible workers, creates execution attempts with renewable leases, and dispatches them over gRPC.

Workers execute jobs inside Docker containers, stream stdout/stderr back to the Control Plane, renew their leases while execution remains valid, and report terminal results.

When a worker disappears, ForgeGrid detects the loss of liveness, waits for authoritative execution ownership to expire, marks the old attempt lost, applies retry policy, creates a new attempt with a higher fencing token, and assigns it to another worker.

The defining property of ForgeGrid is therefore not:

> "It can run Docker commands remotely."

It is:

> "It can reason about, enforce, and recover distributed job ownership when workers fail at inconvenient moments."

---

# 2. V1 Engineering Guarantees

ForgeGrid V1 provides:

| Property                     | Guarantee                                 |
| ---------------------------- | ----------------------------------------- |
| Execution delivery           | At-least-once                             |
| Authoritative job completion | Single winner                             |
| Worker failure detection     | Heartbeat-based                           |
| Execution ownership          | Renewable lease                           |
| Stale worker protection      | Attempt identity + fencing token          |
| Retry count                  | Bounded                                   |
| Scheduler claims             | Transactional                             |
| Final state                  | Durable in PostgreSQL                     |
| Worker communication         | Long-lived outbound gRPC                  |
| Execution                    | Docker                                    |
| Logs                         | Ordered per attempt, replayable           |
| Tracing                      | Control Plane → Worker distributed traces |
| Metrics                      | Prometheus                                |
| Recovery                     | Lease-expiry based                        |

ForgeGrid does **not** claim exactly-once physical execution.

Two physical attempts may temporarily overlap under certain failure conditions.

ForgeGrid guarantees instead that only the current authoritative attempt may mutate the logical job's terminal state.

---

# 3. Core Invariants

These invariants are considered part of the architecture contract.

**I1 — One authoritative attempt**

A logical job has at most one `current_attempt_id`.

**I2 — Attempts are immutable identities**

Retries never reuse an attempt.

```text
Job 42
 ├── Attempt 1
 ├── Attempt 2
 └── Attempt 3
```

**I3 — Fencing is monotonic**

Each new attempt receives a fencing token greater than the previous attempt.

```text
attempt 1 → fence 1
attempt 2 → fence 2
attempt 3 → fence 3
```

**I4 — Stale attempts cannot finalize jobs**

A terminal result is accepted only when:

```text
attempt.id == job.current_attempt_id

AND

attempt.fencing_token == job.fencing_token

AND

attempt is still active

AND

lease has not expired
```

**I5 — Retry ownership requires expired old ownership**

ForgeGrid does not create a new authoritative attempt merely because a heartbeat disappeared.

The previous attempt's lease must cease to be valid.

**I6 — Worker heartbeat and job lease are different concepts**

Heartbeat answers:

> Is this worker session alive?

Lease answers:

> Is this execution attempt still authorized?

One does not implicitly replace the other.

**I7 — Retry count is always bounded**

There is no infinite retry state.

**I8 — Infrastructure failure and workload failure are distinct**

```text
worker disappeared
Docker daemon error
assignment lost
lease expired
```

are infrastructure failures.

```text
process exit code 1
test failure
command failure
```

are workload failures.

They do not automatically share the same retry policy.

**I9 — Terminal transitions are idempotent**

Duplicate completion messages must not produce duplicate side effects.

**I10 — PostgreSQL is authoritative**

gRPC connection state, worker memory and frontend state never override persisted coordination state.

---

# 4. System Topology

```text
                       ┌───────────────────────┐
                       │ React / TypeScript UI │
                       └──────────┬────────────┘
                                  │
                              HTTP / SSE
                                  │
                                  ▼
┌──────────────────────────────────────────────────────┐
│                    CONTROL PLANE                     │
│                                                      │
│ HTTP API                                             │
│ Worker gRPC Gateway                                  │
│ Scheduler                                            │
│ Recovery / Lease Manager                             │
│ DAG Coordinator                                      │
│ Log Ingestion                                        │
│ Observability                                        │
│                                                      │
└───────────────┬──────────────────────────────┬───────┘
                │                              │
                ▼                              │
          PostgreSQL                           │ gRPC
                                               │
                       ┌───────────────────────┼───────────┐
                       │                       │           │
                       ▼                       ▼           ▼
                   Worker A                Worker B    Worker C
                       │                       │           │
                       ▼                       ▼           ▼
                    Docker                  Docker       Docker
                       │                       │           │
                       ▼                       ▼           ▼
                 Job Containers         Job Containers Job Containers
```

Observability:

```text
Control Plane ───┐
                 ├── OTLP → OTel Collector → Jaeger
Workers ─────────┘

Control Plane ───┐
                 ├── /metrics → Prometheus
Workers ─────────┘
```

---

# 5. Control Plane Boundaries

The Control Plane contains several logical modules but remains **one Go binary in V1**.

It is not split into microservices.

### API

Responsible for:

```text
pipeline submission
pipeline inspection
job inspection
attempt inspection
worker inspection
cancellation
log retrieval
```

The public API uses HTTP/JSON.

### Scheduler

Responsible for:

```text
finding QUEUED jobs
finding eligible worker sessions
reserving worker capacity
creating attempts
incrementing fencing tokens
dispatching assignments
```

The Scheduler never executes jobs.

### Worker Session Manager

Responsible for:

```text
gRPC connections
registration
session incarnation
heartbeats
worker capacity
connection lifecycle
control messages
```

### Recovery Manager

Responsible for:

```text
expired leases
lost attempts
retry decisions
retry scheduling
worker liveness transitions
```

### DAG Coordinator

Responsible for:

```text
dependency validation
unblocking jobs
downstream skip propagation
pipeline terminal status
```

### Storage

All authoritative coordination changes go through PostgreSQL transactions.

---

# 6. Worker Architecture

Each worker is one Go process.

Conceptually:

```text
Worker Agent

├── Control Stream
├── Heartbeat Loop
├── Lease Manager
├── Assignment Manager
├── Executor
│     └── Docker Executor
├── Log Collector
├── Result Reporter
└── Local Reconciliation
```

Workers never decide which job they should execute.

They execute assignments issued by the Control Plane.

---

# 7. Worker Identity Model

ForgeGrid distinguishes between:

```text
Worker
```

and:

```text
Worker Session
```

A worker represents a logical machine/agent identity.

Example:

```text
worker-b
```

A session represents one process incarnation.

Example:

```text
worker_id  = worker-b
session_id = 7da8c...
```

If the worker restarts:

```text
worker_id  = worker-b
session_id = b94e2...
```

The session changes.

This protects ForgeGrid from delayed messages originating from an older worker process.

Attempts reference the worker **session**, not merely the worker name.

---

# 8. Worker Session Lifecycle

```text
REGISTERING
      │
      ▼
    ONLINE
      │
      ├─────────► DRAINING
      │                │
      │                ▼
      │             OFFLINE
      │
      ├─────────► OFFLINE
      │
      └─────────► SUPERSEDED
```

`SUPERSEDED` occurs when another process registers a newer session for the same worker identity.

Messages from a superseded session must no longer renew leases.

They may still be recorded diagnostically.

---

# 9. Heartbeat Model

Initial V1 values:

```text
heartbeat interval     2 seconds
offline threshold      6 seconds
attempt lease         10 seconds
```

All values remain configurable.

The Control Plane uses **its own receipt time**.

It does not trust worker timestamps for liveness calculations.

Heartbeat:

```text
Worker
   │
   │ Heartbeat(session_id)
   ▼
Control Plane
   │
   └── updates worker_sessions.last_seen_at
```

Heartbeat does not renew job leases.

---

# 10. Lease Model

Every active attempt has:

```text
lease_expires_at
```

The worker periodically requests renewal.

Conceptually:

```text
Worker
   │
   │ RenewLease(attempt_id, fencing_token)
   ▼
Control Plane
   │
   │ validate ownership
   │ extend DB lease
   ▼
Worker
   │
   └── receives LeaseRenewed
```

Only a successful Control Plane ACK renews local execution authority.

A worker must not assume:

> "I sent the renewal, therefore my lease is renewed."

---

# 11. Worker-Side Lease Guard

The worker also enforces the lease locally.

After receiving a renewal ACK, it starts or extends a **monotonic local timer**.

The worker does not depend on synchronized wall clocks.

Conceptually:

```text
LeaseRenewed(ttl = 10s)
          │
          ▼
local monotonic deadline
          │
          │ no further renewal ACK
          ▼
executor cancellation
          │
          ▼
Docker stop / kill
```

This behavior is especially important during network partitions.

---

# 12. Important Limitation: Worker Process Crash

There is an important physical limitation.

When a worker uses the Docker daemon to create a container, the Docker daemon owns that container.

If the **worker process itself crashes**, its local lease guard disappears.

The job container may continue running.

Therefore ForgeGrid cannot claim:

> Lease expiration guarantees that the old physical process has stopped.

What ForgeGrid can guarantee is:

> The old attempt can no longer produce an authoritative result.

All job containers receive labels such as:

```text
forgegrid.worker_id
forgegrid.worker_session_id
forgegrid.job_id
forgegrid.attempt_id
forgegrid.fencing_token
```

When a worker starts a new session, it performs local reconciliation and cleans up containers left by older sessions belonging to that worker identity.

If an entire worker machine disappears permanently, ForgeGrid cannot remotely kill a process running on that inaccessible machine.

This is another reason V1 explicitly provides at-least-once rather than exactly-once execution semantics.

---

# 13. Job Lifecycle

Logical job states:

```text
BLOCKED
   │ dependencies satisfied
   ▼
QUEUED
   │
   │ attempt created
   ▼
DISPATCHED
   │
   │ worker starts execution
   ▼
RUNNING
   │
   ├──────────────► SUCCEEDED
   │
   ├──────────────► FAILED
   │
   ├──────────────► CANCELLING ───► CANCELLED
   │
   └── infra loss
          │
          ▼
      RETRY_WAIT
          │
          ├── attempts remain ───► QUEUED
          │
          └── exhausted ────────► FAILED
```

Another terminal state exists:

```text
SKIPPED
```

Used when required dependencies failed.

---

# 14. Attempt Lifecycle

Every retry creates a new attempt.

```text
ASSIGNED
    │
    │ worker acknowledges / starts
    ▼
RUNNING
    │
    ├── SUCCEEDED
    ├── FAILED
    ├── TIMED_OUT
    ├── CANCELLED
    └── LOST
```

`LOST` is deliberately different from `FAILED`.

Example:

```text
Job tests

Attempt #1
Worker B
LOST: lease expired

Attempt #2
Worker C
SUCCEEDED

Logical Job
SUCCEEDED
```

The workload did not fail.

The infrastructure did.

---

# 15. Fencing Model

Each logical job owns a monotonic counter:

```text
fencing_token
```

When the Scheduler creates an attempt:

```text
job.fencing_token++
```

Example:

```text
Attempt #1
worker-b
fence = 14

lease expires

Attempt #2
worker-c
fence = 15
```

Worker B later sends:

```text
CompleteAttempt(
  attempt_id = attempt-1,
  fence = 14,
  exit_code = 0
)
```

The Control Plane sees:

```text
current fence = 15
```

and returns:

```text
STALE_ATTEMPT
```

The result cannot overwrite Attempt #2.

This is one of ForgeGrid's primary correctness properties.

---

# 16. Completion Race

Attempt completion uses a PostgreSQL transaction.

Conceptually:

```text
LOCK job
LOCK attempt

verify:
  job.current_attempt_id == attempt.id
  job.fencing_token == attempt.fencing_token
  attempt.state is active
  lease_expires_at > NOW()
```

If all conditions hold:

```text
attempt → terminal
job → terminal/retry state
worker_session.active_slots--
```

Otherwise completion is rejected or treated as an idempotent duplicate.

---

# 17. Lease Expiration Race

Suppose:

```text
T0 lease expires
T1 worker sends SUCCESS
T2 recovery loop notices expiry
```

Completion must still fail.

ForgeGrid must not depend on the recovery scanner having already run.

`CompleteAttempt` checks:

```text
lease_expires_at > NOW()
```

itself.

Therefore an expired attempt is non-authoritative even before cleanup occurs.

---

# 18. Retry Policy

Infrastructure failures are retryable by default.

Examples:

```text
WORKER_LOST
LEASE_EXPIRED
ASSIGNMENT_LOST
EXECUTOR_INFRA_ERROR
```

Workload failures are not automatically retried.

Examples:

```text
EXIT_NON_ZERO
INVALID_COMMAND
JOB_TIMEOUT
```

Cancellation is never retried.

Initial default:

```text
max_attempts = 2
```

meaning:

```text
initial attempt
+
one recovery attempt
```

Retry delay may initially use a small configurable capped exponential backoff.

---

# 19. Scheduler Eligibility

A worker session is eligible when:

```text
state == ONLINE

AND

last_seen_at is fresh

AND

active_slots < capacity_slots

AND

session is connected

AND

not draining
```

Initial workers may expose:

```text
capacity_slots = 1
```

This makes early correctness tests substantially easier.

Capacity greater than one can be enabled after the first recovery milestone works.

---

# 20. Scheduler Selection

V1 does not need sophisticated bin packing.

Selection:

```text
lowest active_slots
then
oldest last_assignment_at
then
worker_id/session_id deterministic tie-break
```

The interesting engineering problem is not ranking.

It is atomic ownership.

---

# 21. Transactional Scheduling

Conceptually:

```text
BEGIN

SELECT runnable job
FOR UPDATE SKIP LOCKED

SELECT eligible worker_session
FOR UPDATE SKIP LOCKED

increment job fencing token

increment job attempt_count

create job_attempt

set job.current_attempt_id

job → DISPATCHED

worker_session.active_slots++

COMMIT
```

Only after commit does the Control Plane send the assignment through gRPC.

If dispatch fails after commit, the attempt eventually loses its lease and recovery occurs.

No special distributed transaction is required.

---

# 22. gRPC Control Protocol

Workers initiate outbound connections to the Control Plane.

One long-lived bidirectional stream carries control messages.

Conceptual service:

```text
WorkerControl.Connect()
```

Worker → Control Plane message classes:

```text
Register
Heartbeat
AssignmentAccepted
AssignmentRejected
AttemptStarted
LeaseRenewRequest
AttemptCompleted
AttemptFailed
CancellationAcknowledged
```

Control Plane → Worker:

```text
Registered
RunAttempt
LeaseRenewed
CancelAttempt
Drain
TerminateSession
```

Every attempt-related message includes:

```text
attempt_id
fencing_token
worker_session_id
```

---

# 23. Log Transport

Logs do not share the high-priority control path.

Separate gRPC streaming is used for logs.

Conceptually:

```text
WorkerLogs.Stream()
```

Log record:

```text
attempt_id
sequence
stream
payload
```

where:

```text
stream = STDOUT | STDERR
```

The worker owns one sequencer for both stdout and stderr.

Therefore ForgeGrid preserves the order in which the worker observed the two streams.

It does not claim to reconstruct an unknowable perfect causal ordering between independent operating-system file descriptors.

---

# 24. Log Idempotency

PostgreSQL uniqueness:

```text
PRIMARY KEY (attempt_id, sequence)
```

If a worker reconnects and resends:

```text
sequence 151
sequence 152
sequence 153
```

existing chunks are ignored.

Logs belong to attempts.

Therefore stale Attempt #1 logs can safely coexist with current Attempt #2 logs.

They cannot corrupt the new attempt's stream.

---

# 25. Browser Live Logs

Browser transport uses SSE.

Example:

```text
GET /api/v1/attempts/{attempt_id}/logs/stream?after=153
```

If browser connection closes:

```text
last sequence = 153
```

then after reconnect:

```text
after=153
```

ForgeGrid resumes at:

```text
154
```

No WebSocket is required for V1.

---

# 26. Pipeline Model

Pipeline DAG is static.

Example:

```text
       build
       /   \
      /     \
 unit-test integration-test
      \     /
       \   /
       package
```

Pipeline submission validates:

```text
unique job keys
existing dependencies
no self dependency
no cycles
valid job spec
valid retry limits
valid timeout
```

Invalid DAGs are rejected before persistence as runnable pipelines.

Milestone 3 implements this static model with bounded submissions (128 jobs, 2,048 edges). Concrete transaction, timeout, cancellation, and terminal-state rules are recorded in [pipeline-semantics-audit.md](pipeline-semantics-audit.md). Presentation/observability were deferred at Milestone 3 and implemented in Milestones 4–5.

---

# 27. Dependency Semantics

Jobs without dependencies:

```text
QUEUED
```

Jobs with unresolved dependencies:

```text
BLOCKED
```

When a dependency succeeds, ForgeGrid evaluates dependent jobs.

If all required dependencies succeeded:

```text
BLOCKED → QUEUED
```

If a required dependency permanently fails:

```text
BLOCKED → SKIPPED
```

Independent branches continue.

Required cancelled/skipped parents also propagate SKIPPED through unresolved descendants. Retryable logical parents remain nonterminal until success or exhaustion; historical failed attempts do not cause premature skips.

V1 does not implement pipeline-wide fail-fast semantics.

---

# 28. Pipeline Terminal State

A pipeline becomes terminal after all jobs become terminal.

Conceptually:

```text
all jobs succeeded
→ SUCCEEDED

at least one job failed
→ FAILED

pipeline cancellation
→ CANCELLED
```

`SKIPPED` downstream jobs do not convert a failed pipeline into success.

The implemented coordinator uses a PostgreSQL pipeline row gate before ordered job locks, then preserves job → attempt → worker-session locking. Attempt finalization, dependency propagation, and aggregation commit together. Whole-pipeline cancellation stays CANCELLING until all jobs terminate and yields CANCELLED. Without whole-pipeline intent, failure takes precedence over individual cancellation, then all-success yields SUCCEEDED.

---

# 29. Job Specification

V1 job specification contains approximately:

```text
job key
Docker image
argv command
dependencies
timeout
max_attempts
environment variables without secrets
```

Prefer:

```text
["go", "test", "./..."]
```

over:

```text
"go test ./..."
```

This avoids unnecessary shell parsing ambiguity.

V1 does not include secret management.

---

# 30. Docker Execution Boundary

The Worker creates one container per attempt.

Job containers are:

```text
non-privileged
without Docker socket
without host PID namespace
without host network
bounded by timeout
labelled with ForgeGrid ownership metadata
```

Network access may be disabled by default during the first milestone.

CPU/memory limits may be added once the executor contract exists.

ForgeGrid V1 executes trusted workloads.

It is not a secure hostile multi-tenant sandbox.

---

# 31. PostgreSQL Schema

Initial authoritative tables:

```text
pipelines
jobs
job_dependencies
job_attempts
workers
worker_sessions
job_log_chunks
```

An append-only events table can be introduced after the recovery milestone if needed for timeline presentation.

---

# 32. pipelines

Conceptual columns:

```text
id
state

idempotency_key
request_hash

created_at
finished_at
```

Idempotency behavior:

Implementation status (Milestones 1–5): submission idempotency keys and request hashes are not implemented. The behavior below is a conceptual design, not a current HTTP API guarantee. Repeating a submission can create another pipeline; completion and log-delivery idempotency are implemented separately.

Same key + same request:

```text
return existing pipeline
```

Same key + different request:

```text
409 conflict
```

---

# 33. jobs

Conceptual columns:

```text
id
pipeline_id
job_key

state

image
command
timeout_seconds

max_attempts
attempt_count

current_attempt_id
fencing_token

retry_available_at
cancel_requested_at

created_at
started_at
finished_at
```

---

# 34. job_dependencies

```text
job_id
depends_on_job_id
```

Unique constraint:

```text
(job_id, depends_on_job_id)
```

---

# 35. job_attempts

```text
id
job_id

attempt_number
fencing_token

worker_session_id

state

lease_expires_at

assigned_at
started_at
finished_at

execution_deadline_at

exit_code

failure_kind
failure_detail
```

`failure_detail` is bounded and sanitized.

It is not an unbounded log field.

---

# 36. workers

Stable logical identities:

```text
id
name
created_at
```

---

# 37. worker_sessions

```text
id
worker_id

state

agent_version

capacity_slots
active_slots

started_at
last_seen_at
last_assignment_at

disconnected_at
ended_at
```

Attempts reference `worker_sessions.id`.

---

# 38. job_log_chunks

```text
attempt_id
sequence
stream
payload
created_at
```

Primary key:

```text
(attempt_id, sequence)
```

Payload has a strict maximum chunk size.

---

# 39. Cancellation

Cancellation is an authoritative database transition.

Conceptually:

```text
user requests cancellation
        │
        ▼
job → CANCELLING
        │
        ├── send CancelAttempt
        │
        ▼
worker kills container
        │
        ▼
attempt → CANCELLED
job → CANCELLED
```

If the worker cannot be reached, ForgeGrid waits for lease expiry.

When recovery handles the lost attempt:

```text
cancel_requested == true
```

means:

```text
do not retry
job → CANCELLED
```

---

# 40. Cancel vs Completion Race

Two transactions race:

```text
CancelJob
CompleteAttempt
```

The first valid authoritative terminal transition wins.

If completion commits first:

```text
SUCCEEDED
```

and later cancellation receives:

```text
already terminal
```

If cancellation commits first, the old successful completion becomes invalid for finalization.

The database is the arbiter.

---

# 41. Control Plane Crash Semantics

If the Control Plane crashes:

```text
workers stop receiving lease renewal ACKs
```

Therefore their local monotonic lease guards eventually expire.

Workers stop their active executions.

After the Control Plane restarts:

```text
workers reconnect
sessions register
expired attempts recover
new attempts may be scheduled
```

Availability is temporarily lost.

Safety is preferred over continuing jobs indefinitely without authoritative ownership.

V1 has one Control Plane instance.

ForgeGrid V1 does **not** claim Control Plane high availability.

---

# 42. Worker Network Partition

Example:

```text
Control Plane
      X
      X network
      X
Worker B
```

Worker B remains alive.

But:

```text
lease renewal ACK stops
```

Therefore Worker B's local lease guard eventually kills its job container.

Meanwhile the Control Plane waits for the persisted lease expiry before creating another authoritative attempt.

This greatly limits split-brain execution.

---

# 43. Worker Process Crash

If Worker B itself crashes:

```text
heartbeats stop
gRPC stream disappears
```

The Control Plane stops assigning new jobs to its session.

Existing attempts are not immediately reassigned.

After:

```text
lease_expires_at
```

they become `LOST`.

If retryable:

```text
job → RETRY_WAIT → QUEUED
```

and another worker receives a new attempt.

Any container surviving the dead agent is stale and cannot produce an authoritative completion.

---

# 44. Failure Matrix

| Failure                  | Expected behavior            |
| ------------------------ | ---------------------------- |
| Worker dies before start | lease expiry → LOST → retry  |
| Worker dies during job   | lease expiry → LOST → retry  |
| Worker network partition | local lease guard stops job  |
| Old worker reconnects    | stale session/fence rejected |
| Old completion arrives   | rejected                     |
| Duplicate completion     | idempotent                   |
| Duplicate heartbeat      | harmless                     |
| Duplicate log chunks     | ignored                      |
| Assignment message lost  | lease expiry recovery        |
| Control Plane crash      | worker leases expire locally |
| PostgreSQL unavailable   | fail closed                  |
| Docker start error       | bounded infra retry          |
| Exit code non-zero       | workload FAILED              |
| Timeout                  | TIMED_OUT                    |
| Cancellation during run  | CANCELLING → CANCELLED       |
| Cancel/result race       | transactional winner         |
| Dependency fails         | downstream SKIPPED           |
| Browser disconnect       | log replay by sequence       |

---

# 45. Observability

Milestone 4 implements this observation layer without adding it to coordination authority. See [observability-audit.md](observability-audit.md) for trace propagation, committed metric snapshots, cardinality, exporter isolation, and evidence. Frontend presentation was deferred at Milestone 4 and implemented in Milestone 5.

Prometheus metrics should include:

```text
forgegrid_workers_online

forgegrid_jobs_queued

forgegrid_attempts_started_total

forgegrid_attempts_completed_total

forgegrid_attempt_retries_total{reason}

forgegrid_lease_expirations_total

forgegrid_attempt_duration_seconds

forgegrid_queue_wait_seconds

forgegrid_scheduler_assignment_seconds

forgegrid_log_bytes_total
```

Do not use high-cardinality identifiers such as:

```text
job_id
attempt_id
worker_session_id
```

as Prometheus labels.

Those belong in traces and structured logs.

---

# 46. Distributed Tracing

Implementation persists bounded W3C traceparent metadata for asynchronous claims/retries and propagates per-attempt context over gRPC. Requests end with responses; stream lifetime is not execution lifetime. Diagnostic queue timing measures eligible queue wait independently of blocked/backoff time.

Important spans:

```text
pipeline.submit

scheduler.claim_job
scheduler.select_worker
scheduler.create_attempt

grpc.dispatch

worker.accept_assignment

worker.docker.create
worker.docker.start
worker.docker.wait

worker.report_result

controlplane.finalize_attempt

recovery.expire_lease
recovery.retry_job
```

Trace context travels inside the assignment.

The browser/API submission HTTP request does not need to remain artificially open while the job executes.

Asynchronous scheduler/execution work should use appropriate trace propagation or links.

---

# 47. Structured Logging

All Control Plane and Worker logs use structured fields.

Examples:

```text
job_id
attempt_id
worker_id
worker_session_id
fencing_token
pipeline_id
```

Secrets and full environment contents are never logged.

---

# 48. Local Development Topology

Docker Compose initially runs:

```text
postgres

control-plane

worker-a
worker-b
worker-c

otel-collector
prometheus
jaeger

web
```

The three workers allow failure recovery to be demonstrated locally.

---

# 49. V1 Message Broker Decision

ForgeGrid V1 intentionally has:

```text
NO NATS
NO Kafka
NO RabbitMQ
NO Redis queue
```

Responsibilities are divided as:

```text
PostgreSQL
→ durable authority

gRPC
→ live worker transport
```

A broker would currently introduce additional consistency problems without solving ForgeGrid's core problems.

A broker may only be introduced after measurement demonstrates a concrete need.

Examples:

```text
large-scale event fan-out
very high worker counts
independent durable event consumers
control-plane decomposition
high-throughput log/event pipelines
```

The architectural decision is:

> No broker until there is a measured requirement.

---

# 50. First Vertical Slice

The first slice does not implement the full DAG system.

It validates the execution backbone.

Required path:

```text
Control Plane starts

Worker B registers
Worker C registers

heartbeats visible

single job submitted

scheduler selects worker

attempt created
lease created
fence assigned

assignment dispatched

Docker container starts

stdout/stderr streamed

result reported

attempt SUCCEEDED

job SUCCEEDED

final state persisted
```

---

# 51. First Recovery Slice

This is the defining ForgeGrid acceptance test.

Initial state:

```text
worker-b ONLINE
worker-c ONLINE

job-42 RUNNING
attempt-1
worker-b
fence=1
```

Failure:

```text
docker kill forgegrid-worker-b
```

Expected sequence:

```text
worker-b heartbeats disappear

worker-b session → OFFLINE

attempt-1 remains authoritative
until its lease expires

lease expires

attempt-1 → LOST

job-42 → RETRY_WAIT

job-42 → QUEUED

scheduler selects worker-c

attempt-2 created

fence=2

worker-c executes job

attempt-2 → SUCCEEDED

job-42 → SUCCEEDED
```

Mandatory final assertion:

```text
A delayed completion from attempt-1/fence-1
must be rejected.
```

Without that assertion, the recovery milestone is not complete.

---

# 52. Development Gates

### Gate A — Execution backbone

Deliver:

```text
schema
Control Plane
worker registration
worker sessions
gRPC
Docker executor
single-job scheduling
logs
final persistence
```

No significant frontend work.

---

### Gate B — Ownership and recovery

Deliver:

```text
heartbeats
leases
renewal ACK
local lease guard
fencing
worker failure detection
LOST attempts
bounded retry
reassignment
stale result rejection
```

ForgeGrid's central engineering value exists after this gate.

---

### Gate C — Concurrency correctness

Prove:

```text
duplicate completion
scheduler races
assignment loss
worker reconnect
old session messages
lease expiration/completion race
cancel/completion race
Control Plane restart
```

Use real PostgreSQL integration tests.

---

### Gate D — Pipeline semantics

Add:

```text
static DAG
cycle detection
BLOCKED jobs
dependency release
SKIPPED descendants
timeouts
cancellation
retry policy
```

---

### Gate E — Observability and presentation

Add:

```text
React UI
pipeline view
worker view
attempt history
live logs
Prometheus
OpenTelemetry
Jaeger
failure demo script
architecture docs
ADRs
README
```

UI is deliberately last.

---

# 53. Repository Layout

```text
forgegrid/
│
├── cmd/
│   ├── controlplane/
│   │   └── main.go
│   └── worker/
│       └── main.go
│
├── internal/
│   ├── controlplane/
│   │   ├── api/
│   │   ├── grpc/
│   │   ├── scheduler/
│   │   ├── recovery/
│   │   ├── dag/
│   │   └── workers/
│   │
│   ├── worker/
│   │   ├── agent/
│   │   ├── leaseguard/
│   │   ├── executor/
│   │   ├── docker/
│   │   └── logs/
│   │
│   ├── domain/
│   │   ├── pipeline/
│   │   ├── job/
│   │   ├── attempt/
│   │   └── worker/
│   │
│   └── store/
│       └── postgres/
│
├── api/
│   └── proto/
│       └── forgegrid/
│           └── v1/
│
├── gen/
│   └── go/
│
├── db/
│   └── migrations/
│
├── web/
│
├── deploy/
│   └── compose/
│
├── tests/
│   ├── integration/
│   ├── recovery/
│   └── e2e/
│
├── scripts/
│   ├── demo-recovery.*
│   └── verify.*
│
├── docs/
│   ├── architecture.md
│   ├── execution-semantics.md
│   ├── failure-model.md
│   ├── protocol.md
│   └── adr/
│
├── docker-compose.yml
├── go.mod
└── README.md
```

One repository.

One Go module initially.

Two production binaries:

```text
forgegrid-controlplane
forgegrid-worker
```

---

# 54. Required ADRs

Architecture decision records:

```text
ADR-001 — Go for Control Plane and Worker
ADR-002 — PostgreSQL as coordination authority
ADR-003 — No message broker in V1
ADR-004 — Worker-initiated long-lived gRPC
ADR-005 — Separate heartbeat and execution leases
ADR-006 — Fencing tokens for stale ownership
ADR-007 — At-least-once execution semantics
ADR-008 — Docker trusted-workload executor
ADR-009 — SSE browser log streaming
ADR-010 — Single Control Plane V1 boundary
ADR-011 — Worker session/incarnation identities
```

---

# 55. Explicit V1 Exclusions

The following are intentionally excluded:

```text
GitHub App
Git cloning
PR triggers
webhooks
Kubernetes
autoscaling
NATS
Kafka
Redis queue
billing
teams
RBAC
multi-tenancy
secrets
distributed cache
matrix jobs
conditional workflow language
plugin system
hosted runners
Windows executors
macOS executors
Control Plane HA
hostile multi-tenant sandboxing
LLM integration
```

They may not appear in README capability claims.

---

# 56. Portfolio Proof

ForgeGrid's primary demo is not the UI.

It is the failure sequence:

```text
RUNNING
worker-b

        ↓ worker killed

HEARTBEAT LOST

        ↓

WORKER OFFLINE

        ↓

LEASE EXPIRED

        ↓

ATTEMPT #1 LOST

        ↓

RETRY

        ↓

ATTEMPT #2
worker-c

        ↓

SUCCEEDED
```

Then deliberately send or simulate the old Attempt #1 completion:

```text
attempt=1
fence=1
SUCCESS
```

and visibly prove:

```text
STALE_ATTEMPT_REJECTED
```

That is the project's signature engineering demonstration.

---

# 57. CV-Level Engineering Claims

Only after implementation and testing may ForgeGrid claim things such as:

> Built a distributed job execution engine in Go using gRPC, PostgreSQL and Docker, coordinating multiple workers through heartbeat-based liveness detection and renewable execution leases.

> Designed lease expiry and fencing-token recovery semantics that reassign orphaned jobs while preventing stale workers from committing authoritative results.

> Implemented concurrency-safe job claiming, bounded infrastructure retries, worker incarnation identities and idempotent terminal transitions backed by PostgreSQL transactions.

> Added cross-process observability with OpenTelemetry, Prometheus and Jaeger across scheduler, worker and Docker execution paths.

No performance or scale claim may be made without benchmark evidence.

---

# 58. Definition of V1 Success

ForgeGrid V1 is successful when we can explain and demonstrate all of the following without hand-waving:

```text
Who owns this job?

How long does that ownership remain valid?

How is ownership renewed?

What happens when renewal stops?

How does ForgeGrid know a worker is gone?

Why doesn't heartbeat loss instantly cause reassignment?

What happens if the old worker returns?

What happens if two completions race?

What happens if cancellation races completion?

What happens if the Control Plane crashes?

What happens if the Worker crashes?

Can a stale attempt overwrite the current attempt?

Can retry run forever?

Where is authoritative state stored?

What delivery semantic does ForgeGrid provide?

What does ForgeGrid explicitly not guarantee?
```

If all of those answers are visible in code, tests, database transitions, traces and documentation, ForgeGrid has achieved its purpose.

---

# Milestone 5 implementation note

The operational React/TypeScript/Vite console implements the architecture's inspection layer: static DAGs, persisted job/attempt/session state, recovery chronology and resumable SSE logs. Read-only API additions expose existing PostgreSQL metadata; no new coordination authority, ownership transition, gRPC service or schema is introduced. Browser clocks, polling, trace availability and connection state never grant execution authority. See [the operational console audit](operational-console-audit.md) for routes, snapshot boundaries, evidence and intentionally unsupported states.
