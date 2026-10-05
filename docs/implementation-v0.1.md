# First slice implementation decisions

Architectural source: docs/architecture-v0.1.md, read completely before implementation. No architectural blocker or product redesign was required.

1. One Go module, two production binaries, one Control Plane. The scheduler and recovery manager are logical functions in the Control Plane package, rather than separate services. A third development-only executable replays stale results for the acceptance demo.
2. Five tables only: jobs, job_attempts, workers, worker_sessions, job_log_chunks. Single-job HTTP submission omits later pipeline/DAG tables and APIs. Explicit pgx SQL handles coordination; no ORM or broker.
3. PostgreSQL time after row-lock acquisition validates lease authority. This implements the architecture's lease-validity rule more precisely than transaction-start NOW(). Completion/renewal/recovery share job -> attempt -> session lock order. Registration never acquires job locks.
4. Fencing tokens and attempt numbers increment while the job row is locked; schema uniqueness and a partial active-attempt index reinforce code invariants. Dispatch occurs after commit. Current pointers may reference a terminal historical attempt while RETRY_WAIT/QUEUED; no attempt is active then.
5. Worker sessions expose capacity one. Session supersession invalidates old process messages but cannot create new ownership for an unexpired job. Persisted connection flags are cleared at Control Plane startup; live stream presence is a transport eligibility hint alongside PostgreSQL checks.
6. The Docker CLI executes argv directly with an explicit entrypoint and deterministic container name. Five labels support reconciliation. Workload containers have no host mounts/socket, privilege, host namespaces, or network. Agents use the local Docker daemon socket and execute trusted workloads.
7. Initial execution authority comes from a renewal ACK. Request-send time plus acknowledged TTL defines the worker deadline conservatively, avoiding the late-ACK extension problem. Control and log RPCs have distinct connections; logs have bounded chunks/queue/retries and durable ACKs.
8. Only infrastructure failures retry, using capped exponential delay and max_attempts. LOST is distinct from workload FAILED/TIMED_OUT. Completion duplicates are read-only acknowledgements. Stale results are explicit rejections.
9. Dockerized Go/protoc tooling supports this Windows repository without host tool installation. PostgreSQL integration tests create isolated schemas; normal/recovery/lease-guard scripts test real processes and containers. No UI or deferred architecture gates were added.

The architecture's metrics/tracing, browser SSE, public cancellation, DAGs, and other later gates remain deferred. This document records slice choices; it does not replace the architectural source of truth.

## Gate C correctness decisions

The pre-change classification and regression evidence are in [correctness-audit.md](correctness-audit.md). No architectural blocker was found. PostgreSQL coordination, fencing, independent liveness/lease checks, bounded retry policy, and the ownership lock order are unchanged.

- Registered is queued before a stream becomes visible to scheduling. A physically finished execution with drained logs can accept the next PostgreSQL-authorized assignment while its old result ACK is delayed; old ACKs cannot clear the new attempt.
- The architecture's cancellation/completion race is implemented as an internal Store.Cancel primitive only. Migration 002 adds cancel_requested_at and CANCELLING/CANCELLED states without adding tables. Cancellation locks job -> attempt -> session. Active cancellation retains ownership and capacity until a valid cancelled-execution result or lease-expiry recovery; it disables renewals and never retries. A queued job cancels directly. Public cancellation remains deferred.
- Workers ignore renewal ACKs received after cancellation intent. A cancellation ACK follows executor return and cleanup attempts, including log draining; Docker shutdown remains best effort if its daemon cannot be reached.
- Tests order transactions with PostgreSQL triggers, advisory barriers, and observed lock waits. Injected write failures and deferred constraint-trigger failures test full rollback, including commit-time failure. These hooks exist only in isolated test schemas, not in production coordination code.
- Separate test processes run production Control Plane/worker components. SIGKILL and real Docker executions verify exact crash windows, physical orphan survival/overlap, startup reconciliation, and stale result rejection. No fault endpoints or additional infrastructure are introduced.

## Milestone 3 implementation

Gate D adds pipelines and immutable job_dependencies to the original five tables. Standalone jobs remain supported with nullable membership. Validation rejects invalid DAGs before one submission transaction persists jobs/edges.

A PostgreSQL pipeline row gate and all its jobs in ID order precede attempt/session locks. Scheduling/recovery use SKIP LOCKED at the gate before candidate jobs; completion/cancellation/renewal wait on it. Whole-pipeline cancellation prelocks active attempts and sessions by ID. This serializes bounded pipeline coordination while allowing Docker branches to execute concurrently and preserves ownership lock order.

Completion/recovery/cancellation propagate dependencies/transitive skips and aggregate state in their authoritative transaction. Logical retry states remain nonterminal. HTTP pipeline submit/inspect/cancel and job cancel reuse existing execution/cancellation. No DAG transport service is added.

Attempt deadlines are write-once at initial renewal (start fallback), separate from renewable leases. ACKs carry remaining execution budget for monotonic enforcement. Expired success is independently rejected; missing stop ACK retains reservation until lease expiry. See [pipeline semantics audit](pipeline-semantics-audit.md) for precedence, boundaries, races, and evidence. Earlier slice/Gate C scope statements above are historical; public cancellation and static DAGs are now implemented.
