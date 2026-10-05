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
