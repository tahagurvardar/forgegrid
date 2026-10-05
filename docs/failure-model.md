# Failure model for the first vertical slice

| Failure | Behavior |
|---|---|
| Agent dies before/during execution | Session becomes OFFLINE after heartbeat threshold; reserved attempt remains current until lease expiry, then LOST and bounded retry. |
| Dispatch lost after commit | Reservation stays in PostgreSQL; lease expiry recovers it. |
| Renewal ACK lost or delayed | Local monotonic guard grants no extra authority; worker cancels and attempts Docker removal. |
| Old process sends a result | Identity, fence, current attempt, state, session, and lease checks reject stale authority. |
| Duplicate accepted completion | Exact current terminal result receives duplicate ACK; no repeated slot release. |
| Session superseded | Old heartbeats and renewals fail; completion cannot finalize an active attempt from that session. Old lease still expires before reassignment. |
| Control Plane stops | No renewal ACKs; workers enforce local guards. Compose restarts exited workers as fresh sessions; the restarted Control Plane clears persisted connection flags and recovers expired attempts. |
| PostgreSQL unavailable | Coordination fails closed; workers lose renewal ACKs and cancel. No in-memory state can finalize a job. |
| Docker unavailable | Executor infrastructure failure may retry, within max_attempts. Container cleanup is best effort and separately bounded. |
| Workload exits nonzero / invalid executable / timeout | Attempt FAILED / FAILED / TIMED_OUT; job FAILED; no workload retry. |
| Log RPC fails | Chunk is resent with the same identity and sequence. Duplicate insert does nothing. After bounded delivery failures, execution is cancelled and reported as infrastructure failure if its lease is still valid. |
| Recovery and completion contend | Shared ownership locks serialize transitions. Expired completion is independently rejected; a valid completion holding the job lock cannot be overwritten by recovery. |
| Renewal and completion contend | Renewal first may extend the active lease, then completion may win. Completion first makes a later renewal stale; it cannot renew a terminal attempt. |
| Internal cancellation and success contend | First committed transition wins. Cancellation retains the slot until a valid stop ACK or expiry, rejects renewal/success, and never retries. |
| AttemptStarted not persisted before Control Plane crash | ASSIGNED still owns the committed lease. A started Docker container does not imply the start transaction committed; recovery waits for expiry. |
| Transaction write or commit fails | PostgreSQL rolls back that transition's attempt, job, and slot changes together. No rolled-back assignment is delivered. Offline marking and retry requeueing are separate transactions, not one all-or-nothing recovery batch. |
| Next assignment overtakes old result ACK | Once its previous executor/log drain finished, the worker accepts the new authoritative assignment; delayed old ACKs cannot clear it. |

Docker owns job containers independently of the agent process. SIGKILL removes the worker-side lease guard but can leave the job alive. All containers have worker ID, session ID, job ID, attempt ID, and fencing token labels. New process sessions reconcile containers from older sessions of their worker identity. Containers on permanently inaccessible hosts cannot be remotely stopped. Buffer contents disappear on process crash; acknowledged log chunks remain durable in PostgreSQL.

Run `scripts/demo-recovery.ps1` for the worker-b -> worker-c proof, including delayed result replay. Run `scripts/test-leaseguard.ps1` to stop the Control Plane and prove the live worker cancels without renewal ACKs. Integration tests additionally exercise scheduling/completion concurrency, supersession, and lease expiry during a real row-lock wait.

Run `scripts/verify-recovery.ps1` for isolated-schema, separate-process crash tests: commit before dispatch, Docker start before persisted AttemptStarted, worker SIGKILL with a surviving container and overlapping retry, fresh-session orphan reconciliation, and cancellation of a real Docker execution. Tests kill only their own helper processes and remove only containers labelled with their unique worker IDs. Their PostgreSQL barriers select the crash windows explicitly; they do not depend on guessing transaction timing.

## Pipeline failures

| Failure/race | Behavior |
|---|---|
| Parent permanently fails | Unresolved descendants become SKIPPED in that transaction; unrelated branches continue. |
| Parent has retry capacity | Children remain BLOCKED until valid success/exhaustion; stale attempts cannot release them. |
| Concurrent fan-in completion | PostgreSQL gate serializes readiness; conditional BLOCKED transition releases once. |
| Cancel races dependency release | Same gate/job locks; cancelled children cannot be resurrected. |
| Pipeline cancelled with active workers | All nonterminal jobs receive durable intent atomically; slots remain reserved until valid stop ACK or expiry. |
| Timeout with worker gone | No renewal/late success accepted; recovery records TIMED_OUT without retry when its persisted execution deadline precedes or equals lease expiry. Earlier lease expiry remains LOST with bounded infrastructure retry, even if scanning occurs after both deadlines. Prior committed cancellation yields logical CANCELLED. |
| Another branch active | Pipeline stays RUNNING/CANCELLING until every job is terminal. |
| Control Send fails while Recv blocks | Handler observes send failure independently and disconnects; ownership remains reserved. |

Execution budget excludes queue/dependency wait and includes preparation/log drain/accepted reporting. PostgreSQL deadlines are independent of scanning and never renewed. ACK budgets avoid a fresh timeout on delayed delivery. See the [pipeline audit](pipeline-semantics-audit.md). Run scripts/demo-pipeline.ps1 for parallel success/failure and scripts/demo-pipeline-recovery.ps1 for fenced worker-b → worker-c recovery inside the DAG.
