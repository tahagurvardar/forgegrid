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

Docker owns job containers independently of the agent process. SIGKILL removes the worker-side lease guard but can leave the job alive. All containers have worker ID, session ID, job ID, attempt ID, and fencing token labels. New process sessions reconcile containers from older sessions of their worker identity. Containers on permanently inaccessible hosts cannot be remotely stopped. Buffer contents disappear on process crash; acknowledged log chunks remain durable in PostgreSQL.

Run `scripts/demo-recovery.ps1` for the worker-b -> worker-c proof, including delayed result replay. Run `scripts/test-leaseguard.ps1` to stop the Control Plane and prove the live worker cancels without renewal ACKs. Integration tests additionally exercise scheduling/completion concurrency, supersession, and lease expiry during a real row-lock wait.
