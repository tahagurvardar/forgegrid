# Execution semantics

Current implemented semantics, extended through DAGs, observability and the operational console. Historical milestone evidence is preserved in the audit documents; [release-audit.md](release-audit.md) records the current full verification.

PostgreSQL is the authority. A job has one current_attempt_id and a monotonically increasing fencing_token. Retries allocate a fresh UUID, increment attempt_number and fencing_token, and preserve prior attempt identities and history. A partial unique index permits at most one ASSIGNED/RUNNING attempt per job. A composite foreign key keeps the current attempt pointer within its own job.

Scheduling locks a QUEUED job with FOR UPDATE SKIP LOCKED, then an eligible session with the same locking mode. Eligibility requires ONLINE, persisted connected=true, fresh server-receipt heartbeat, free capacity, and a currently registered control stream. Database reservations, attempt creation, and job ownership commit before dispatch. A lost dispatch remains reserved until lease expiry; no in-memory delivery failure transfers ownership.

Ownership operations lock job -> attempt -> worker session, then read clock_timestamp() in PostgreSQL. A preliminary unlocked lookup reads only the immutable attempt-to-job relationship. Session registration locks worker -> sessions and never locks jobs or attempts. Heartbeats and offline scanning touch sessions only, in separate transactions from recovery. This order avoids a session -> job lock cycle.

Completion validates request attempt ID, request fence against both job and attempt, worker session ID, current authoritative attempt, active state, nonsuperseded ONLINE session, and a strictly unexpired lease. It checks lease validity independently of the recovery scanner. Lease renewal applies the same checks and additionally requires a connected session; it cannot resurrect an expired attempt. The lease time check occurs after every required lock is acquired. PostgreSQL NOW() would retain the transaction's start time through a lock wait; clock_timestamp() prevents that stale-time completion bug. The validity check under exclusive ownership locks is the transition's linearization point.

An exact repeated terminal result for the current winner is acknowledged as a duplicate without writing or releasing another slot, even if its old lease has since expired. A conflicting terminal result is rejected. LOST attempts and attempts superseded by a newer current attempt return STALE_ATTEMPT. Accepted terminal transitions update the attempt, job, and reserved slot atomically.

Heartbeat loss changes session liveness only. Recovery waits until the authoritative lease is invalid, marks the attempt LOST, releases its old slot, and chooses RETRY_WAIT or FAILED. Due RETRY_WAIT jobs become QUEUED. Retrying creates another attempt; no identity is reused. max_attempts includes the initial execution. Infrastructure failures (expired lease, executor/daemon failure, assignment rejection) may retry within that bound. Nonzero workload exit, invalid executable, and workload timeout fail without automatic retry.

Workers begin Docker execution only after their first successful renewal ACK. For each request, they retain local monotonic send time and the request ID. A matching ACK sets the deadline to request-send-time + acknowledged TTL - 100ms. This conservatively includes queue and network delay; receiving an ACK does not grant extra time after transport delay. Sending a request grants no authority. Missing ACKs expire the local guard and cancel execution, which attempts to force-remove the labelled container. No synchronized wall-clock assumption is used.

Physical execution is at least once. Lease expiry does not prove that a crashed worker's Docker container stopped. Two physical executions can overlap, while only the current valid attempt can produce a PostgreSQL-authoritative terminal winner. Fences protect ForgeGrid state; they do not fence arbitrary external side effects in user workloads.

## Cancellation and message ordering

HTTP job and pipeline cancellation reuse the authoritative primitive. It serializes with completion using job -> attempt -> session locks (pipeline operations acquire their outer gate first). Success committing first returns ALREADY_TERMINAL to cancellation. Cancellation committing first sets CANCELLING and invalidates successful completion. The current attempt and slot stay reserved; cancellation neither transfers ownership nor decrements capacity. Renewals are rejected. BLOCKED/QUEUED/RETRY_WAIT jobs become CANCELLED directly.

After receiving CancelAttempt with reason CANCELLATION_REQUESTED, the worker cancels its executor, waits for executor return/cleanup and log draining, then reports CANCELLED, exit_code=-1, failure_kind=JOB_CANCELLED. A delayed renewal ACK cannot start or extend cancelled execution. The authoritative ACK requires the current attempt, matching fence/session, active state, ONLINE session, and valid lease. Duplicate cancelled results release no additional slot. If no valid ACK arrives, expiry records LOST and terminal job CANCELLED, releases the slot once, and suppresses retries. Cleanup is best effort when Docker is unavailable; authoritative cancellation does not guarantee remote physical shutdown.

The validity check after all ownership locks is the linearization point. A completion that passed it before expiry can commit after expiry while retaining those locks; the scanner skips that locked job and cannot install another owner. A request that obtains the locks after expiry is rejected even if the scanner has not run.

Registered precedes every assignment on a newly published stream. A new authoritative assignment may arrive before the previous completion ACK because the database releases capacity before transport delivery. Workers accept it only after the previous physical executor and log draining have finished, and ignore the delayed previous ACK by identity.

## Static pipelines and execution budgets

Pipeline transitions acquire the PostgreSQL pipeline row and all its jobs in ID order before attempt/session locks. Dependency release, fixed-point skips, and aggregation commit with parent terminal transitions. BLOCKED children queue only after all logical parents succeed; retry states prevent premature skips. Permanently failed/cancelled/skipped parents skip unresolved descendants. Independent branches continue.

Pipelines finalize only after every job is terminal. Whole-pipeline cancellation intent yields CANCELLED; otherwise failure takes precedence over individual cancellation, then all-success yields SUCCEEDED. Whole cancellation atomically cancels all nonterminal jobs and waits for active reservations to stop or expire.

Each attempt's write-once execution_deadline_at begins at initial renewal (start fallback). It excludes queue/dependency wait and includes Docker preparation, execution, cleanup/log draining, and accepted reporting. Lease ACKs carry separate execution_budget_ms; workers derive a monotonic execution timer from request-send time, never extended by renewal. Exhausted initial ACK cannot start execution. Completion independently rejects late success as EXECUTION_TIMEOUT; a timeout stop ACK still needs a valid lease. Without it, recovery waits for expiry, records TIMED_OUT/JOB_TIMEOUT, and suppresses workload retry. Committed cancellation overrides timeout during recovery. Lease expiry before its budget retains normal bounded infrastructure retry.

The [pipeline audit](pipeline-semantics-audit.md) defines exact precedence, boundaries, conservative reporting behavior, and test evidence. Workers/Control Plane upgrade together: zero execution budget means exhausted.

## Observation boundary

The operational console reads persisted state and deadlines. Its polling interval, browser clock and SSE connection do not grant authority or determine loss. A displayed current pointer may reference a terminal attempt. Cancellation uses the existing durable API and waits for authoritative refresh; closing the browser does not stop execution. See [operational-console-audit.md](operational-console-audit.md).

Trace context and queue timing are diagnostic metadata. They do not participate in assignment eligibility, ownership, lease validity, timeout, retry, cancellation, or DAG decisions. Spans may describe work later rolled back; enclosing committed flags and PostgreSQL distinguish it. Committed-history metrics exclude duplicate/rolled-back transitions and are cached asynchronously. Exporter/scrape failures never supply authority or execution ACKs. See [observability audit](observability-audit.md).
