# ForgeGrid: portfolio and interview notes

ForgeGrid is a Go distributed execution engine with CI-style static DAGs. The portfolio evidence is transaction correctness and recovery across real worker/Control Plane failures, rather than a throughput benchmark. See [release verification](release-audit.md) and [screenshots](screenshots.md).

## Factual CV bullets

- Built a Go/gRPC distributed Docker execution engine with PostgreSQL-authoritative attempts, worker incarnations, renewable leases, fencing and bounded infrastructure retries.
- Implemented static DAG coordination with transactional fan-in release, transitive failure skips, durable cancellation and per-attempt budgets; verified races and rollback with real PostgreSQL barriers and injected transaction faults.
- Demonstrated real process-crash recovery, including overlapping surviving Docker containers and rejection of delayed stale results, using isolated-schema integration and Docker end-to-end tests.
- Built a React/TypeScript operational console with resumable bounded SSE logs and visible LOST/retry history, plus correlated OpenTelemetry/Jaeger traces and bounded-cardinality Prometheus metrics; verified recovery in a real browser.

## Interview answers

**Why PostgreSQL instead of NATS/Kafka?**

One authoritative decision must cover ownership, capacity, results and dependencies. Row locks, constraints and a transaction express it directly. gRPC delivers after commit; delivery failure leaves the reservation to expire. A broker would not replace that ownership transaction and is unnecessary for this bounded scope. This is project-specific, not a general replacement for brokers.

**Why leases?**

Missing heartbeats mean a session is unreachable, not that its container stopped. The lease defines authority expiry; recovery waits, and completion checks it even before scanning. Workers require successful ACKs and enforce conservative monotonic deadlines.

**Why fencing tokens?**

A retry has a fresh attempt ID and a greater job-local fence. Delayed messages cannot present an old fence as current authority. Current pointer, session, state and lease are also checked. Fences protect ForgeGrid results, not arbitrary external side effects.

**Why isn't execution exactly-once?**

A crashed agent can leave daemon-owned execution alive while an expired attempt is retried elsewhere. ForgeGrid keeps one authoritative terminal winner, but cannot make physical work/side effects exactly-once. Bounded retries can exhaust; at-least-once is not an eventual-success promise.

**What happens during a network partition?**

Without renewal ACKs, a live worker's monotonic guard cancels and attempts bounded cleanup. The Control Plane marks liveness offline separately and waits for the persisted lease before transfer. An inaccessible daemon or crashed agent can leave execution alive. Asymmetric/half-open network faults are not exhaustively injected; instant detection is not guaranteed.

**Why worker identity versus worker session?**

`worker_id` is stable; `worker_session_id` identifies each process incarnation. Registration supersedes older live sessions; stale heartbeats/renewals cannot revive them. Supersession does not transfer an unexpired lease. History records the exact process holding each attempt.

**What happens if the Control Plane dies?**

Committed assignments remain in PostgreSQL. Workers cannot renew without ACKs and attempt to stop. Restart clears persisted connection hints; workers reconnect as fresh process sessions. Recovery applies existing expiry/cancellation/timeout/retry rules. Uncommitted assignments are not delivered. There is downtime: only one Control Plane exists.

**Why can an old Docker container survive a worker crash?**

The daemon owns it independently; SIGKILL does not run agent cleanup. Deterministic names and job/attempt/fence/worker/session labels permit reconciliation of old-session containers when that worker returns to an accessible daemon. Permanently inaccessible hosts require operator cleanup.

**How are stale results rejected?**

After the pipeline gate and job → attempt → session locks, PostgreSQL time is read. Completion checks identity, fences, current pointer, active state, nonsuperseded ONLINE session and lease; timeout/cancellation also constrain results. Exact duplicates of the current accepted winner get a read-only ACK. LOST/superseded attempts cannot overwrite it.

**How are scheduler races prevented?**

`FOR UPDATE SKIP LOCKED` claims eligible rows; state and capacity are rechecked under locks. Reservation, new attempt/fence and current pointer commit together before dispatch. Pipeline transitions acquire the outer pipeline gate and ordered job locks; fan-in releases a BLOCKED child transactionally once. Real PostgreSQL barriers prove both competing lock orders.

**What would need to change for multiple Control Plane instances?**

Database predicates help, but startup globally clears session connection flags and each process has its own live gRPC stream map. Multi-instance support needs explicit connection ownership/routing, safe startup/reconnect reconciliation, coordinated recovery/shutdown/migrations and failover/partition/duplicate-dispatch tests. That requires a separate design; no new service/broker is assumed here.

## Walkthrough

Run [demo-recovery.ps1](../scripts/demo-recovery.ps1), explain the fences/stale replay, then inspect [both attempts](assets/console/recovery.png). Explain why LOST does not prove physical shutdown. Show parallel branches and permanent failure with [demo-pipeline.ps1](../scripts/demo-pipeline.ps1). Finally use [demo-observability.ps1](../scripts/demo-observability.ps1) for fresh trace URLs. A crashed worker may leave an unfinished span; Control Plane recovery explains the persisted result.

Do not invent scale/uptime numbers or imply exhaustive proof. [Release claims and intentional nonclaims](release-audit.md).
