# Correctness audit: Gate C

Historical Gate C findings/evidence, retained without rewriting their original classifications. Later milestones add public cancellation, DAGs, observability and console inspection; current scope and verification are in [release-audit.md](release-audit.md).

Audited against architecture-v0.1.md and implementation-v0.1.md before changing code on 2026-10-05. PostgreSQL remains authoritative; the job -> attempt -> session lock order and lease/fencing model are retained. No public product surface is added.

## Findings before modification

| Requested case | Initial assessment | Existing evidence / gap |
|---|---|---|
| Duplicate completion | Safe, tested | Sequential and concurrent duplicate tests; exact terminal duplicates do not decrement slots again. |
| Completion after expiry before scanner | Safe, tested | Completion checks database time after locks; recovery test checks rejection before scanning. |
| Renewal racing completion | Safe by shared locks, untested | Both lock acquisition orders need explicit barriers. |
| Recovery racing completion | Safe, partially tested | Existing test starts both after expiry; valid-completion-first order is missing. |
| Cancellation racing success | Missing | No authoritative cancellation state/transition exists; stale-execution CancelAttempt is not logical cancellation. Add only an internal primitive and acknowledgement, without a public cancellation API. |
| Two scheduler loops | Safe, tested | Existing 32-way tests verify one claim and capacity; add a deterministic held-claim case. |
| Old session reconnect | Safe by unique session identity, untested | Re-registration must roll back supersession and leave the newer session intact. |
| Supersession / delayed heartbeat | Safe, tested serially | Add queued old heartbeat/renewal behind registration's actual session locks. |
| Delayed renewal from stale attempt | Safe, partially tested | Old session and expired renewal tested; add old attempt after a new fence is installed. |
| Commit then dispatch failure | Safe by lease reservation, untested | Verify failed transport does not release capacity or replace ownership. |
| Control Plane crash after commit | Safe by persistence, untested at exact window | General restart/lease-guard script exists; add exact-window process tests. |
| Crash after Docker start before persisted AttemptStarted | Safe because ASSIGNED owns a lease, untested at exact window | Add a real started container while the start transition is blocked, then interrupt the Control Plane. |
| Worker crash with surviving Docker container | Accepted physical limitation, partially tested | Existing recovery demo kills the agent but does not explicitly assert physical container survival. |
| Duplicate log chunks | Safe, tested serially / over gRPC | Add concurrent duplicate ingestion. |
| Slot leak / double decrement | Safe on tested paths | Existing active-attempt reconciliation assertions; extend to all races and rollbacks. |
| Ownership rollback | Transactional by inspection, untested | Inject PostgreSQL trigger failures at every write and deferred failures at commit; compare complete persisted snapshots. |

Two additional message-order races were found: publishing a session before queueing Registered permits RunAttempt to arrive first; committing completion releases database capacity before the result ACK is delivered, so a new assignment can reach a worker still retaining its finished result and be spuriously rejected. These need minimal ordering fixes and deterministic regressions.

Verification results and final failure semantics are recorded below after execution; this initial assessment is not a claim that uncovered cases have already passed.

## Implemented corrections

1. Queue Registered before publishing the control stream to scheduling.
2. Permit a new authoritative assignment after the previous executor/log drain finished even when its result ACK is delayed. Match delayed ACKs to the full identity.
3. Implement the missing architectural cancellation/completion transition internally. No public cancellation endpoint is added. Cancellation retains active ownership/capacity until a valid stopped-execution result or expiry, prevents renewal/success, and suppresses infrastructure retries.
4. During cancellation testing, an initial renewal ACK delayed behind CancelAttempt exposed a further race: the worker could start execution after cancellation. The real gRPC/PostgreSQL regression failed before the fix and passed after the worker was changed to ignore that ACK.

No architecture specification change was required. PostgreSQL is still authoritative; ownership locks remain job -> attempt -> worker session. Existing physical at-least-once and single authoritative terminal winner semantics are preserved.

## Verification coverage

| Case | Regression evidence |
|---|---|
| Duplicate completion | TestCompletionDuplicatesAndFencing; TestConcurrentDuplicateCompletion; Docker protocol idempotency test. |
| Expiry before scanner / expiry during lock wait | TestWorkerBToWorkerCRecovery; TestLeaseExpirationCompletionRace; TestCompletionBlockedPastLeaseDeadline. |
| Renewal vs completion | TestRenewalCompletionLockOrders, both actual lock acquisition orders. |
| Recovery vs completion | TestRecoveryCompletionLockOrders, expired recovery first and valid completion first; scanner skips the held job. |
| Cancellation vs success | TestCancellationCompletionLockOrders, both orders; TestCancellationExpiryAndQueuedJobs; TestUnrequestedCancellationRejected. |
| Concurrent schedulers / scanners | TestSchedulerConcurrency; TestDeterministicSchedulerAndRecoveryClaims, with a held first transition. |
| Old session reconnect | TestOldSessionCannotReregister, same worker and different worker; complete snapshots unchanged on rejection. |
| Supersession / delayed heartbeat | TestSessionSupersession; TestQueuedOldSessionMessagesAfterSupersession, real queued session locks. |
| Stale renewal | TestDelayedRenewalAfterNewFence; queued old-session renewal; crash tests replay the old renewal. |
| Committed assignment, failed delivery | TestCommittedAssignmentSurvivesDispatchFailure, transport Send error and full outgoing queue; no premature slot release. |
| Control Plane crash after commit | TestControlPlaneCrashImmediatelyAfterAssignmentCommit, committed reservation before any delivery; SIGKILL then restart/recovery. |
| Docker start before persisted start | TestControlPlaneCrashAfterDockerStartsBeforeStartedCommit, live labelled Docker container and a blocked start transaction; killed process cannot commit; rollback asserted. |
| Worker crash / surviving container | TestWorkerCrashLeavesDockerAliveUntilRecovery, SIGKILL, offline-before-expiry reservation, physical overlap with worker-c retry, new worker-b orphan reconciliation, stale replay. |
| Duplicate log chunks | TestLogIdempotency; TestConcurrentDuplicateLogs; Docker/gRPC duplicate protocol test. |
| Slot accounting | Active-attempt/session-slot equality in store race tests; exact retained/released totals in gateway, worker, cancellation, and process tests. |
| Ownership write/commit rollback | TestOwnershipWriteAndCommitRollback; TestCancellationRollback. Faults at each mutation and deferred commit; all five tables compared including timestamps, identities, and slots. |
| Registration and result ACK ordering | TestRegisteredQueuedBeforeSessionPublication; real Connect dispatch-failure integration; TestWorkerMessageOrderingWithPostgres including next assignment before result ACK, running cancellation, and cancel before initial renewal ACK. |
| Cancellation physical stop | TestInternalCancellationStopsRealDockerBeforeReleasingSlot, a real stopped execution acknowledged without retry. |
| Migration compatibility | TestUpgradeAndIdempotentCancellationMigration, initial-schema upgrade, repeat application, retained data, and exactly five tables. |

The store races are deterministic database-barrier tests, rather than assertions based on goroutine launch order. AFTER triggers pause a transition while it holds real ownership locks; pg_stat_activity confirms the competing transaction's lock wait. Recovery/scheduling tests separately assert SKIP LOCKED behavior. Trigger exceptions and deferred constraint triggers force PostgreSQL rollback at write and COMMIT boundaries; no transaction semantics are mocked.

Process tests use isolated schemas, production components in separate OS processes, real gRPC, and the accessible Docker daemon. The start-crash barrier is released after process death so PostgreSQL can detect the dead socket and unwind its explicit transaction; the test verifies ASSIGNED/DISPATCHED remain persisted rather than allowing the failed transaction to count as started. The helper entry test intentionally skips in the parent and runs only in children.

## Verified semantics and limits

- Lease validation under acquired ownership locks is the linearization point. A valid completion retaining those locks can commit after expiry; recovery cannot install another owner while that transition holds the job lock. Completion acquiring its locks after expiry fails even before scanning.
- Offline marking does not transfer ownership. Dispatch failure or Control Plane crash leaves the assignment reserved until expiry. Restart clears connection eligibility hints, not attempt ownership.
- Cancellation intent is durable and serialized with completion. Slots decrement only on accepted terminal result or expiry recovery, never on cancellation delivery or liveness loss. Cancellation retries no workload.
- Exact duplicate terminal results are read-only acknowledgements. Stale identities/fences/sessions, expired authority, and conflicting terminal results cannot replace the winner. Logs remain diagnostic and deduplicate independently.
- Rollback guarantees apply to each transaction. Offline marking and due-retry requeueing are distinct transactions from ownership recovery; a whole scanner iteration is not a single atomic batch.
- Worker SIGKILL can leave a Docker container alive and physical executions can overlap. Reconciliation works on the accessible daemon for a returning worker identity. Docker cleanup is best effort when unavailable; fences do not protect arbitrary external workload side effects. No exactly-once or scale claim follows from these tests.

Verification commands from the repository root:

```powershell
./scripts/verify.ps1
./scripts/verify-e2e.ps1
./scripts/verify-recovery.ps1
./scripts/demo-normal.ps1
./scripts/demo-recovery.ps1
./scripts/test-leaseguard.ps1
```

The first script checks formatting, vet, compilation, unit tests, and all race-enabled PostgreSQL integration tests. The next two run race-enabled real Docker/protocol and OS-process crash suites, respectively. Tests run with -count=1. The demos additionally prove the ordinary path, worker-b -> worker-c recovery with STALE_ATTEMPT_REJECTED, and local monotonic lease enforcement while the Control Plane is stopped.

Final verification on 2026-10-05: **all six commands passed**. The full PostgreSQL suite passed under the Go race detector, including write/commit rollback and migration tests. The Docker suite passed success/idempotency and all three workload failure cases. The final process suite passed all four crash/cancellation tests in 74.099s; only the parent subprocess-helper entry intentionally skips. Demo markers were NORMAL_EXECUTION_PASSED, STALE_ATTEMPT_REJECTED, WORKER_B_TO_WORKER_C_RECOVERY_PASSED, LOCAL_LEASE_GUARD_STOPPED_EXECUTION_WITHOUT_CONTROL_PLANE_ACKS, and CONTROL_PLANE_RESTART_RECOVERY_PASSED. The initial start-crash test barrier timeout was corrected as described above; the final full run passed with rollback assertions retained. No commit or push was made.
