# Milestone 3: pipeline semantics audit

Historical Milestone 3 scope/evidence. Its frontend/observability exclusions describe that stage; Milestones 4–5 subsequently implement those layers. See [release-audit.md](release-audit.md) for current verification and limitations.

Architectural sources: architecture-v0.1.md sections 26–29 and 39–40, implementation-v0.1.md, and the verified Gate C audit. Gate C was checkpointed and pushed as `b295eccab7e2d198105dbdedeba94c1b55bf1910` before implementation. No architectural blocker or additional infrastructure was required.

## DAG validation and states

Submissions contain 1–128 jobs and at most 2,048 static dependency edges. Declaration order is irrelevant. Validation before the database transaction checks job specs, unique nonempty bounded job keys, dependency existence, self-dependency, duplicate edges, and cycles across the complete graph. Invalid DAGs persist no partial or runnable work. Database uniqueness/composite foreign keys reinforce key uniqueness and prevent duplicate, self, and cross-pipeline edges. There is no mutation/dynamic expansion API. Existing standalone jobs retain nullable pipeline membership and their original submission route.

| Job transition | Condition |
|---|---|
| Initial → QUEUED | No dependencies. |
| Initial → BLOCKED | Has required dependencies. |
| BLOCKED → QUEUED | Every required parent logically SUCCEEDED. |
| BLOCKED → SKIPPED | A required parent is permanently FAILED, CANCELLED, or SKIPPED. |
| QUEUED → DISPATCHED → RUNNING | Existing transactional assignment and worker execution. |
| DISPATCHED/RUNNING → RETRY_WAIT → QUEUED | Retryable infrastructure failure with attempts remaining. |
| Active → SUCCEEDED/FAILED | Accepted terminal result and bounded retry decision. |
| BLOCKED/QUEUED/RETRY_WAIT → CANCELLED | Committed cancellation, no active reservation. |
| DISPATCHED/RUNNING → CANCELLING → CANCELLED | Valid stopped result or expired-lease recovery. |

Terminal jobs are SUCCEEDED, FAILED, CANCELLED, SKIPPED. A TIMED_OUT attempt produces a FAILED job. SKIPPED jobs never acquire an attempt/fence. There is no pipeline fail-fast: unrelated branches continue.

Pipelines start RUNNING. Whole-pipeline cancellation sets CANCELLING while any job remains nonterminal. Finalization waits for **all** jobs to be terminal, with this priority: whole-pipeline cancellation intent → CANCELLED; any FAILED job → FAILED; otherwise any individually CANCELLED job → CANCELLED; otherwise any SKIPPED job → FAILED; otherwise all succeeded → SUCCEEDED. Already terminal jobs retain history. Individual cancellation does not hide an unrelated failed job. Whole-pipeline cancellation cannot overwrite an already terminal pipeline.

## Dependency release and retries

Attempt completion/expiry updates attempt, logical job, and reserved slot, then propagates the DAG and aggregates the pipeline in the same transaction. BLOCKED descendants with permanent failed dependencies become SKIPPED to a fixed point; eligible remaining children then become QUEUED. No client can observe committed parent success without eligible child release or permanent parent failure without downstream propagation.

Dependency evaluation uses logical job state, not historical failed/lost attempts. RETRY_WAIT and retry QUEUED/DISPATCHED/RUNNING are nonterminal. Remaining retry capacity prevents premature skipping. Retry success releases children; exhaustion produces FAILED and transitive skips. Every retry has a fresh attempt and greater fence. Stale parent messages cannot release descendants. Exact duplicate terminal winners are read-only acknowledgements and repeat neither release nor aggregation.

## Schema, transactions, and locks

Migration 003 adds pipelines and immutable job_dependencies, nullable job membership/key columns, BLOCKED/SKIPPED states, and write-once attempt execution_deadline_at. Seven authoritative tables retain existing data; initial-schema upgrade and reapplication tests remain enabled.

| Operation | Atomic boundary |
|---|---|
| Submit | Validate first; insert pipeline, all jobs, all edges; commit once. |
| Schedule | Candidate IDs are hints; gate/locks, eligibility recheck, session reservation, new attempt/fence/current pointer; commit before dispatch. |
| Accept/start/renew | Gate/ownership locks; identity, session, lease, cancellation, timeout checks; mutation; commit before ACK. |
| Complete | Gate/ownership checks; attempt/job/slot mutation, dependency release/skips, pipeline aggregate; one commit before ACK. |
| Recover | Gate/locks, expired-lease recheck, loss/timeout/cancellation outcome, slot release, DAG propagation/aggregate; one commit per attempt. |
| Requeue | Gate/job locks; recheck due RETRY_WAIT; QUEUED; commit per job. |
| Cancel job | Gate/ownership locks; reuse existing cancellation transition, propagate/aggregate; commit before notification. |
| Cancel pipeline | Gate/all job locks; active attempts/sessions sorted; durable intent and cancellation of all nonterminal jobs, aggregate; one commit before notifications. |
| Inspect | One read-only REPEATABLE READ transaction for pipeline, jobs, edges, and attempt histories. |

For pipeline transitions, lock the pipeline row FOR UPDATE, then all its jobs in ID order, **before** any attempt/session lock. This retains job → attempt → worker-session ordering and avoids acquiring an unlocked child after holding a parent's session. Whole-pipeline cancellation prelocks all active attempts and sessions in ID order before reusing per-job logic. Standalone jobs retain their original lock order.

Scheduling/recovery try the outer gate with SKIP LOCKED before candidate job locks and recheck state under locks. They never hold a job while waiting for its gate. Ownership checks obtain PostgreSQL time after required locks. Registration/heartbeats/offline marking never acquire pipeline/job locks. Offline marking and retry requeue remain separate from ownership recovery; a scanner iteration is not one atomic batch.

The gate intentionally serializes coordination within a bounded pipeline; Docker jobs still execute in parallel. This is a single-Control-Plane correctness choice, not a scale/throughput claim. No process-local mutex decides readiness or final state.

## Cancellation semantics

HTTP job/pipeline cancellation exposes the verified internal primitive. BLOCKED/QUEUED/RETRY_WAIT jobs cancel directly. Active jobs keep their current attempt, fence, lease, and slot until a valid stopped result or lease expiry. Whole-pipeline cancellation atomically cancels all nonterminal jobs, including blocked children. Individual parent cancellation causes blocked descendants to be SKIPPED while unrelated branches continue. Cancellation suppresses retries during worker loss or timeout.

CancelAttempt notifications are best effort. Rejected renewals/results, monotonic local guards, and expiry recovery handle missed notification. Cancellation committing first invalidates racing success; accepted success committing first returns ALREADY_TERMINAL to cancellation. In-progress duplicate cancellation is acknowledged; terminal requests return HTTP 409. Cancellation ACK still needs valid lease and identity/session authority. Physical shutdown remains best effort when Docker is inaccessible.

## Timeout semantics

The first successful renewal establishes execution_deadline_at; start supplies a fallback for direct protocol callers. The deadline is write-once per attempt. Queue/dependency wait is excluded. The execution budget includes image preparation, execution, cleanup/log draining, and accepted result reporting: a physically finished job with success arriving after deadline is conservatively rejected. Retries receive a new per-attempt budget.

LeaseRenewed carries independent lease TTL and execution_budget_ms. Workers derive a monotonic execution deadline from request-send time plus the remaining budget, capped by the assignment timeout. Later ACKs do not extend that timer. An exhausted initial budget cannot start Docker; zero means exhausted, so Control Plane/workers must be upgraded together. No synchronized wall clock is assumed.

Completion rejects non-timeout success/failure after execution deadline without waiting for scanning. TIMED_OUT stopped results still require current identity/fence, active state, ONLINE session, and valid lease. Expired execution rejects renewal and prompts EXECUTION_TIMEOUT cancellation, without releasing a slot. Without a valid stopped result, recovery waits for lease expiry and records TIMED_OUT/JOB_TIMEOUT, logical FAILED, no workload retry. If lease expiry occurs before its budget, ordinary LOST/bounded infrastructure retry applies. Committed cancellation takes precedence during recovery.

Recovery compares persisted execution and lease deadlines, rather than scanner arrival time. A long Control Plane outage can leave both expired: an earlier lease expiry remains infrastructure loss with bounded retry; an earlier execution deadline (or equal deadlines) remains workload timeout. Final review caught and corrected an implementation that otherwise reclassified earlier lease loss when scanning late.

The post-lock validity check remains the linearization point. A valid completion retaining its locks may commit after a later timeout/lease instant; no competing owner can be installed. A request obtaining locks after expiry cannot commit success. Exact duplicates remain read-only.

## Race analysis and test evidence

| Case | Evidence |
|---|---|
| Single/linear/fan-out/fan-in/diamond | TestPipelineSuccessShapes; TestDockerPipelineSuccessShapes. |
| Missing/self/cyclic/duplicate keys | TestStaticDAGValidation; TestPipelineInvalidDAGDoesNotPersist; HTTP validation tests. |
| Premature blocked scheduling | Success-shape tests call the scheduler on blocked children and require no assignment. |
| Transitive failure and unrelated branch | TestPipelineFailureSkipsTransitivelyWithoutFailFast; real Docker failed branch skips package while lint continues. |
| Retry capacity/success/exhaustion | TestPipelineRetryReleaseAndExhaustion; TestPipelineRecoveryReleasesDependencyOnlyAfterValidRetrySuccess. |
| Concurrent parents / final sibling success vs retry failure | TestPipelineConcurrentParentsReleaseOnce, both parent lock orders, with exactly-one-release audit trigger. |
| Cancellation vs release/scheduling | TestPipelineCancellationDependencyReleaseLockOrders; TestPipelineSchedulerCancellationLockOrders, both orders. |
| Cancellation vs success | Gate C job tests; TestPipelineCancelCompletionLockOrders. |
| Cancellation in every state / worker loss | TestPipelineCancelEveryJobStateAndWorkerLoss; TestWholePipelineCancellation. |
| Timeout vs success / lease expiry | TestPipelineTimeoutCompletionAndExpiry; TestPipelineTimeoutLeaseRecoveryLockOrders, both winners. |
| Late scanner after both deadlines | TestPipelineRecoveryClassifiesFirstExpiredAuthority proves both persisted deadline orders, retry fencing/dependency retention for lease-first, and permanent downstream skip for timeout-first. |
| Non-resetting deadline / delayed initial ACK | TestPipelineRenewalsNeverResetExecutionDeadline; real gRPC/PostgreSQL worker tests for timeout before initial ACK and exhausted-budget ACK. |
| Concurrent aggregation / active branch | TestPipelineConcurrentTerminalAggregation; failure/cancellation tests forbid premature finalization. |
| Write/commit rollback | TestPipelineOwnershipAndPropagationRollback; TestPipelineDependencyPropagationRollbackAfterParentMutation fails at release and second-level skip after parent/slot writes; seven-table snapshots stay unchanged. |
| Public cancellation and real timeout | HTTP integration tests; TestDockerPipelineTimeoutAndPublicCancellation. |
| Failed Send while Recv blocks | Prior dispatch test exposed a timing gap. Connect now observes send errors independently of Recv; TestCommittedAssignmentSurvivesDispatchFailure requires termination without another heartbeat. Ownership remains reserved. |

PostgreSQL triggers/advisory barriers and observed lock waits establish ordering; no transaction mocking or guessed goroutine timing is used. Existing Gate C tests still run. The DAG recovery demo kills worker-b inside build → test → package, requires package to remain BLOCKED before expiry and during retry, rejects stale replay, and proves worker-c retry success releases package.

## Verification and limitations

```powershell
./scripts/verify.ps1
./scripts/verify-e2e.ps1
./scripts/verify-recovery.ps1
./scripts/demo-normal.ps1
./scripts/demo-recovery.ps1
./scripts/test-leaseguard.ps1
./scripts/demo-pipeline.ps1
./scripts/demo-pipeline-recovery.ps1
```

Final verification on 2026-10-05 passed all eight commands above with exit status 0 after the late-scanner fix. Formatting, integration-tag vet, and full build passed; race-enabled uncached domain/PostgreSQL/HTTP/worker tests passed (PostgreSQL package 34.193s). Real Docker end-to-end tests passed (32.353s), including every DAG shape, concurrent branches, permanent failure, timeout, and public cancellation. The four separate-process recovery tests passed (74.847s), including the surviving Docker container case.

Normal/recovery demos verified both log streams, offline-before-expiry ownership, fenced retry, and stale replay rejection. The lease-guard script verified Docker stopping without Control Plane ACKs and successful restart recovery. Pipeline demos emitted PIPELINE_PARALLEL_BRANCHES_RUNNING, PIPELINE_SUCCESS_PASSED, PIPELINE_FAILURE_SKIPS_PACKAGE_PASSED, and PIPELINE_WORKER_B_TO_WORKER_C_RECOVERY_PASSED. The recovery demo retained the dependent BLOCKED before expiry and during retry; only valid retry success released it. Existing assertions remain enabled; migration table-count and rollback snapshots were extended to the two new authoritative tables.

Limits: one Control Plane, bounded static DAGs, no submission idempotency key or DAG edits. Matrix, expressions, fail-fast, dynamic DAGs, artifacts, secrets, authentication, frontend, brokers, and observability infrastructure remain deferred. Log/history retention is unchanged. Worker crashes can leave Docker containers alive and physical executions may overlap. Inaccessible daemons cannot be cleaned up; terminal state does not prove remote physical shutdown. Fences protect ForgeGrid state, not arbitrary workload side effects. Physical execution remains at least once with a single authoritative terminal winner; no scale or exactly-once claim is made.
