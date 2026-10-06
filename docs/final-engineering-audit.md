# Final engineering audit — Milestone 6

Historical Milestone 6 findings and its 2026-10-05 verification are preserved here. Milestone 7 adds release presentation/licensing and a fresh complete run in [release-audit.md](release-audit.md); it does not replace this audit's original evidence.

Audit date: 2026-10-05. Baseline: c5db1bdac361eaf7db6d45db48a72e2cccae43a4, pushed to origin/main as `feat: add ForgeGrid operational console`. That checkpoint includes the previously uncommitted, verified observability and console work. This audit's changes are intentionally uncommitted.

Scope: a trusted, local, single-Control-Plane portfolio system. Reviewed architecture/implementation/failure/execution/protocol documentation, all migrations, protobuf contracts, coordination transitions, transport lifecycle, worker guards/executor, telemetry, inspection/frontend, verification scripts and critical test assertions. This is an implementation audit with executable evidence, not a formal proof, penetration test or certification for production deployment. No product features or infrastructure were added.

## Findings reported before fixes

### F1 — Cancelling a SKIPPED job rewrites a terminal outcome

- Severity: **Medium**.
- Location: `internal/store/postgres/store.go`, `cancelJob`.
- Scenario: permanent parent failure/cancellation has already skipped descendants. Direct public cancellation of a skipped descendant returns 202, changes SKIPPED to CANCELLED, adds cancellation intent and rewrites its finished timestamp. This can happen while an unrelated branch is active or after the pipeline finalized.
- Why it matters: terminal DAG history is no longer immutable; the documented terminal cancellation conflict is false. The console disabling that action is insufficient protection for the API.
- Minimum fix: use existing `domain.TerminalJob` instead of an incomplete hand-written terminal list. No lock, lease, fencing or retry changes.
- Release blocked: **yes until fixed and verified**.
- Evidence: new PostgreSQL and HTTP regressions failed before the fix (nil error/202) and pass after it. The store regression snapshots all seven tables, checks child and transitive descendant, repeats cancellation, and checks both active/terminal pipeline phases and slot accounting. HTTP regression requires 409 and identical inspection history.

### F2 — Known vulnerabilities in backend dependencies

- Severity: **High**, considering transport resource exhaustion and exporter memory exhaustion; individual advisories have different prerequisites.
- Location: `go.mod`, `go.sum`.
- Scenario: initial `govulncheck` reports ten advisories in required/reachable packages. OTLP response bodies were not size limited; a misconfigured or malicious collector could cause excessive allocation/OOM despite a bounded span queue and timeout. gRPC has HTTP/2 transport vulnerabilities. An available collector is not necessarily a well-behaved collector.
- Why it matters: exporter failure isolation does not imply safety against unbounded response memory. Known transport vulnerabilities should not be shipped as the public audit baseline.
- Minimum fix: pgx 5.9.2, gRPC 1.83.2, OpenTelemetry 1.45.0, their required transitive versions, then `go mod tidy`; Go 1.25 remains sufficient. gRPC 1.83.1 fixes fragmentation but is still in an affected authority-header range, hence 1.83.2. No protocol/schema redesign.
- Release blocked: **yes until patched, rescanned and regression tested**.
- Evidence: after update, source `govulncheck` reports no vulnerabilities. A real HTTP exporter regression checks oversized 200 and 503 collector responses are rejected as body-too-large and are not retried. Existing non-blocking/timeout/queue tests remain enabled.

Advisory assessment (scanner reachability is not proof that every exploit prerequisite exists):

| Advisory | Published behavior | ForgeGrid assessment |
|---|---|---|
| [GO-2026-4985](https://pkg.go.dev/vuln/GO-2026-4985) | OTLP HTTP response memory unbounded | HTTP exporter is used; directly relevant to telemetry isolation. |
| [GO-2026-6348](https://pkg.go.dev/vuln/GO-2026-6348) | HTTP/2 DATA fragmentation memory exhaustion | gRPC client/server transport is used. |
| [GO-2026-6061](https://pkg.go.dev/vuln/GO-2026-6061) | HTTP/2 transport / xDS RBAC issues | Transport is used; xDS/RBAC is not configured. |
| [GO-2026-6443](https://pkg.go.dev/vuln/GO-2026-6443) | Missing authority/Host panic with xDS routing | xDS routing is not configured; patched dependency still warranted. |
| [GO-2026-4762](https://pkg.go.dev/vuln/GO-2026-4762) | gRPC authorization bypass via path handling | No authorization layer is implemented; do not claim a demonstrated auth bypass here. |
| [GO-2026-5970](https://pkg.go.dev/vuln/GO-2026-5970) | Invalid UTF-8 normalization can loop | Transitive normalization symbols reported; not a reproduced exploit. |
| [GO-2026-5004](https://pkg.go.dev/vuln/GO-2026-5004) | pgx simple-protocol dollar-quote substitution injection | Production workload queries are parameterized/default protocol; migration SQL has no untrusted substitutions. No demonstrated ForgeGrid SQL injection. |
| [GO-2026-5506](https://pkg.go.dev/vuln/GO-2026-5506) | Baggage parsing excessive allocation | Only TraceContext propagation is configured, no baggage. |
| [GO-2026-4394](https://pkg.go.dev/vuln/GO-2026-4394) | SDK PATH hijacking | Resources are explicit/schemaless; OS resource detection is not enabled. |
| [GO-2026-6505](https://pkg.go.dev/vuln/GO-2026-6505) | Exporter config logging can leak endpoint URLs | Advisory was marked unreviewed at audit time. Compose has no endpoint credentials; generic operational error logging remains. Patched along with the exporter. |

The scanner itself requires Go 1.26: `GOTOOLCHAIN=auto` is used only for installing that tool in a disposable tools container. Application builds and tests remain on the Dockerfile's Go 1.25. Frontend `npm audit` is separate. Neither check is a complete scan of operating-system/container image packages, nor a permanent zero-vulnerability guarantee.

### F3 — Recovery metrics demo can pass using old history

- Severity: **Medium**.
- Location: `scripts/demo-observability.ps1`.
- Scenario: nonzero lease-expiration/retry counters from an earlier demo satisfy the current demo's regex even if new metric sampling never reflects the current recovery.
- Why it matters: documented persistent metric increment evidence is stronger than the assertion actually proves.
- Minimum fix: record both closed-name counters immediately before recovery; require each to increase by at least one afterward. Escape metric names and parse numbers with invariant culture, including scientific notation. No database deletion to manufacture a zero baseline.
- Release blocked: **yes for the claimed demonstration until corrected**.
- Evidence: strengthened real recovery demo is part of the final matrix. An explicit current-recovery increment marker supplements, rather than replaces, trace and scrape assertions.

### F4 — Malformed SSE payload escapes the operational error state

- Severity: **Medium**.
- Location: `frontend/src/components.tsx`, `LiveLogs`.
- Scenario: JSON `null` or invalid base64 reaches `appendChunk` inside a deferred React state updater. The surrounding event-handler catch cannot catch later updater execution; the console can unmount instead of showing Invalid log data.
- Why it matters: inspection should retain its last useful output and expose failure honestly. Production server output is bounded, valid base64; this is defensive handling of broken data, not a demonstrated hostile-workload escape.
- Minimum fix: catch decoding failure inside the updater and retain the existing buffer/cursor.
- Release blocked: **yes until the existing error behavior is reliable**.
- Evidence: new component regression failed before the fix, passes after it, keeps prior output/cursor, and accepts a subsequent valid chunk. No view redesign or authoritative frontend state was added.

### F5 — Conceptual submission idempotency can be mistaken for an API guarantee

- Severity: **Low**.
- Location: `docs/architecture-v0.1.md`, section 32.
- Scenario: conceptual idempotency-key/request-hash columns and 409 behavior are presented without a local implementation-status note. README and implementation audits correctly list these as deferred.
- Why it matters: callers must not assume repeating POST is safe after losing a response.
- Minimum fix: annotate that section; retain the conceptual architecture without implementing a feature.
- Release blocked: **no**, but materially misleading scope wording was clarified.

### F6 — Bounded chunks do not bound total host/database usage

- Severity: **Medium**, non-blocking in the explicitly trusted local scope.
- Location: `internal/worker/docker.go`, `job_log_chunks` and retained histories.
- Scenario: a trusted workload can consume host CPU/RAM and produce large Docker/PostgreSQL logs over time. Chunk/queue/browser bounds do not impose total storage retention or workload resource quotas. Worker agents mount the Docker socket and have daemon-level authority; workload containers do not receive it.
- Why it matters: the executor must not be advertised as hostile multi-tenant isolation or resource containment.
- Minimum fix: preserve the explicit trusted-local limitations; no speculative quotas/security features in this audit.
- Release blocked: **no for a portfolio release with these limitations**; not approved for hostile/public execution.

### F7 — Network fault coverage is narrower than every possible partition

- Severity: **Low**.
- Location: lease-guard, worker protocol and separate-process recovery tests/scripts.
- Scenario: real Control Plane interruption and missing/delayed ACK behavior are verified, but a general asymmetric packet-drop/blackhole fault harness and a real PostgreSQL daemon-outage/failover suite do not exist. Connection establishment/reconnection also relies on transport error detection; it is not a bounded universal network recovery SLA. SSE replay/cursor semantics have server integration tests and component-event tests, but no dedicated browser TCP-drop test asserting the browser's emitted Last-Event-ID header.
- Why it matters: do not turn missing-ACK evidence into a claim that all network failures were exhaustively fault-injected.
- Minimum fix: record the scope of evidence, retain existing assertions; no new infrastructure.
- Release blocked: **no**.

### F8 — Windows PowerShell HTML parsing confirmation fails in a noninteractive host

- Severity: **Medium**.
- Location: `scripts/demo-pipeline-recovery.ps1` and `scripts/demo-observability.ps1`, three `Invoke-WebRequest` calls.
- Scenario: the final matrix under Windows PowerShell 5.1 fails in pipeline submission with `System.NullReferenceException`. A read-only `/healthz` request reproduces the same error without flags; the same endpoint returns 200 with `-UseBasicParsing`. The exception stack identifies `InvokeWebRequestCommand.ProcessResponse` → `ShouldContinue` → `ConsoleHostUserInterface.PromptForChoice`: the HTML/script parsing confirmation cannot be handled by this noninteractive host. The initial diagnosis identified the parsing path; the stack localizes the immediate failure to its confirmation prompt, rather than proving an unavailable browser engine.
- Why it matters: JSON/metrics demos should use basic parsing rather than enter an interactive HTML/script parsing path. README recommends PowerShell 7, but maintaining Windows PowerShell compatibility is also an existing constraint.
- Minimum fix: `-UseBasicParsing` on those requests. HTTP requests, headers, payloads and assertions stay unchanged; the flag is accepted by PowerShell 7 too.
- Source: [Microsoft's Windows PowerShell 5.1 documentation](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.utility/invoke-webrequest?view=powershell-5.1) describes the security-update parsing confirmation and recommends basic parsing to avoid web-script execution. The fix selects that safer parsing mode; it does not accept or automate the risky prompt.
- Release blocked: **yes for Windows PowerShell demo verification until fixed and rerun**.
- Evidence: initial 5.1 recovery run failed; explicit health probe distinguishes the failing/default path from basic parsing. The corrected full recovery and observability scripts are rerun in the same 5.1 host. A read-only probe under PowerShell 7 checks compatibility as well.

### F9 — SDK upgrade exposes incorrect generic endpoint override

- Severity: **Medium**; discovered during audit patch verification, not a confirmed failure of the original 1.38 baseline.
- Location: `internal/observability/tracing.go`, exporter initialization.
- Scenario: OpenTelemetry 1.45 treats `WithEndpointURL` as the complete signal URL. Passing generic `http://otel-collector:4318` explicitly makes trace export POST `/` instead of `/v1/traces`. Coordination succeeds, but the strengthened real demo cannot find its trace. A disposable exporter probe confirms the exact 404 URL; no production debug endpoint was added.
- Why it matters: security dependency updates need behavioral verification. Existing unavailable/timeout tests did not assert the HTTP signal path and could pass while successful export was broken.
- Minimum fix: remove the explicit generic-URL override and let the SDK read its standard environment configuration, which appends the trace signal path to the base URL. Timeout, no-retry, queue limits and authority semantics remain unchanged.
- Release blocked: **yes until export is restored and the complete matrix is rerun**.
- Evidence: four real HTTP-server regression cases failed before the fix: base URL, trailing slash, base path prefix and explicit trace signal override. They pass after it. The full observability demo still requires all original distributed spans, distinct retry attempt, worker-c execution, current metric increments and four scrape targets.

## Distributed correctness and PostgreSQL review

Ownership candidates and connected-stream maps are hints. Scheduling reserves capacity, creates a new immutable attempt identity/fence and updates the current pointer in one PostgreSQL transaction; only then is dispatch attempted. Dispatch loss/crash leaves the reservation intact until lease expiry. No stream error performs immediate ownership transfer.

Standalone ownership locks are job → attempt → worker-session. Pipeline operations acquire pipeline gate → all jobs sorted by ID before attempt/session locks. Whole-pipeline cancellation prelocks attempts and sessions in ID order. Registration is worker → session and never acquires job/attempt locks; heartbeat/offline marking are session-only. Scheduling/recovery use SKIP LOCKED on candidate/gate paths, not to bypass validation during ownership mutation.

Clock validity is read after all required locks using `clock_timestamp()`. The locked validity check is the linearization point: an already-valid transaction can commit after the wall-clock expiry while retaining exclusive ownership locks. It does not permit a late request that acquired locks after expiry to succeed. Scanner execution is not a prerequisite for rejecting expired results.

Current attempt, matching attempt/session/fence, active attempt, session authority and lease are independently checked. Renewal also checks connection/cancellation/execution budget. An exact duplicate of the current accepted terminal result is a read-only ACK even after its old lease expires; this is not a fresh grant of authority. LOST/conflicting results cannot finalize another winner. Terminal job cancellation now consistently includes SKIPPED.

Attempt terminal mutation, logical job outcome, slot decrement, dependency release/fixed-point skip propagation and pipeline aggregation share one transaction. Cancellation retains active reservations until stopped-result authority or lease expiry. Retry bounds include the first execution; workload failure/timeout do not receive infrastructure retries. Cancellation takes precedence when recovery resolves lost ownership. Timeout-vs-lease classification uses the persisted order of deadlines, not scanner arrival time.

Constraints cover valid states, capacity, unique attempt numbers/fences, at most one active attempt/job, one online session/worker, own-job current-attempt FK, DAG key/edge identity and unique log sequence. Arbitrary external SQL is not a supported coordination interface; schema constraints alone do not prove all application invariants. Indexes match candidate/current/history access. Migration application is one advisory-locked transaction; fresh/legacy/reapplication tests preserve data. There is no migration checksum ledger or rolling mixed-version upgrade guarantee.

Seven-table snapshots and real PostgreSQL immediate/deferred trigger faults test rollback at ownership and DAG mutations. Lock-wait/advisory barriers establish race order instead of guessing goroutine order. These tests do not mock transaction semantics. No additional ownership, fencing, slot or dependency defect was confirmed by this audit beyond F1.

## gRPC, worker and Docker review

Registration precedes publication/assignment; every process uses a new session UUID. Replaying an old UUID cannot revive it. Superseded heartbeats/renewals fail, while an old reserved attempt still waits for expiry before recovery. Control send failures are observed independently of blocked receive. Queues and operation deadlines are bounded; logs use a separate connection/stream with persistence ACKs and deduplicated sequence numbers.

Workers start only after successful initial renewal ACK. A monotonic deadline is derived from request send time, TTL and safety margin; delayed/old/unsolicited ACKs grant no new authority. Execution budget is independently capped and not renewed. Context cancellation and guard expiry attempt force removal; result acceptance remains a PostgreSQL decision.

Docker commands are argv via `exec.CommandContext`, not interpolated host-shell commands. Deliberate shell commands in demo payloads run inside the requested workload container. Containers carry job/attempt/fence/worker/session labels, have no workload Docker socket/host mounts, network disabled, capabilities dropped and no-new-privileges. This is defense in depth for trusted local workloads, not a hostile sandbox. The agent itself needs the daemon socket.

Deterministic naming permits cleanup even after a lost create response. Cleanup/reconciliation use independent bounded contexts. Startup reconciliation removes old-session containers for the same worker identity. SIGKILL can leave Docker execution alive and overlap its retry; inaccessible hosts/daemons cannot be reconciled remotely. Cancellation terminal state does not prove physical remote shutdown. Application shutdown stops gRPC rather than promising to drain every pending stream/result; lost ACKs/recovery remain part of the failure model.

## Telemetry and frontend review

Submission HTTP spans end with their response. Persisted traceparent metadata and assignment propagation connect later scheduling, distinct attempts, worker/Docker/result and recovery/DAG spans. The long-lived worker stream is not every attempt's trace parent. Rolled-back spans can exist; committed flags and PostgreSQL distinguish authoritative transitions.

Span batching is non-blocking and bounded; exports have timeout/no retry, bounded shutdown, and now a patched bounded-response exporter. Read-only metrics sampling uses a separate one-connection pool, timeout and cached immutable snapshot. Missing telemetry may lose spans or retain a stale metric snapshot, never grant authority. History counters reflect retained rows and can decrease if an operator deletes history. No IDs, sessions, containers or arbitrary error text become application Prometheus labels. Workload output is not duplicated into operational metadata.

Inspection uses read-only snapshots; frontend polling/SSE/browser time never mutate execution authority. Cancel/submit invoke existing public transitions and refresh persisted state. LOST attempts and diagnostic logs remain visible after retry success. Recovery chronology uses persisted assignments/start/end/lease/last heartbeat; it explicitly does not invent an exact offline-transition timestamp. DRAINING is not implemented and is not fabricated by the UI. SSE uses persisted sequence/Last-Event-ID with duplicate rejection and bounded browser memory; disconnected/stale/loading/error views are explicit. F4 additionally preserves logs on malformed payloads.

## Repository and test credibility

The M5 checkpoint excluded pre-existing untracked `ersPCProjectsforgegrid` and `tatus --short` terminal-output files. They remain untouched and outside history. Node/build/browser reports, .env and executable output are ignored; tracked-file inspection found no runtime logs/database artifacts. No runtime output is intended for the release source tree. Targeted private-key/token patterns were checked across all reachable Git history without printing credential contents, with no matches; this is not proof that every possible secret format is detectable. Compose's `forgegrid` database password is an explicitly local development default, not a production credential. Host API/database/console/telemetry ports bind loopback in Compose; native binaries need a trusted network. No auth/TLS/debug administration feature is claimed.

Dependency evidence is version/date specific. Go source reachability and npm lockfile audit do not replace image/OS vulnerability scanning or a penetration test. No unsupported claim of a completely secure repository is made.

Real process tests use production components in OS subprocesses and kill them in PostgreSQL-selected crash windows, with separate isolated schemas and uniquely labelled Docker containers. The subprocess-helper parent entry intentionally skips; the four actual crash/cancellation scenarios must execute. The browser recovery scenario uses real Compose workers and Docker: worker-b dies, old attempt remains LOST/fence 1, worker-c succeeds/fence 2, historical logs remain inspectable and the dependent releases. Fixture browser tests complement this proof rather than replacing it.

Demos poll bounded external conditions. Their offline-before-expiry observation assumes the local host remains responsive within the configured 6s/10s window; severe host stalls can fail the demo rather than silently weaken its assertion. No throughput, scale or uptime benchmark is present.

## Final verification matrix

Final run on 2026-10-05: **all eleven scripts passed with exit status 0 after all fixes**, using Windows PowerShell 5.1. Before that run, no jobs were active; all Compose containers/network were removed and recreated. PostgreSQL data and Docker volumes were retained, not wiped. Tests create isolated schemas. No assertion was removed or weakened. PowerShell 7 basic-parsing compatibility was also checked with a read-only request.

| Command | Final result / evidence |
|---|---|
| `./scripts/verify.ps1` | **PASS** — format, integration-tag vet, full build, uncached race-enabled unit/real PostgreSQL integration suites; PostgreSQL package 32.763s, observability 5.230s. |
| `./scripts/verify-e2e.ps1` | **PASS** — real Docker execution/protocol idempotency, workload failures, all static DAG shapes, parallel/permanent-failure branches, timeout and public cancellation; 31.031s. |
| `./scripts/verify-recovery.ps1` | **PASS** — all four real process crash/cancellation tests; 73.621s. Only the parent subprocess-helper entry intentionally skips. |
| `./scripts/demo-normal.ps1` | **PASS** — authoritative success and both persisted output streams. |
| `./scripts/demo-recovery.ps1` | **PASS** — offline before transfer, old attempt LOST, worker-c fence 2 succeeds, stale replay rejected. |
| `./scripts/test-leaseguard.ps1` | **PASS** — local Docker stop without Control Plane renewal ACKs; bounded restart retry succeeds. |
| `./scripts/demo-pipeline.ps1` | **PASS** — parallel success and permanent failure with unexecuted package SKIPPED while lint succeeds. |
| `./scripts/demo-pipeline-recovery.ps1` | **PASS** — package remains BLOCKED through loss/retry; valid worker-c success releases it; stale replay rejected. |
| `./scripts/verify-observability.ps1` | **PASS** — all backends unavailable at startup; Collector, Jaeger and Prometheus separately stopped during real execution; each job succeeds with exactly one recorded attempt. |
| `./scripts/demo-observability.ps1` | **PASS** — original required normal/recovery spans, distinct retry, worker-c, current expiry/retry counter increments and four healthy scrape targets. |
| `./scripts/verify-frontend.ps1` | **PASS** — strict TypeScript, production build, 24 unit/component tests, 3 fixture browser tests, real Docker/browser recovery (33.4s), restoration of three workers. |

Initial failures are retained as findings, not replaced by weaker checks: skipped-job/SSE regressions first failed, Windows PowerShell default parsing failed, and the SDK-upgrade export regression failed the real trace demo. The final complete run starts again after their fixes and passes every suite.

Added regression evidence:

- `TestSkippedJobCancellationPreservesTerminalState` — byte snapshots of every authoritative table, transitive skipped descendants, active and terminal pipeline phases, repeated terminal cancellation and slots.
- `TestPublicSkippedJobCancellationIsReadOnlyConflict` — public HTTP 409 and unchanged persisted inspection history.
- `TestExporterRejectsOversizedCollectorResponseWithoutRetry` — real HTTP server, success/error responses, bounded body rejection and exactly one request.
- `TestExporterUsesOTLPSignalPathFromEnvironment` — actual Init/export path for generic base URL, trailing slash, prefixed base and signal override.
- Malformed SSE component regression — retained output/cursor, visible error, continued valid delivery; original reconnect/eviction/UTF-8 tests remain.

Inspect the final retained local proofs (transient Jaeger data can disappear on restart):

- Normal trace: http://localhost:16686/trace/b004e04e2a01545b3df332ee1fa4dfbb
- Recovery trace: http://localhost:16686/trace/0a2d9c96acc264dc276aa1aae69a3b25
- Real browser recovery pipeline: http://localhost:5173/pipelines/566f95dd-c622-42b2-8553-e63079fbaefb
- Preserved LOST attempt: http://localhost:5173/attempts/a67d88e4-4275-4360-9615-d4dd56d7bd2f

Separate dependency/hygiene checks on 2026-10-05: npm lockfile audit reports zero known vulnerabilities; source `govulncheck ./...` reports no vulnerabilities on the project's Go 1.25.14 toolchain, and `go mod verify` reports all modules verified. Tracked-artifact/history-pattern checks and final `git diff --check` pass; none replaces a full security assessment.

## Release decision and prohibited claims

The six blocking/material verification issues (F1/F2/F3/F4/F8/F9) are corrected and their regression matrix passes. No confirmed release blocker remains for the documented trusted-local, single-Control-Plane portfolio scope. F5's misleading scope wording is clarified; F6/F7 remain explicit limitations. No frontend redesign, product feature, ownership architecture or infrastructure change was made. M6 changes remain uncommitted/unpushed; the earlier M5 checkpoint is the only Git checkpoint authorized/completed in this task. This audit does not create a tag or begin Milestone 7.

Do not claim exactly-once physical execution/side effects, hostile multi-tenant sandbox security, HA/multiple active Control Planes, exhaustive network-fault coverage, guaranteed remote container termination, lossless durable log/trace delivery, unbounded-scale performance, submission idempotency, implemented DRAINING, or a production-ready public deployment.
