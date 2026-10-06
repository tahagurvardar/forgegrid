# v1.0.0 release preparation audit

Milestone 7 prepares presentation of the verified engine; it adds no product feature, execution authority, service or infrastructure. M6 changes remain in the working tree and are preserved. No commit, push, tag or GitHub Release is created.

SHA-256 comparisons against the start of M7 show all 63 existing engine/frontend source, test and script files checked are unchanged, including the pending M6 fixes. Package version/license and ignore/documentation changes are presentation metadata; dependency versions are unchanged in M7.

## Scope and documentation consistency

README now leads with ownership/recovery, the signature stale-result demonstration and current pipeline/console/telemetry capabilities. Its Mermaid diagram distinguishes the single Control Plane's scheduler/recovery functions from external processes and shows telemetry as observation. Prominent limitations qualify at-least-once execution, cancellation and retention. Four factual CV bullets and eleven interview answers are in [portfolio.md](portfolio.md). [Prepared release notes](releases/v1.0.0.md) contain only implemented scope.

The original architecture remains a design/history source. Its conceptual schema/examples are not a migration specification: use committed SQL/Protobuf and execution/protocol documents for runtime details. Historical five-table, internal cancellation, deferred frontend/observability statements and old test counts are retained with explicit stage notes. Current scope is seven authoritative tables, public job/pipeline cancellation, observability and the console; DRAINING and submission idempotency remain deferred.

## VERIFIED CLAIMS

- PostgreSQL transactions/constraints and lock order coordinate current attempt, fence, capacity, completion and DAG transitions; dispatch follows commit.
- Worker IDs are stable and process session IDs distinct. Heartbeat liveness does not transfer an unexpired lease. Successful renewal ACKs gate local monotonic execution authority.
- Lease checks use database time after ownership locks. Stale/expired attempts cannot commit authoritative success; exact current-winner duplicates release no extra slot.
- Infrastructure retries are bounded and create fresh attempts/higher fences. Timeout/nonzero workload failure do not automatically retry. Cancellation is durable but physical cleanup is best effort.
- Static DAG validation precedes runnable persistence; fan-in release/skips aggregate transactionally; unrelated branches continue and retryable parents keep descendants blocked.
- The console preserves LOST attempts, reads authoritative snapshots, shows degradation honestly and resumes bounded SSE logs by sequence.
- Telemetry observes boundaries with async trace context and bounded queues/cardinality. Backend outages do not become authority failures.
- Real PostgreSQL barrier/rollback tests, real Docker/process crashes and real browser worker-b → worker-c recovery supply specific evidence. Screenshots read those states, not fixture APIs.

## CLAIMS INTENTIONALLY NOT MADE

Exactly-once physical execution/side effects; production readiness; hostile secure sandbox; highly scalable/fault-proof/zero-data-loss operation; HA; guaranteed shutdown; lossless logs/traces; guaranteed eventual success; implemented submission idempotency or DRAINING; exhaustive security/network/failover coverage.

Case-insensitive repository searches cover these phrases and TODO/FIXME. Remaining occurrences are explicit nonclaims, historical design notes or engineering questions, not unsupported capability advertising. No useful test or fault evidence is removed.

## Repository hygiene and licensing

The owner selected [MIT](../LICENSE); [licensing.md](licensing.md) identifies dependency notice sources without relicensing third-party packages/images. Console package metadata is 1.0.0/MIT; locked dependency versions are unchanged. Generated Protobuf Go source remains committed intentionally.

Ignore rules cover environment variants, logs/temp/sqlite files, executables, dependencies, builds and browser reports. Docker contexts exclude runtime output and portfolio PNGs; no image needs those assets for execution. PNGs are marked binary in Git attributes; the verification shell keeps LF endings for Windows/Docker compatibility. Two pre-existing untracked terminal-output files (`ersPCProjectsforgegrid`, `tatus --short`) contain only Git/less output and are removed as accidental artifacts, not source/history. No DB volume is deleted.

Tracked/current candidate files and reachable history are checked for targeted private-key/token patterns without printing potential credentials. This is not a proof covering every secret format. Local Compose credentials are documented development defaults. No telemetry DB, logs, .env, Docker data, browser trace/video or compiled bundle belongs to release source. Only six intentional console PNGs and their provenance manifest are included as assets.

## Reproduce the complete verification

Run exclusively/sequentially from the root. Clean Compose state means containers/network recreated, **not** deletion of PostgreSQL data/volumes:

```powershell
docker compose --profile console --profile tools down
./scripts/verify.ps1
./scripts/verify-e2e.ps1
./scripts/verify-recovery.ps1
./scripts/test-leaseguard.ps1
./scripts/demo-normal.ps1
./scripts/demo-recovery.ps1
./scripts/demo-pipeline.ps1
./scripts/demo-pipeline-recovery.ps1
./scripts/verify-observability.ps1
./scripts/demo-observability.ps1
./scripts/verify-frontend.ps1
# Scanner tool installation may require a newer toolchain; the project remains Go 1.25.
docker compose run --rm -e GOTOOLCHAIN=auto verify sh -c 'go install golang.org/x/vuln/cmd/govulncheck@v1.8.0 && go version && govulncheck ./... && go mod verify'
Push-Location frontend
npm.cmd audit
Pop-Location
git diff --check
```

Each verification script checks its child exits; do not combine commands into a claim of success without checking all results. Scanners report known advisories at the run date; source/npm scans do not replace container OS scans or a penetration test. The real recovery suite intentionally skips the parent helper-only entry; all four actual process scenarios must run.

## Final verification matrix

**2026-10-06: PASS. ForgeGrid is eligible for the v1.0.0 public portfolio release within its documented trusted-local, single-Control-Plane scope.** All eleven unchanged verification/demo scripts exited zero after containers/network were recreated; data/volumes were retained. Windows PowerShell 5.1 ran each script sequentially. No assertion was removed, weakened or bypassed.

| Check | Result / evidence |
|---|---|
| `verify.ps1` | **PASS**, 95.9s including setup: format/vet/build and race-enabled real PostgreSQL suites (store 33.227s, observability 5.229s). |
| `verify-e2e.ps1` | **PASS**, 64.4s including setup; real Docker package 28.250s, protocol duplicates/failure and DAG/cancellation/timeout tests. |
| `verify-recovery.ps1` | **PASS**, 99.2s including setup; all four actual crash/cancellation tests 73.748s. Only the helper-only parent entry intentionally skips. |
| `test-leaseguard.ps1` | **PASS**, 40.5s; missing ACK stop and Control Plane restart recovery. |
| `demo-normal.ps1` | **PASS**, 32.1s; success and both output streams. |
| `demo-recovery.ps1` | **PASS**, 65.4s; offline-before-transfer, LOST, higher fence, worker-c success and stale replay rejection. |
| `demo-pipeline.ps1` | **PASS**, 19.4s; fan-out/fan-in success, permanent failed branch skips package while unrelated lint succeeds. |
| `demo-pipeline-recovery.ps1` | **PASS**, 47.1s; dependent BLOCKED through retry and released after valid success. |
| `verify-observability.ps1` | **PASS**, 70.5s; missing backends at startup and separate Collector/Jaeger/Prometheus loss mid-job. |
| `demo-observability.ps1` | **PASS**, 64.3s; actual required spans, worker-c retry, current metric increments and healthy scrape targets. |
| `verify-frontend.ps1` | **PASS**, 102.5s; strict TypeScript, production build, 24 unit/component tests, 3 fixture browser tests (5.7s), 1 real Docker/browser recovery (33.7s). |
| `govulncheck ./...` | **PASS**, no vulnerabilities found on project Go 1.25.14; v1.8.0 scanner installed with its separate automatic toolchain. |
| `npm audit --json` | **PASS**, zero reported vulnerabilities. |
| `go mod verify` | **PASS**, all modules verified. |
| `git diff --check` | **PASS**, including final release document updates. |

Additional checks: the screenshot helper passes against the real browser pipeline; invalid UUID/nonlocal origin guards reject input; all six captures were visually reviewed. `agent-browser` verifies the real recovery page/navigation, LOST/winner history and absence of collected page/console errors. Markdown local links/fences, PowerShell parsing, candidate artifact/targeted secret and reachable-history scans pass. The frontend lockfile is structurally identical after excluding only the intended root version/license fields. The Git index and HEAD remain unchanged.

Two ad hoc check expressions initially produced false alarms: serialized JSON differed only in property insertion order, and directly piping Invoke-RestMethod treated its array as one object. Structural comparison and explicit array enumeration verified the same unmodified dependencies/worker state. These were check-expression corrections, not failures of an engine suite or weakened assertions. Final inspection shows zero active jobs and all three current workers ONLINE, connected, with zero occupied slots.

## Portfolio assets

[Workflow](screenshots.md) uses existing pipeline demo and real browser test. The read-only capture helper validates persisted LOST/worker-b/fence-1 and SUCCEEDED/worker-c/fence-2 identities, pointer, dependent success, sessions and retained stdout before saving six PNGs. [Manifest](assets/console/capture.md) records provenance. It does not create fictional events or modify images/state.

Captured from this run's browser recovery pipeline `18deb1a2-848e-4779-83ad-d86ce5e0b3ee` and actual fan-out/fan-in pipeline `b8adf3aa-cdfc-48d7-9e5a-8b121319b449`. Old LOST attempt `61f2c0af-b650-42e4-b623-e05942a9036b` remains visible alongside winner `70303c34-c041-4b88-91c0-7dcf7514750b`. Six PNGs total 612,872 bytes; they are unedited browser captures, not benchmark/scale evidence.

## Known limitations

Single Control Plane; trusted/local scope; no hostile multi-tenant sandbox/auth/TLS/public deployment readiness. At-least-once physical executions can overlap and arbitrary external effects are not fenced. An inaccessible daemon may retain containers. Pending logs/traces can be lost; no durable spool/total log retention, transient Jaeger. Static bounds and serialized pipeline gates are correctness choices, not scale guarantees. Exact offline-transition times are not persisted; snapshot/offset history windows are incomplete. Exhaustive asymmetric network, PostgreSQL failover and image/OS fault/security coverage remain outside scope. No feature work is added to close these limits in M7.

## Exact M7 file changes

| Files | Change |
|---|---|
| `README.md` | Release introduction, ownership/recovery model, Mermaid, console/pipeline scope, screenshots, demos and limits |
| `LICENSE`, `docs/licensing.md` | Owner-selected MIT and dependency notice index |
| `.gitignore`, `.dockerignore`, `frontend/.dockerignore`, `.gitattributes` | Runtime/environment/build-context exclusions and binary PNG handling; shell LF retained |
| `frontend/package.json`, `frontend/package-lock.json` | Version 1.0.0/MIT metadata only; no dependency changes |
| `docs/architecture-v0.1.md`, `docs/implementation-v0.1.md`, `docs/correctness-audit.md`, `docs/pipeline-semantics-audit.md`, `docs/observability-audit.md`, `docs/operational-console-audit.md`, `docs/final-engineering-audit.md` | Historical/current scope cross-references; original audit evidence preserved |
| `docs/execution-semantics.md`, `docs/failure-model.md` | Current-scope titles and release evidence links |
| `docs/portfolio.md`, `docs/releases/v1.0.0.md`, `docs/release-audit.md` | CV/interview notes, prepared notes, claims/hygiene/verification record |
| `docs/screenshots.md`, `scripts/capture-portfolio.mjs` | Reproducible read-only real-state capture |
| `docs/assets/console/overview.png`, `docs/assets/console/pipeline-dag.png`, `docs/assets/console/workers.png`, `docs/assets/console/attempt-logs.png`, `docs/assets/console/recovery.png`, `docs/assets/console/attempt-lost.png`, `docs/assets/console/capture.md` | Six reviewed captures and actual provenance |

The working tree also contains pre-existing M6 modifications to `frontend/src/components.tsx`, `frontend/src/sse.test.tsx`, `go.mod`, `go.sum`, `internal/controlplane/pipeline_http_integration_test.go`, `internal/observability/tracing.go`, `internal/observability/tracing_test.go`, `internal/store/postgres/pipeline_integration_test.go`, `internal/store/postgres/store.go`, `scripts/demo-observability.ps1`, `scripts/demo-pipeline-recovery.ps1`; these are preserved unchanged by M7, not new release-polish fixes. The two accidental untracked terminal files listed above are removed; no tracked source file is deleted.
