# Milestone 5: operational console

This preserves Milestone 5 scope and its original verification counts. M6 adds the malformed-SSE regression (24 unit/component tests) and terminal-SKIPPED cancellation protection. See [final-engineering-audit.md](final-engineering-audit.md) for those findings and [release-audit.md](release-audit.md) for the current complete run.

The React / TypeScript / Vite console observes the existing engine. PostgreSQL and Control Plane APIs remain authoritative. Browser refreshes, navigation, missing telemetry, SSE disconnection and closing the console cannot renew a lease or decide an execution result. No ownership, retry, cancellation, timeout, scheduling, recovery or DAG transition was changed for this milestone.

## Audit and minimum API additions

The existing API already supported standalone job and static pipeline submission, individual job/pipeline inspection with attempt histories, worker sessions, ordered log retrieval, cancellation, health and Prometheus metrics. Persisted lifecycle timestamps and diagnostic trace context existed in PostgreSQL. Browser SSE was specified in the architecture but not yet implemented.

Missing frontend prerequisites were bounded lists, direct attempt inspection, exposed timestamps and a browser log stream. These are read-only additions, not a BFF or another coordination service:

| Route | Result |
| --- | --- |
| GET /api/v1/pipelines?offset=0 | 50 pipeline summaries and nullable next_offset; state, job/terminal/attempt counts, submitted/started/finished timestamps |
| GET /api/v1/jobs?offset=0 | 50 jobs with immutable attempt histories and nullable next_offset |
| GET /api/v1/overview | Server snapshot time, job-state counts, latest 50 LOST/retry attempts and active attempts |
| GET /api/v1/attempts/{id} | The containing job and its complete attempt history, allowing inspection of historical attempts |
| GET /api/v1/attempts/{id}/logs/stream?after=0 | Ordered SSE log chunks, resumable by sequence |

Existing job/pipeline responses now expose stored lifecycle timestamps. Pipeline start is the minimum persisted attempt started_at, not a fabricated pipeline event. Attempts expose a Jaeger trace_id only when persisted W3C traceparent validates. Worker responses retain the array contract, adding started_at, disconnected_at and current (latest session by stored start time and ID).

List ordering is submitted timestamp descending, then ID descending. Offsets are validated in 0..100000; pages contain at most 50 entries. Concurrent submissions can shift offset pages; this is an inspection window, not an export snapshot. Jobs/overview use read-only REPEATABLE READ transactions; pipeline summaries use a single SQL statement snapshot. Direct attempt inspection resolves immutable membership then reads its job snapshot. These paths acquire no ownership row locks and write no coordination state. No migration or gRPC change was required. Existing pipeline gate and job → attempt → worker-session lock order remain intact.

## Views and state rendering

| Browser route | Purpose |
| --- | --- |
| / | Control Plane/PostgreSQL health, current worker capacity, job states, persisted recovery/retry activity |
| /pipelines | Paginated pipeline status, execution summary and lifecycle timestamps |
| /pipelines/new | Submit the existing static DAG JSON contract; validation remains on the backend |
| /pipelines/{id} | Dependency arrows, selectable job state, configuration, attempts and recovery chronology |
| /jobs | Paginated standalone and pipeline jobs |
| /jobs/{id} | Job inspection and all attempts |
| /attempts/{id} | Identity, session, fencing token, stored lease/execution deadlines, timestamps, exit/failure, trace link and live output |
| /workers | Current/historical sessions, connected state, occupied capacity, heartbeat, active attempt links and bounded recent execution history |

Job badges distinguish BLOCKED, QUEUED, DISPATCHED, RUNNING, RETRY_WAIT, SUCCEEDED, FAILED, SKIPPED and CANCELLED, and also the backend's intermediate CANCELLING. Attempt ASSIGNED/LOST/TIMED_OUT states remain visible. Old attempts are never replaced with the final job state. A current_attempt_id may still reference a terminal attempt; the UI calls it the current pointer, not proof of active authority.

Polling is sequential per resource, normally every two seconds after the previous request, with an eight-second request timeout and abort/cleanup on navigation. Independent resources load concurrently. A failed refresh retains the last successful snapshot with an explicit stale/error notice and last received time. Health failure is shown as unavailable; worker capacity is not inferred from heartbeat age in the browser. Available slots include only ONLINE, connected current sessions; historical reservations are reported separately. DRAINING is not implemented by the backend and is explicitly unsupported in the console.

Job and pipeline cancellation use the existing POST endpoints after inline confirmation. The browser reports that a request was sent and waits for authoritative refresh; it does not optimistically declare execution stopped. There is no worker drain control, scheduler setting or frontend execution state machine.

## Recovery chronology

The timeline uses persisted assignment, start, finish, failure kind, lease expiry, attempt number and fence. An actual LEASE_EXPIRED loss can show the stored lease boundary and later LOST finish, followed by a newly assigned bounded retry and its outcome. A currently OFFLINE session can show its last persisted heartbeat, explicitly stating that the exact offline transition timestamp is not persisted. It does not invent a heartbeat-loss event or a worker crash timestamp. Session information and job inspection are separate snapshots and may temporarily differ.

No browser clock is an authority check. Displayed deadlines are stored server deadlines, timestamps use browser local time with exact UTC tooltips, and success/loss comes from persisted state. LOST does not prove a Docker container stopped; a crashed agent can leave a container alive. Fencing protects authoritative results, not workload side effects. Physical execution remains at-least-once.

## SSE and bounded logs

The stream reuses the existing PostgreSQL log store: event type chunk, event ID equal to sequence, and JSON payload with base64 bytes and STDOUT/STDERR. The initial cursor is after; reconnect uses the maximum of after and native EventSource Last-Event-ID. Ordered pages contain at most 1,000 chunks; repeated/replayed sequences are harmless in the browser. Invalid/negative cursors are rejected before streaming. Streams rotate after 30 seconds, advertise a one-second retry, emit keepalive comments, and extend only their own bounded write deadline. They bypass the ordinary ten-second HTTP TimeoutHandler; normal API deadlines remain unchanged. A slow reader cannot block indefinitely. The lightweight SSE adapter reads persisted logs every 500ms; browser logs do not use polling.

The browser retains at most 200 chunks and 131,072 UTF-16 characters plus a maximum three-byte incomplete UTF-8 suffix per stdout/stderr stream. Eviction never decreases the reconnect cursor; removed chunk counts are shown. Valid split UTF-8 characters are reconstructed across chunks without mutable decoder state inside React updates. Invalid bytes use replacement decoding; a final incomplete suffix may remain undisplayed. Raw bytes remain retrievable through the existing JSON log API. ANSI escapes are not interpreted and output is rendered as text, never HTML.

Selection closes the previous EventSource. Connecting, connected, reconnecting, disconnected and invalid-data states are explicit. Terminal/stale attempts can still receive diagnostic log chunks under existing backend rules, so terminal state does not automatically close the viewer or imply complete physical output. Backend queue/crash loss and lack of durable spool/total storage retention remain unchanged.

## Local operation

```powershell
docker compose --profile console up -d --build --wait postgres control-plane worker-a worker-b worker-c console
# Open http://localhost:5173
# Optional independent trace/metrics services:
docker compose up -d otel-collector prometheus jaeger
```

The optional console Compose profile serves the production build through Nginx on loopback port 5173. Same-origin HTTP/SSE proxying targets the existing Control Plane; Docker DNS is refreshed so Control Plane recreation does not require frontend recreation. This proxy only serves files and forwards HTTP; it has no business logic. Jaeger and Prometheus links target their existing local ports. Neither is required for the console or execution.

For frontend development, stop the console service to free port 5173, start the backend normally, then:

```powershell
cd frontend
npm ci
npm run dev
```

Vite proxies /api and /healthz to localhost:8080 (override with FORGEGRID_API_URL). Node 24 was used for verification. No authentication/TLS or hostile workload sandbox is added; this remains trusted local development.

## Test evidence

- Strict TypeScript checking and production Vite build.
- Component/domain tests for every operational state, immutable LOST history, deterministic DAG layout, honest stale rendering, duplicate/out-of-order log handling, bounded eviction, split UTF-8 reconstruction, reconnect status and EventSource cleanup.
- Deterministic browser fixtures for DAG → old attempt → failure/fence/log inspection, backend outage while preserving stale data, and mobile navigation/worker history without document overflow. Fixtures are only imported by tests, never by the production console.
- Real PostgreSQL inspection/SSE tests verify timestamps, Last-Event-ID precedence, rejection of invalid cursors/missing IDs, late diagnostic logs after completion, 51-entry pagination boundaries, and byte-for-byte unchanged authoritative snapshots after reads.
- Real Docker browser recovery submits a pipeline through the UI, inspects RUNNING stdout on worker-b, kills worker-b, observes its session OFFLINE and attempt LOST, retains old history, then requires worker-c success with fence 2 and downstream pipeline success. All three workers are restored afterward.

```powershell
./scripts/verify-frontend.ps1
```

This installs/builds the production console, runs strict/unit/browser checks and the real recovery test. It prints actual pipeline and LOST attempt URLs for inspection. It requires Docker, Node/npm and Chromium (installed by Playwright), exclusively uses local port 5173, and must run without concurrent submissions or recovery demos. Browser traces/reports, dependencies and dist are ignored runtime artifacts. The entire existing backend verification matrix is also run unchanged; final results are recorded below after completion.

## Known limits and intentionally deferred scope

Read snapshots are eventually refreshed; there is no event archive, exact heartbeat/offline transition history, complete worker execution archive, export pagination guarantee or storage-retention system. Worker execution history is explicitly limited to the latest 50 submitted jobs. State snapshots are bounded by the existing implementation but historical sessions and active-attempt inspection may grow; no scale claim is made. The DAG is static and subject to existing 128-job/2,048-edge limits.

Authentication, teams, billing, GitHub integration, matrix/dynamic DAGs, expressions, artifacts, secrets, speculative settings, theme switching, brokers, Kubernetes and autoscaling are intentionally deferred. The console does not introduce frontend-owned orchestration or exactly-once claims.

## Final verification — 2026-10-05

| Suite | Result |
| --- | --- |
| ./scripts/verify.ps1 | PASS: gofmt, vet, build, race-enabled units and real PostgreSQL integration, including inspection/SSE/pagination tests |
| ./scripts/verify-e2e.ps1 | PASS: real Docker execution, logs/protocol idempotency, workload failures, DAG shapes, cancellation and timeout |
| ./scripts/verify-recovery.ps1 | PASS: assignment/start crash windows, surviving physical containers, recovery and internal cancellation |
| ./scripts/demo-normal.ps1 | PASS |
| ./scripts/demo-recovery.ps1 | PASS: worker-b → worker-c and stale-result rejection |
| ./scripts/test-leaseguard.ps1 | PASS: stop without renewal ACKs and Control Plane restart recovery |
| ./scripts/demo-pipeline.ps1 | PASS: parallel success and permanent branch failure/SKIPPED package |
| ./scripts/demo-pipeline-recovery.ps1 | PASS: fenced recovery releases dependent job |
| ./scripts/verify-observability.ps1 | PASS: backends absent at startup and Collector/Jaeger/Prometheus loss mid-job |
| ./scripts/demo-observability.ps1 | PASS: normal/recovery distributed traces and persisted metric increments |
| ./scripts/verify-frontend.ps1 via Windows PowerShell 5.1 | PASS: strict check, production build, 23 unit/component tests, 3 fixture browser tests and 1 real Docker browser recovery test |
| npm audit | PASS: zero reported vulnerabilities with the locked dependencies |

The real browser recovery checks the final dependency graph as well as old/new attempt views: package SUCCEEDED, attempt #1 still LOST, attempt #2 SUCCEEDED on worker-c with fence 2. The script prints the actual URLs and restores workers. Manual browser inspection covered real overview, DAG and LOST attempt pages; JavaScript error collection was empty. Production bundle inspection found no deterministic fixture markers. The same console proxy remained healthy after Control Plane recreation during observability verification.

React review confirmed primitive effect dependencies, independent snapshot requests, stable job/attempt keys, abort/stream cleanup, pure immutable log updates and no page-wide auto-scroll for output. Runtime assets, dependencies and browser reports remain ignored; no files were staged, committed or pushed.
