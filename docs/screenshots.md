# Reproducible portfolio screenshots

Captures come from real local PostgreSQL/Compose execution. The helper reads existing APIs and drives the production console; it does not write job state, seed fixtures, kill workers, replace responses or edit pixels. IDs, totals, times and historical sessions vary per run. [Capture manifest](assets/console/capture.md) records actual source IDs/time; it is provenance, not a portable database snapshot.

## Reproduce

Run from the repository root with Docker/Compose, Node/npm and no concurrent submissions:

```powershell
./scripts/demo-pipeline.ps1
./scripts/verify-frontend.ps1
# Use the UUID printed in BROWSER_RECOVERY_PIPELINE by the real browser test:
$recoveryPipelineId = 'PASTE-THE-ACTUAL-UUID-HERE'
node ./scripts/capture-portfolio.mjs --pipeline-id $recoveryPipelineId
```

The frontend script installs locked tooling/Chromium, verifies the production console, runs real worker failure and restores workers. The capture helper requires a succeeded pipeline with LOST attempt #1 on worker-b/fence 1, SUCCEEDED attempt #2 on worker-c/fence 2, current pointer to #2 and a succeeded dependent. It checks retained output and actual sessions. Invalid states fail instead of generating fabricated images.

It selects a succeeded four-job build/unit-test/lint/package DAG from the latest 50 pipeline summaries produced by the demo. Use `--dag-pipeline-id <uuid>` to choose a specific retained DAG. Default `--base-url` is `http://127.0.0.1:5173`; only loopback HTTP is allowed. Only six named PNGs and `capture.md` under `docs/assets/console/` are overwritten. The browser closes; no video, trace or raw API dump is stored.

| Asset | Actual state |
|---|---|
| [overview.png](assets/console/overview.png) | Health, slots, persisted job counts and recovery/retry activity |
| [pipeline-dag.png](assets/console/pipeline-dag.png) | Succeeded static fan-out/fan-in |
| [workers.png](assets/console/workers.png) | Current worker/session identity, connections, capacity and heartbeat |
| [attempt-logs.png](assets/console/attempt-logs.png) | Successful replacement, fence/session/deadlines and persisted SSE output |
| [recovery.png](assets/console/recovery.png) | Two actual attempts and persisted recovery chronology |
| [attempt-lost.png](assets/console/attempt-lost.png) | Historical LOST/fence-1 attempt and retained output |

Captures are historical evidence, not live health claims. Display timezone is UTC. Exact offline transition timestamps are not fabricated. Current workers may have returned online; the LOST attempt retains its old session. Terminal cancellation controls remain disabled.

Jaeger is transient: run `./scripts/demo-observability.ps1` for fresh traces rather than relying on old screenshot links. No GIF or simulated animation substitutes for the immutable attempt history and real browser test.
