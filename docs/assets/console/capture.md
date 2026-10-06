# Actual console capture provenance

Captured 2026-10-06T11:23:24.372Z from http://127.0.0.1:5173, Chromium 153.0.8010.12, Node v24.11.1. Viewport 1600 × 1050, timezone UTC. Production console/API; no fixtures, response interception, database mutations or image editing. Counts/history reflect the retained local database, not benchmarks.

- Recovery pipeline: `18deb1a2-848e-4779-83ad-d86ce5e0b3ee` (SUCCEEDED).
- Recovery job: `e80773db-36d7-41b5-ba8d-1ac1a055d2b3`; current pointer `70303c34-c041-4b88-91c0-7dcf7514750b`.
- Old attempt: `61f2c0af-b650-42e4-b623-e05942a9036b`, LOST / LEASE_EXPIRED, worker-b session `b18af170-2c3a-4f1b-8bc3-1f8ccfe4d4ee`, attempt 1 / fence 1.
- Winner: `70303c34-c041-4b88-91c0-7dcf7514750b`, SUCCEEDED, worker-c session `99faddc1-c7a8-4e4e-a93a-1db73b9cd361`, attempt 2 / fence 2.
- Fan-out/fan-in pipeline: `b8adf3aa-cdfc-48d7-9e5a-8b121319b449` (all four jobs SUCCEEDED).
- Assets: [overview.png](overview.png), [pipeline-dag.png](pipeline-dag.png), [workers.png](workers.png), [attempt-logs.png](attempt-logs.png), [attempt-lost.png](attempt-lost.png), [recovery.png](recovery.png).

Reproduce from the root: `node ./scripts/capture-portfolio.mjs --pipeline-id 18deb1a2-848e-4779-83ad-d86ce5e0b3ee --dag-pipeline-id b8adf3aa-cdfc-48d7-9e5a-8b121319b449`. These IDs require this retained database; a fresh run must use new IDs from the real demos/test. See [workflow](../../screenshots.md).
