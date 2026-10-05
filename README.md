# ForgeGrid

ForgeGrid is a Go distributed job execution engine. Milestones 1A and 1B implement the execution backbone described in [architecture-v0.1](docs/architecture-v0.1.md): PostgreSQL owns coordination state, outbound gRPC connects workers, and Docker executes trusted local jobs.

Execution is **at least once physically**, with a single authoritative terminal winner. Killing an agent can leave its Docker job container alive. Lease expiry and fencing prevent that old attempt from finalizing the logical job.

## Run locally

Requirements: Docker Desktop with its Linux engine running, Docker Compose, and PowerShell 7. Host Go and protoc are optional; development tools run in Docker. Ports 5432, 8080, and 9090 must be available. Initial builds require internet access.

From the repository root:

```powershell
./scripts/demo-normal.ps1
./scripts/demo-recovery.ps1
```

The normal demo builds the stack, registers worker-a/b/c, submits an argv job, and asserts PostgreSQL-backed success and both log streams. The recovery demo temporarily stops a/c, starts a fresh worker-b session, kills worker-b with SIGKILL, starts c, observes the offline session while attempt #1 still owns the job, waits for lease expiry, verifies LOST -> attempt #2 on c -> SUCCEEDED, and replays the old result through gRPC. It must print `STALE_ATTEMPT_REJECTED` and `WORKER_B_TO_WORKER_C_RECOVERY_PASSED`. Both demos use the existing local database; run them when no other jobs are being submitted. The recovery demo restores all three workers afterward.

Start the stack without submitting a demo:

```powershell
docker compose up -d --build --wait postgres control-plane worker-a worker-b worker-c
```

Submit a job:

```powershell
$body = @{ image = 'alpine:3.22'; command = @('echo', 'hello ForgeGrid'); timeout_seconds = 30; max_attempts = 2 } | ConvertTo-Json
$job = Invoke-RestMethod http://localhost:8080/api/v1/jobs -Method Post -ContentType application/json -Body $body
Invoke-RestMethod "http://localhost:8080/api/v1/jobs/$($job.id)"
Invoke-RestMethod http://localhost:8080/api/v1/workers
```

Read attempt logs with `GET /api/v1/attempts/{attempt_id}/logs?after=0`. Chunks are ordered by sequence; JSON payloads are base64-encoded bytes. Responses contain at most 1,000 chunks; paginate using the last sequence. The executor directly uses argv with the image entrypoint overridden by argv[0]. A shell is used only when explicitly requested in argv, as in the demo's stdout/stderr command. Jobs have no network, host mounts, Docker socket, privileged mode, or host namespaces.

Stop services while retaining PostgreSQL data:

```powershell
docker compose down
```

## Verify

```powershell
./scripts/verify.ps1
./scripts/verify-e2e.ps1
./scripts/demo-recovery.ps1
./scripts/test-leaseguard.ps1
```

`verify.ps1` checks gofmt, go vet, compilation, unit tests, and race-enabled integration tests against real PostgreSQL. Integration tests use a unique schema per test and remove only that schema. Explicitly requested integration tests fail if `TEST_DATABASE_URL` is missing. `verify-e2e.ps1` tests real Docker execution, stdout/stderr persistence, duplicate result/log delivery, stale fencing, nonzero exit, invalid command, and timeout. The lease-guard script stops the Control Plane, proves the old job container stops without renewal ACKs, then checks recovery after restart.

With a local Go toolchain:

```powershell
go test ./...
$env:TEST_DATABASE_URL = 'postgres://forgegrid:forgegrid@localhost:5432/forgegrid?sslmode=disable'
go test -race -tags integration ./...
# Requires the running Compose stack:
go test -race -tags e2e ./tests/e2e
```

Regenerate the committed Protobuf Go bindings:

```powershell
./scripts/generate-proto.ps1
```

## Implementation

- `cmd/controlplane`, `internal/controlplane`: HTTP/gRPC gateway, transactional scheduling, heartbeat detection, and lease recovery in one process.
- `cmd/worker`, `internal/worker`: process session UUID, heartbeats, renewal requests, monotonic lease guard, Docker CLI executor, log collector, and startup reconciliation.
- `internal/domain`: ownership checks, specification validation, result classification, and capped infrastructure retry policy.
- `internal/store/postgres`, `db/migrations`: explicit pgx SQL, row locks, durable attempts, reserved capacity, and duplicate-safe log chunks. The embedded initial migration runs transactionally under a PostgreSQL advisory lock; it is idempotent and introduces only the five slice tables.
- `api/proto/forgegrid/v1`, `gen/go`: versioned contracts and generated bindings.
- `tests`, `scripts`: Docker end-to-end checks, diagnostic result replay, and repeatable demos.

Development defaults: heartbeat 2s, offline threshold 6s, execution lease 10s, renewal interval 2s, retry base 1s. Set `HEARTBEAT_INTERVAL`, `OFFLINE_THRESHOLD`, `EXECUTION_LEASE`, `LEASE_RENEW_INTERVAL`, and `RETRY_BASE` before Compose startup to override them. Each worker session has capacity one. `max_attempts` defaults to two and is bounded to 1..10. Infrastructure retry delay doubles and caps at 30s. Timeout defaults to 30s and is bounded to 1..86,400 seconds.

See [execution semantics](docs/execution-semantics.md), [failure model](docs/failure-model.md), [protocol](docs/protocol.md), and [implementation decisions](docs/implementation-v0.1.md).

## Limits of this slice

One Control Plane, no HA; no frontend or pipelines/DAGs. No authentication or TLS: HTTP/gRPC are for trusted local development, with host bindings restricted to loopback. Workers require privileged access to the Docker daemon; this is not a hostile multi-tenant sandbox. Workload containers themselves are non-privileged.

A process crash can leave a physical container running. Startup reconciliation removes older-session containers for the same worker identity on the accessible daemon; inaccessible machines cannot be remotely cleaned up. Local cancellation attempts Docker removal with a bounded timeout and logs failures, so physical shutdown cannot be guaranteed when the daemon is unavailable. A permanently lost worker's container may require manual removal using its ForgeGrid labels.

Log delivery has a bounded in-memory queue and bounded retries with persistence ACKs. Abrupt worker crashes or cancellation can lose logs that were not acknowledged; there is no durable worker spool or browser streaming. Each chunk is bounded, but total log storage has no retention policy yet. Logs are diagnostic and may be appended for old attempts with valid immutable identity. Structured process logs are implemented; Prometheus/OpenTelemetry instrumentation is deferred. Submission has no idempotency key in this single-job slice. Public cancellation, authentication, secrets, integrations, and the later architecture gates are not implemented.
