import { useState } from "react";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import { api } from "./api";
import {
  AttemptTable,
  Badge,
  Cancel,
  Dag,
  Freshness,
  LiveLogs,
  Stamp,
  Timeline,
} from "./components";
import { useSnapshot } from "./useSnapshot";
import { duration, short } from "./presentation";
import { terminalJob } from "./models";
import type {
  Job,
  Overview,
  Page,
  Pipeline,
  PipelineSummary,
  Worker,
} from "./models";
export function OverviewPage() {
  const health = useSnapshot<{ status: string }>("/healthz"),
    overview = useSnapshot<Overview>("/api/v1/overview"),
    workers = useSnapshot<Worker[]>("/api/v1/workers");
  const current = (workers.data ?? []).filter((w) => w.current);
  const occupied = current.reduce((n, w) => n + w.active_slots, 0),
    available = current
      .filter((w) => w.state === "ONLINE" && w.connected)
      .reduce((n, w) => n + w.capacity_slots - w.active_slots, 0);
  return (
    <>
      <header className="page-heading">
        <div>
          <p className="eyebrow">COORDINATION / LIVE SNAPSHOT</p>
          <h1>System overview</h1>
          <p>
            Execution authority, capacity and recovery — as persisted by the
            Control Plane.
          </p>
        </div>
        <span className={`health ${health.error ? "degraded" : ""}`}>
          {health.error
            ? "Control Plane unavailable"
            : health.data
              ? "Control Plane + PostgreSQL healthy"
              : "Checking Control Plane…"}
        </span>
      </header>
      <Freshness {...overview} />
      {workers.error ? <Freshness {...workers} /> : null}
      <div className="system-strip">
        <div>
          <strong>
            {workers.data
              ? current.filter((w) => w.state === "ONLINE").length
              : "—"}
          </strong>
          <span>ONLINE workers</span>
        </div>
        <div>
          <strong>
            {workers.data
              ? current.filter((w) => w.state === "OFFLINE").length
              : "—"}
          </strong>
          <span>OFFLINE workers</span>
        </div>
        <div>
          <strong>
            {workers.data ? available : "—"}{" "}
            <em>/ {workers.data ? occupied : "—"}</em>
          </strong>
          <span>available / occupied current-session slots</span>
        </div>
        <div>
          <strong>—</strong>
          <span>DRAINING unsupported</span>
        </div>
      </div>
      {workers.data ? (
        <p className="muted mono">
          Historical-session reservations:{" "}
          {workers.data
            .filter((w) => !w.current)
            .reduce((n, w) => n + w.active_slots, 0)}
          . Supersession does not transfer an unexpired attempt lease.
        </p>
      ) : null}
      <div className="state-strip">
        {[
          "BLOCKED",
          "QUEUED",
          "DISPATCHED",
          "RUNNING",
          "RETRY_WAIT",
          "CANCELLING",
          "SUCCEEDED",
          "FAILED",
          "CANCELLED",
          "SKIPPED",
        ].map((state) => (
          <div key={state}>
            <Badge state={state} />
            <strong className="mono">
              {overview.data ? (overview.data.jobs[state] ?? 0) : "—"}
            </strong>
          </div>
        ))}
      </div>
      <section>
        <div className="section-heading">
          <h2>Recovery & retry activity</h2>
          <span className="muted">
            Latest 50 persisted LOST / retry attempts
          </span>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Attempt</th>
                <th>Worker</th>
                <th>State</th>
                <th>Failure boundary</th>
                <th>Assigned / finished</th>
              </tr>
            </thead>
            <tbody>
              {overview.data?.recent_activity.map((a) => (
                <tr key={a.attempt_id}>
                  <td>
                    <Link to={`/attempts/${a.attempt_id}`}>
                      #{a.attempt_number}{" "}
                      <span className="mono">{short(a.attempt_id)}</span>
                    </Link>
                    <small>
                      <Link to={`/jobs/${a.job_id}`}>Inspect job</Link>
                    </small>
                  </td>
                  <td className="mono">{a.worker_id}</td>
                  <td>
                    <Badge state={a.state} />
                  </td>
                  <td className="mono">
                    {a.failure_kind || "Retry execution"}
                  </td>
                  <td>
                    <Stamp value={a.finished_at ?? a.assigned_at} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {overview.data && !overview.data.recent_activity.length ? (
            <p className="empty">No persisted recovery or retry activity.</p>
          ) : null}
        </div>
      </section>
      <p className="boundary">
        Heartbeat liveness and execution leases are separate. An offline worker
        can still hold an unexpired reservation.
      </p>
    </>
  );
}
export function PipelinesPage() {
  const [offset, setOffset] = useState(0);
  const data = useSnapshot<Page<PipelineSummary>>(
    `/api/v1/pipelines?offset=${offset}`,
  );
  return (
    <>
      <header className="page-heading">
        <div>
          <p className="eyebrow">STATIC DAG EXECUTION</p>
          <h1>Pipelines</h1>
          <p>
            Bounded retries. Transactional dependency release. Independent
            branches continue.
          </p>
        </div>
        <Link className="button primary" to="/pipelines/new">
          Submit pipeline
        </Link>
      </header>
      <Freshness {...data} />
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Pipeline</th>
              <th>State</th>
              <th>Execution</th>
              <th>Submitted</th>
              <th>Started</th>
              <th>Finished</th>
            </tr>
          </thead>
          <tbody>
            {data.data?.items.map((p) => (
              <tr key={p.id}>
                <td>
                  <Link className="mono" to={`/pipelines/${p.id}`}>
                    {short(p.id)}
                  </Link>
                  <small>{duration(p.started_at, p.finished_at)}</small>
                </td>
                <td>
                  <Badge state={p.state} />
                </td>
                <td className="mono">
                  {p.terminal_count}/{p.job_count} terminal
                  <small>{p.attempt_count} attempts</small>
                </td>
                <td>
                  <Stamp value={p.created_at} />
                </td>
                <td>
                  <Stamp value={p.started_at} />
                </td>
                <td>
                  <Stamp value={p.finished_at} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {data.data && !data.data.items.length ? (
          <p className="empty">
            No pipelines on this page. Submit a static DAG to inspect execution.
          </p>
        ) : null}
      </div>
      <Pager
        offset={offset}
        next={data.data?.next_offset ?? null}
        set={setOffset}
      />
    </>
  );
}
function Pager({
  offset,
  next,
  set,
}: {
  offset: number;
  next: number | null;
  set: (n: number) => void;
}) {
  return (
    <nav className="pager" aria-label="Pagination">
      <button disabled={!offset} onClick={() => set(Math.max(0, offset - 50))}>
        Previous
      </button>
      <span className="mono">
        Rows {offset + 1}–{offset + 50}
      </span>
      <button
        disabled={next === null}
        onClick={() => {
          if (next !== null) set(next);
        }}
      >
        Next
      </button>
    </nav>
  );
}
export function JobsPage() {
  const [offset, setOffset] = useState(0),
    data = useSnapshot<Page<Job>>(`/api/v1/jobs?offset=${offset}`);
  return (
    <>
      <header className="page-heading">
        <div>
          <p className="eyebrow">LOGICAL EXECUTIONS</p>
          <h1>Jobs</h1>
        </div>
      </header>
      <Freshness {...data} />
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Job</th>
              <th>State</th>
              <th>Image</th>
              <th>Attempts</th>
              <th>Submitted</th>
            </tr>
          </thead>
          <tbody>
            {data.data?.items.map((j) => (
              <tr key={j.id}>
                <td>
                  <Link to={`/jobs/${j.id}`}>{j.key ?? short(j.id)}</Link>
                  <small className="mono">{short(j.id)}</small>
                </td>
                <td>
                  <Badge state={j.state} />
                </td>
                <td className="mono">{j.image}</td>
                <td className="mono">
                  {j.attempt_count}/{j.max_attempts}
                </td>
                <td>
                  <Stamp value={j.created_at} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.data && !data.data.items.length ? (
        <p className="empty">No jobs on this page.</p>
      ) : null}
      <Pager
        offset={offset}
        next={data.data?.next_offset ?? null}
        set={setOffset}
      />
    </>
  );
}
function JobInspection({ job, workers }: { job: Job; workers: Worker[] }) {
  return (
    <>
      <div className="section-heading">
        <h2>{job.key ?? short(job.id)}</h2>
        <Badge state={job.state} />
        <Link className="mono" to={`/jobs/${job.id}`}>
          {short(job.id)}
        </Link>
      </div>
      <div className="job-spec">
        <div>
          <span>Image</span>
          <code>{job.image}</code>
        </div>
        <div>
          <span>Argv</span>
          <code>{JSON.stringify(job.command)}</code>
        </div>
        <div>
          <span>Dependencies</span>
          <code>{job.dependencies?.join(", ") || "Root job"}</code>
        </div>
        <div>
          <span>Attempt budget / timeout</span>
          <code>
            {job.attempt_count}/{job.max_attempts} · {job.timeout_seconds}s
          </code>
        </div>
        {job.retry_available_at ? (
          <div>
            <span>Retry available at</span>
            <Stamp value={job.retry_available_at} />
          </div>
        ) : null}
      </div>
      <AttemptTable job={job} />
      <Timeline job={job} workers={workers} />
      <Cancel
        key={job.id}
        path={`/api/v1/jobs/${job.id}/cancel`}
        label="Cancel job"
        disabled={terminalJob(job.state) || job.state === "CANCELLING"}
      />
    </>
  );
}
export function PipelinePage() {
  const { id } = useParams(),
    data = useSnapshot<Pipeline>(`/api/v1/pipelines/${id}`),
    workers = useSnapshot<Worker[]>("/api/v1/workers"),
    [query, setQuery] = useSearchParams();
  const pipeline = data.data;
  const selected =
    pipeline?.jobs.find((j) => j.id === query.get("job")) ?? pipeline?.jobs[0];
  return (
    <>
      <header className="page-heading">
        <div>
          <p className="eyebrow">
            <Link to="/pipelines">PIPELINES</Link> / {id}
          </p>
          <h1>Pipeline execution</h1>
        </div>
        {pipeline ? <Badge state={pipeline.state} /> : null}
      </header>
      <Freshness {...data} />
      {pipeline ? (
        <>
          <div className="metadata-line">
            <span>
              Submitted <Stamp value={pipeline.created_at} />
            </span>
            <span>
              Started <Stamp value={pipeline.started_at} />
            </span>
            <span>
              Finished <Stamp value={pipeline.finished_at} />
            </span>
            <span className="mono">
              {pipeline.jobs.filter((j) => terminalJob(j.state)).length}/
              {pipeline.jobs.length} terminal
            </span>
          </div>
          <section>
            <div className="section-heading">
              <h2>Dependency graph</h2>
              <span className="muted">
                Select a node to inspect immutable attempts
              </span>
            </div>
            <Dag
              jobs={pipeline.jobs}
              selected={selected?.id ?? ""}
              onSelect={(job) => setQuery({ job })}
            />
          </section>
          {selected ? (
            <section className="job-inspection">
              <JobInspection job={selected} workers={workers.data ?? []} />
              {workers.error ? <Freshness {...workers} /> : null}
            </section>
          ) : null}
          <Cancel
            key={pipeline.id}
            path={`/api/v1/pipelines/${pipeline.id}/cancel`}
            label="Cancel pipeline"
            disabled={[
              "SUCCEEDED",
              "FAILED",
              "CANCELLED",
              "CANCELLING",
            ].includes(pipeline.state)}
          />
        </>
      ) : null}
    </>
  );
}
export function JobPage() {
  const { id } = useParams(),
    data = useSnapshot<Job>(`/api/v1/jobs/${id}`),
    workers = useSnapshot<Worker[]>("/api/v1/workers");
  return (
    <>
      <header className="page-heading">
        <div>
          <p className="eyebrow">JOB / {id}</p>
          <h1>Job inspection</h1>
          {data.data?.pipeline_id ? (
            <Link to={`/pipelines/${data.data.pipeline_id}?job=${id}`}>
              Open in pipeline DAG →
            </Link>
          ) : null}
        </div>
      </header>
      <Freshness {...data} />
      {data.data ? (
        <JobInspection job={data.data} workers={workers.data ?? []} />
      ) : null}
      {workers.error ? <Freshness {...workers} /> : null}
    </>
  );
}
export function AttemptPage() {
  const { id } = useParams(),
    data = useSnapshot<Job>(`/api/v1/attempts/${id}`),
    workers = useSnapshot<Worker[]>("/api/v1/workers");
  const job = data.data,
    attempt = job?.attempts.find((a) => a.attempt_id === id);
  return (
    <>
      <header className="page-heading">
        <div>
          <p className="eyebrow">IMMUTABLE ATTEMPT / {id}</p>
          <h1>Attempt #{attempt?.attempt_number ?? "—"}</h1>
          {job ? (
            <Link to={`/jobs/${job.id}`}>
              ← {job.key ?? short(job.id)} · all attempts
            </Link>
          ) : null}
        </div>
        {attempt ? <Badge state={attempt.state} /> : null}
      </header>
      <Freshness {...data} />
      {attempt && job ? (
        <>
          <dl className="attempt-metadata">
            <div>
              <dt>Worker</dt>
              <dd>{attempt.worker_id}</dd>
            </div>
            <div>
              <dt>Worker session</dt>
              <dd>{attempt.worker_session_id}</dd>
            </div>
            <div>
              <dt>Fencing token</dt>
              <dd>{attempt.fencing_token}</dd>
            </div>
            <div>
              <dt>Current pointer</dt>
              <dd>
                {job.current_attempt_id === id
                  ? "Current authoritative pointer"
                  : "Historical / stale for finalization"}
              </dd>
            </div>
            <div>
              <dt>Assigned</dt>
              <dd>
                <Stamp value={attempt.assigned_at} />
              </dd>
            </div>
            <div>
              <dt>Started</dt>
              <dd>
                <Stamp value={attempt.started_at} />
              </dd>
            </div>
            <div>
              <dt>Finished</dt>
              <dd>
                <Stamp value={attempt.finished_at} />
              </dd>
            </div>
            <div>
              <dt>Persisted lease expiry</dt>
              <dd>
                <Stamp value={attempt.lease_expires_at} />
              </dd>
            </div>
            <div>
              <dt>Execution deadline</dt>
              <dd>
                <Stamp value={attempt.execution_deadline_at} />
              </dd>
            </div>
            <div>
              <dt>Exit / failure kind</dt>
              <dd>
                {attempt.exit_code ?? "—"} / {attempt.failure_kind || "—"}
              </dd>
            </div>
          </dl>
          {attempt.failure_detail ? (
            <p className="failure-detail">{attempt.failure_detail}</p>
          ) : null}
          {attempt.trace_id ? (
            <a
              className="trace-link mono"
              href={`http://localhost:16686/trace/${attempt.trace_id}`}
              target="_blank"
              rel="noreferrer"
            >
              Jaeger trace {attempt.trace_id} ↗
            </a>
          ) : (
            <p className="muted">No persisted trace context.</p>
          )}
          <p className="boundary">
            Lease expiry is a persisted deadline, not a browser grant of
            authority. LOST does not prove remote physical shutdown.
          </p>
          <LiveLogs key={attempt.attempt_id} attempt={attempt} />
          <h2>Attempt history</h2>
          <AttemptTable job={job} />
          <Timeline job={job} workers={workers.data ?? []} />
          {workers.error ? <Freshness {...workers} /> : null}
        </>
      ) : null}
    </>
  );
}
export function WorkersPage() {
  const data = useSnapshot<Worker[]>("/api/v1/workers"),
    overview = useSnapshot<Overview>("/api/v1/overview"),
    jobs = useSnapshot<Page<Job>>("/api/v1/jobs");
  const [history, setHistory] = useState(false);
  const [selectedWorker, setSelectedWorker] = useState("");
  return (
    <>
      <header className="page-heading">
        <div>
          <p className="eyebrow">WORKER INCARNATIONS</p>
          <h1>Workers</h1>
          <p>
            A stable identity can have multiple sessions. Reservations survive
            transport loss until lease expiry.
          </p>
        </div>
        <label>
          <input
            type="checkbox"
            checked={history}
            onChange={(e) => setHistory(e.target.checked)}
          />{" "}
          Include historical sessions
        </label>
      </header>
      <Freshness {...data} />
      {overview.error ? <Freshness {...overview} /> : null}
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Worker / session</th>
              <th>State</th>
              <th>Control stream</th>
              <th>Slots</th>
              <th>Last heartbeat</th>
              <th>Assigned attempts</th>
            </tr>
          </thead>
          <tbody>
            {data.data
              ?.filter((w) => history || w.current)
              .map((w) => (
                <tr key={w.worker_session_id}>
                  <td>
                    <button
                      className="text-button mono"
                      onClick={() => setSelectedWorker(w.worker_id)}
                      aria-label={`Inspect ${w.worker_id} execution history`}
                    >
                      {w.worker_id}
                    </button>
                    <small className="mono" title={w.worker_session_id}>
                      {short(w.worker_session_id)} ·{" "}
                      {w.current ? "current" : "historical"}
                    </small>
                  </td>
                  <td>
                    <Badge state={w.state} />
                  </td>
                  <td>{w.connected ? "Connected" : "Disconnected"}</td>
                  <td className="mono">
                    {w.active_slots}/{w.capacity_slots} occupied
                  </td>
                  <td>
                    <Stamp value={w.last_seen_at} />
                  </td>
                  <td>
                    {overview.data
                      ? overview.data.active_attempts
                          .filter(
                            (a) => a.worker_session_id === w.worker_session_id,
                          )
                          .map((a) => (
                            <Link
                              className="assigned-link"
                              key={a.attempt_id}
                              to={`/attempts/${a.attempt_id}`}
                            >
                              #{a.attempt_number}{" "}
                              <span className="mono">
                                {short(a.attempt_id)}
                              </span>{" "}
                              <Badge state={a.state} />
                            </Link>
                          ))
                      : "Unavailable"}
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
        {data.data && !data.data.length ? (
          <p className="empty">No worker sessions registered.</p>
        ) : null}
      </div>
      <p className="muted">
        DRAINING is not implemented by this backend. Session state and assigned
        attempts are separate read snapshots; refresh may briefly show different
        observations.
      </p>
      {selectedWorker ? (
        <section>
          <h2>{selectedWorker} · recent execution history</h2>
          <p className="muted">
            Attempts from the latest 50 submitted jobs; this is a bounded
            inspection window, not a complete worker archive.
          </p>
          <Freshness {...jobs} />
          {jobs.data?.items
            .filter((j) =>
              j.attempts.some((a) => a.worker_id === selectedWorker),
            )
            .map((j) => (
              <div key={j.id}>
                <h3>
                  <Link to={`/jobs/${j.id}`}>{j.key ?? short(j.id)}</Link>
                </h3>
                <AttemptTable
                  job={{
                    ...j,
                    attempts: j.attempts.filter(
                      (a) => a.worker_id === selectedWorker,
                    ),
                  }}
                />
              </div>
            ))}
          {jobs.data &&
          !jobs.data.items.some((j) =>
            j.attempts.some((a) => a.worker_id === selectedWorker),
          ) ? (
            <p className="empty">No attempts in this inspection window.</p>
          ) : null}
        </section>
      ) : null}
    </>
  );
}
const example = JSON.stringify(
  {
    jobs: [
      {
        key: "build",
        image: "alpine:3.22",
        command: ["echo", "build"],
        timeout_seconds: 30,
        max_attempts: 2,
      },
      {
        key: "unit-test",
        image: "alpine:3.22",
        command: ["sleep", "5"],
        dependencies: ["build"],
        timeout_seconds: 30,
        max_attempts: 2,
      },
      {
        key: "lint",
        image: "alpine:3.22",
        command: ["echo", "lint"],
        dependencies: ["build"],
        timeout_seconds: 30,
        max_attempts: 2,
      },
      {
        key: "package",
        image: "alpine:3.22",
        command: ["echo", "package"],
        dependencies: ["unit-test", "lint"],
        timeout_seconds: 30,
        max_attempts: 2,
      },
    ],
  },
  null,
  2,
);
export function SubmitPage() {
  const [payload, setPayload] = useState(example),
    [error, setError] = useState(""),
    [pending, setPending] = useState(false);
  const navigate = useNavigate();
  return (
    <>
      <header className="page-heading">
        <div>
          <p className="eyebrow">EXISTING STATIC DAG API</p>
          <h1>Submit pipeline</h1>
          <p>
            Trusted local Docker workloads. Commands are argv arrays. Validation
            and execution remain server-owned.
          </p>
        </div>
      </header>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          setPending(true);
          setError("");
          void (async () => {
            try {
              const spec: unknown = JSON.parse(payload);
              const p = await api<{ id: string }>(
                "/api/v1/pipelines",
                undefined,
                spec,
              );
              navigate(`/pipelines/${p.id}`);
            } catch (e) {
              setError(e instanceof Error ? e.message : "Submission failed");
            } finally {
              setPending(false);
            }
          })();
        }}
      >
        <label htmlFor="pipeline-json">
          Pipeline JSON{" "}
          <span className="muted">
            · editable example, no submission until requested
          </span>
        </label>
        <textarea
          id="pipeline-json"
          spellCheck={false}
          value={payload}
          onChange={(e) => setPayload(e.target.value)}
          rows={22}
          maxLength={1048576}
        />
        {error ? (
          <p className="degraded" role="alert">
            {error}
          </p>
        ) : null}
        <button className="primary" disabled={pending} type="submit">
          {pending ? "Submitting…" : "Submit pipeline"}
        </button>
      </form>
    </>
  );
}
