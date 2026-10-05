import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import type { Attempt, Job, Worker, LogChunk } from "./models";
import { terminalJob } from "./models";
import { chronology, dagLayers, short, time } from "./presentation";
import { appendChunk, emptyBuffer } from "./logs";
import { api } from "./api";
export function Badge({ state }: { state: string }) {
  return (
    <span className={`badge state-${state.toLowerCase()}`}>
      <span aria-hidden="true" className="dot" />
      {state.replaceAll("_", " ")}
    </span>
  );
}
export function Freshness({
  error,
  updated,
}: {
  error?: string;
  updated?: Date;
}) {
  return (
    <div
      className={`freshness ${error ? "degraded" : ""}`}
      role={error ? "alert" : "status"}
    >
      {error ? (
        <>
          <strong>Inspection unavailable.</strong> {error}{" "}
          {updated
            ? "Showing last successful snapshot; data may be stale."
            : "No authoritative data available."}
        </>
      ) : updated ? (
        `Snapshot ${updated.toLocaleTimeString("en-GB")} · refresh every 2s`
      ) : (
        "Loading authoritative snapshot…"
      )}
    </div>
  );
}
export function Stamp({ value }: { value: string | null | undefined }) {
  return (
    <time
      dateTime={value ?? undefined}
      title={value ?? "Not persisted"}
      className="mono"
    >
      {time(value)}
    </time>
  );
}
export function AttemptTable({ job }: { job: Job }) {
  return (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Attempt</th>
            <th>State</th>
            <th>Worker / session</th>
            <th>Fence</th>
            <th>Failure</th>
          </tr>
        </thead>
        <tbody>
          {job.attempts.map((a) => (
            <tr key={a.attempt_id}>
              <td>
                <Link to={`/attempts/${a.attempt_id}`}>
                  #{a.attempt_number}{" "}
                  <span className="mono">{short(a.attempt_id)}</span>
                </Link>
                {job.current_attempt_id === a.attempt_id ? (
                  <small>Current pointer</small>
                ) : (
                  <small>Historical attempt</small>
                )}
              </td>
              <td>
                <Badge state={a.state} />
              </td>
              <td className="mono">
                {a.worker_id}
                <small title={a.worker_session_id}>
                  {short(a.worker_session_id)}
                </small>
              </td>
              <td className="mono">{a.fencing_token}</td>
              <td className="mono">{a.failure_kind || "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {!job.attempts.length ? (
        <p className="empty">
          No attempts created.{" "}
          {job.state === "BLOCKED"
            ? "Waiting for required dependencies."
            : terminalJob(job.state)
              ? "Job is terminal; no execution was assigned."
              : "Awaiting scheduling."}
        </p>
      ) : null}
    </div>
  );
}
export function Timeline({ job, workers }: { job: Job; workers: Worker[] }) {
  const entries = chronology(job, workers);
  return (
    <section>
      <h2>Execution & recovery chronology</h2>
      <p className="muted">
        Persisted attempt boundaries. A LOST attempt can still have a surviving
        Docker container.
      </p>
      <ol className="timeline">
        {entries.map((e, i) => (
          <li key={`${e.at}-${i}`}>
            <span
              className={`timeline-mark state-${e.kind.toLowerCase()}`}
              aria-hidden="true"
            />
            <Stamp value={e.at} />
            <div>
              <strong>{e.title}</strong>
              <p>{e.detail}</p>
            </div>
          </li>
        ))}
      </ol>
      {!entries.length ? (
        <p className="empty">No execution history yet.</p>
      ) : null}
    </section>
  );
}
export function Dag({
  jobs,
  selected,
  onSelect,
}: {
  jobs: Job[];
  selected: string;
  onSelect: (id: string) => void;
}) {
  const layers = dagLayers(jobs);
  const positions = new Map<string, { x: number; y: number }>();
  layers.forEach((layer, l) =>
    layer.forEach((job, r) =>
      positions.set(job.key ?? job.id, { x: l * 260 + 16, y: r * 104 + 16 }),
    ),
  );
  const width = Math.max(280, layers.length * 260);
  const height = Math.max(120, ...layers.map((l) => l.length * 104 + 16));
  return (
    <div className="dag-scroll" aria-label="Static dependency graph">
      <div className="dag" style={{ width, height }}>
        <svg width={width} height={height} aria-hidden="true">
          <defs>
            <marker
              id="arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto"
            >
              <path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor" />
            </marker>
          </defs>
          {jobs.flatMap((j) =>
            (j.dependencies ?? []).map((parent) => {
              const from = positions.get(parent),
                to = positions.get(j.key ?? j.id);
              if (!from || !to) return null;
              return (
                <path
                  key={`${parent}-${j.id}`}
                  d={`M ${from.x + 220} ${from.y + 38} H ${to.x - 14} V ${to.y + 38} H ${to.x}`}
                  fill="none"
                  markerEnd="url(#arrow)"
                />
              );
            }),
          )}
        </svg>
        {jobs.map((j) => {
          const p = positions.get(j.key ?? j.id);
          return (
            <button
              key={j.id}
              className={`dag-node ${selected === j.id ? "selected" : ""}`}
              style={{ left: p?.x, top: p?.y }}
              onClick={() => onSelect(j.id)}
              aria-pressed={selected === j.id}
              aria-label={`Select job ${j.key ?? short(j.id)}: ${j.state}`}
            >
              <strong>{j.key ?? short(j.id)}</strong>
              <Badge state={j.state} />
              <span className="mono">
                {j.attempt_count}/{j.max_attempts} attempts
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}
export function Cancel({
  path,
  label,
  disabled,
}: {
  path: string;
  label: string;
  disabled: boolean;
}) {
  const [confirm, setConfirm] = useState(false),
    [pending, setPending] = useState(false),
    [message, setMessage] = useState("");
  const send = async () => {
    setPending(true);
    try {
      await api(path, undefined, {});
      setMessage("Cancellation requested. Awaiting authoritative state.");
      setConfirm(false);
    } catch (e) {
      setMessage(e instanceof Error ? e.message : "Request failed");
    } finally {
      setPending(false);
    }
  };
  return (
    <div className="cancel">
      {confirm ? (
        <>
          <span>Request cancellation?</span>
          <button disabled={pending} onClick={() => void send()}>
            Confirm {label}
          </button>
          <button disabled={pending} onClick={() => setConfirm(false)}>
            Keep executing
          </button>
        </>
      ) : (
        <button
          className="danger"
          disabled={disabled || pending}
          onClick={() => setConfirm(true)}
        >
          {label}
        </button>
      )}
      {message ? <p role="status">{message}</p> : null}
    </div>
  );
}
export function LiveLogs({ attempt }: { attempt: Attempt }) {
  const [view, setView] = useState({
    id: attempt.attempt_id,
    buffer: emptyBuffer(),
    status: "Connecting",
  });
  const [follow, setFollow] = useState(true);
  const logPane = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const id = attempt.attempt_id;
    setView({ id, buffer: emptyBuffer(), status: "Connecting" });
    const source = new EventSource(
      `/api/v1/attempts/${id}/logs/stream?after=0`,
    );
    source.onopen = () => setView((v) => ({ ...v, status: "Connected" }));
    source.onerror = () =>
      setView((v) => ({
        ...v,
        status:
          source.readyState === EventSource.CLOSED
            ? "Disconnected"
            : "Reconnecting",
      }));
    source.addEventListener("chunk", (event) => {
      try {
        const chunk = JSON.parse(
          (event as MessageEvent<string>).data,
        ) as LogChunk;
        setView((v) => ({ ...v, buffer: appendChunk(v.buffer, chunk) }));
      } catch {
        setView((v) => ({ ...v, status: "Invalid log data" }));
      }
    });
    return () => source.close();
  }, [attempt.attempt_id]);
  const buffer = view.id === attempt.attempt_id ? view.buffer : emptyBuffer();
  useEffect(() => {
    if (follow && logPane.current)
      logPane.current.scrollTop = logPane.current.scrollHeight;
  }, [buffer.cursor, follow]);
  return (
    <section className="log-section">
      <div className="section-heading">
        <h2>Attempt output</h2>
        <span
          role="status"
          className={view.status === "Connected" ? "connection" : "degraded"}
        >
          {view.status}
        </span>
        <label>
          <input
            type="checkbox"
            checked={follow}
            onChange={(e) => setFollow(e.target.checked)}
          />{" "}
          Follow output
        </label>
      </div>
      <div className="log-meta mono">
        Cursor {buffer.cursor} · stdout / stderr · last 200 chunks / 128 Ki
        characters{" "}
        {buffer.discarded ? `· ${buffer.discarded} older chunks removed` : ""}
      </div>
      <div ref={logPane} className="logs" aria-label="Live stdout and stderr">
        <div>
          {buffer.lines.map((l) => (
            <div
              className={`log-line ${l.stream.toLowerCase()}`}
              key={l.sequence}
            >
              <span className="log-sequence">{l.sequence}</span>
              <span className="log-stream">
                {l.stream === "STDERR" ? "err" : "out"}
              </span>
              <pre>{l.text}</pre>
            </div>
          ))}
          {!buffer.lines.length ? (
            <p className="empty">No persisted output yet.</p>
          ) : null}
        </div>
      </div>
      <p className="muted">
        SSE reconnect resumes from the last event sequence. Output is
        diagnostic, including for LOST attempts.
      </p>
    </section>
  );
}
