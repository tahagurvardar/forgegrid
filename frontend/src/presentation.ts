import type { Job, Worker } from "./models";
export const short = (id: string) => id.slice(0, 8);
export const time = (value: string | null | undefined) =>
  value ? new Date(value).toLocaleString("en-GB", { hour12: false }) : "—";
export function duration(
  start: string | null | undefined,
  end: string | null | undefined,
) {
  if (!start || !end) return "—";
  const seconds = Math.max(0, (Date.parse(end) - Date.parse(start)) / 1000);
  return `${seconds.toFixed(1)}s`;
}
export function dagLayers(jobs: Job[]): Job[][] {
  const pending = new Map(jobs.map((j) => [j.key ?? j.id, j]));
  const done = new Set<string>();
  const layers: Job[][] = [];
  while (pending.size) {
    const layer = [...pending.values()].filter((j) =>
      (j.dependencies ?? []).every((d) => done.has(d)),
    );
    if (!layer.length) {
      layers.push([...pending.values()]);
      break;
    }
    layers.push(layer);
    for (const j of layer) {
      pending.delete(j.key ?? j.id);
      done.add(j.key ?? j.id);
    }
  }
  return layers;
}
export interface TimelineEntry {
  at: string;
  title: string;
  detail: string;
  kind: string;
}
export function chronology(job: Job, workers: Worker[]): TimelineEntry[] {
  const entries: TimelineEntry[] = [];
  for (const a of job.attempts) {
    const prefix = `Attempt #${a.attempt_number}`;
    entries.push({
      at: a.assigned_at,
      title: `${prefix} assigned to ${a.worker_id}`,
      detail: `Fence ${a.fencing_token} · session ${short(a.worker_session_id)}`,
      kind: "ASSIGNED",
    });
    if (a.started_at)
      entries.push({
        at: a.started_at,
        title: `${prefix} started`,
        detail: `${a.worker_id} · persisted AttemptStarted`,
        kind: "RUNNING",
      });
    const session = workers.find(
      (w) => w.worker_session_id === a.worker_session_id,
    );
    if (a.failure_kind === "LEASE_EXPIRED") {
      if (session)
        entries.push({
          at: session.last_seen_at,
          title: `${a.worker_id}: last persisted heartbeat`,
          detail: `Session is now ${session.state}. Exact offline transition time is not persisted.`,
          kind: session.state,
        });
      entries.push({
        at: a.lease_expires_at,
        title: `${prefix} lease expired`,
        detail:
          "Persisted lease deadline; ownership cannot transfer before this boundary.",
        kind: "LOST",
      });
    }
    if (a.finished_at)
      entries.push({
        at: a.finished_at,
        title: `${prefix} ${a.state}`,
        detail: a.failure_kind || `Exit ${a.exit_code ?? "—"}`,
        kind: a.state,
      });
    if (a.attempt_number > 1)
      entries.push({
        at: a.assigned_at,
        title: `Bounded retry #${a.attempt_number - 1}`,
        detail: `Fresh attempt and fence · ${a.attempt_number}/${job.max_attempts} attempts used`,
        kind: "RETRY_WAIT",
      });
  }
  return entries.sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
}
