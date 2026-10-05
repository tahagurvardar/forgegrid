import { render, screen } from "@testing-library/react";
import { BrowserRouter } from "react-router-dom";
import { AttemptTable, Badge, Freshness, Timeline } from "./components";
import { baseAttempt, oldWorker, recoveryJob } from "./fixtures";
import { chronology, dagLayers } from "./presentation";
import { appendChunk, emptyBuffer } from "./logs";
test("state rendering preserves LOST and current retry after logical success", () => {
  render(
    <BrowserRouter>
      <AttemptTable job={recoveryJob} />
      <Timeline job={recoveryJob} workers={[oldWorker]} />
    </BrowserRouter>,
  );
  expect(screen.getByText("LOST")).toBeInTheDocument();
  expect(screen.getByText("Historical attempt")).toBeInTheDocument();
  expect(screen.getByText("Current pointer")).toBeInTheDocument();
  expect(screen.getByText("Attempt #1 LOST")).toBeInTheDocument();
  expect(screen.getByText("Attempt #2 SUCCEEDED")).toBeInTheDocument();
  expect(
    screen.getByText(/Exact offline transition time is not persisted/),
  ).toBeInTheDocument();
});
test.each([
  "BLOCKED",
  "QUEUED",
  "DISPATCHED",
  "RUNNING",
  "RETRY_WAIT",
  "SUCCEEDED",
  "FAILED",
  "SKIPPED",
  "CANCELLED",
  "CANCELLING",
  "SUPERSEDED",
])("renders precise %s state", (state) => {
  render(<Badge state={state} />);
  expect(screen.getByText(state.replaceAll("_", " "))).toBeInTheDocument();
});
test("stale snapshots are visibly unavailable instead of appearing current", () => {
  render(<Freshness error="503: database unavailable" updated={new Date()} />);
  expect(screen.getByRole("alert")).toHaveTextContent("data may be stale");
});
test.each(["SKIPPED", "CANCELLED"])(
  "unassigned %s jobs do not claim they are awaiting scheduling",
  (state) => {
    render(
      <BrowserRouter>
        <AttemptTable job={{ ...recoveryJob, state, attempts: [] }} />
      </BrowserRouter>,
    );
    expect(
      screen.getByText(/Job is terminal; no execution was assigned/),
    ).toBeInTheDocument();
    expect(screen.queryByText(/Awaiting scheduling/)).not.toBeInTheDocument();
  },
);
test("chronology has stored lease boundary and bounded retry but no invented offline timestamp", () => {
  const events = chronology(recoveryJob, [oldWorker]);
  expect(events.find((e) => e.title.includes("lease expired"))?.at).toBe(
    baseAttempt.lease_expires_at,
  );
  expect(events.some((e) => e.title === "Bounded retry #1")).toBe(true);
  expect(events.some((e) => e.title === "worker-b offline")).toBe(false);
  expect(events.filter((e) => e.title.includes("lease expired"))).toHaveLength(
    1,
  );
});
test("fan-in is placed only after both dependency layers", () => {
  const job = (key: string, deps: string[]) => ({
    ...recoveryJob,
    id: key,
    key,
    dependencies: deps,
  });
  const layers = dagLayers([
    job("package", ["unit", "lint"]),
    job("lint", ["build"]),
    job("build", []),
    job("unit", ["build"]),
  ]);
  expect(layers.map((l) => l.map((j) => j.key))).toEqual([
    ["build"],
    ["lint", "unit"],
    ["package"],
  ]);
});
test("lease loss does not invent an offline session state", () => {
  const entries = chronology(recoveryJob, [{ ...oldWorker, state: "ONLINE" }]);
  const heartbeat = entries.find((e) =>
    e.title.includes("last persisted heartbeat"),
  );
  expect(heartbeat?.kind).toBe("ONLINE");
  expect(heartbeat?.detail).toContain("Session is now ONLINE");
});
test("logs deduplicate replay and retain bounded memory with monotonic cursor", () => {
  let buffer = emptyBuffer();
  const chunk = {
    sequence: 1,
    stream: "STDERR" as const,
    payload: btoa("proof"),
  };
  buffer = appendChunk(buffer, chunk);
  expect(appendChunk(buffer, chunk)).toBe(buffer);
  for (let sequence = 2; sequence < 300; sequence++)
    buffer = appendChunk(buffer, {
      ...chunk,
      sequence,
      payload: btoa("x".repeat(2000)),
    });
  expect(buffer.cursor).toBe(299);
  expect(buffer.lines.length).toBeLessThanOrEqual(200);
  expect(buffer.bytes).toBeLessThanOrEqual(131072);
  expect(buffer.discarded).toBeGreaterThan(0);
  expect(buffer.lines.every((l) => l.stream === "STDERR")).toBe(true);
});
test("UTF-8 split across chunks is decoded independently for stdout and stderr", () => {
  const bytes = new TextEncoder().encode("🐳");
  const encode = (part: Uint8Array) => btoa(String.fromCharCode(...part));
  let b = appendChunk(emptyBuffer(), {
    sequence: 1,
    stream: "STDOUT",
    payload: encode(bytes.slice(0, 2)),
  });
  b = appendChunk(b, { sequence: 2, stream: "STDERR", payload: btoa("err") });
  b = appendChunk(b, {
    sequence: 3,
    stream: "STDOUT",
    payload: encode(bytes.slice(2)),
  });
  expect(b.lines.map((l) => l.text).join("")).toBe("err🐳");
  expect(b.carry.STDOUT).toEqual([]);
});
