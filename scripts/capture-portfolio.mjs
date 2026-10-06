// Read-only captures of real persisted execution, using existing frontend tooling.
import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = fileURLToPath(new URL("../", import.meta.url));
const require = createRequire(path.join(root, "frontend/package.json"));
const { chromium, expect } = require("@playwright/test");
const args = process.argv.slice(2);
const options = new Map();
for (let i = 0; i < args.length; i += 2) {
  assert(["--pipeline-id", "--dag-pipeline-id", "--base-url"].includes(args[i]), "Unknown argument");
  assert(args[i + 1] && !args[i + 1].startsWith("--"), "Missing argument value");
  assert(!options.has(args[i]), "Duplicate argument");
  options.set(args[i], args[i + 1]);
}
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const recoveryID = options.get("--pipeline-id");
assert(uuid.test(recoveryID ?? ""), "Use --pipeline-id with the real BROWSER_RECOVERY_PIPELINE UUID");
const base = new URL(options.get("--base-url") ?? "http://127.0.0.1:5173");
assert(base.protocol === "http:" && ["localhost", "127.0.0.1"].includes(base.hostname)
  && !base.username && !base.password && base.pathname === "/" && !base.search && !base.hash,
  "Capture only from a loopback HTTP console origin");
async function read(route) {
  const response = await fetch(new URL(route, base), { signal: AbortSignal.timeout(8000) });
  assert(response.ok, `Read failed: ${route} HTTP ${response.status}`);
  return response.json();
}
const recovery = await read(`/api/v1/pipelines/${recoveryID}`);
assert.equal(recovery.state, "SUCCEEDED");
const testJob = recovery.jobs.find((j) => j.key === "test");
assert(testJob && testJob.state === "SUCCEEDED" && testJob.attempts.length === 2);
const [old, winner] = testJob.attempts;
assert.deepEqual(testJob.attempts.map((a) => [a.state, a.worker_id, a.attempt_number, a.fencing_token]),
  [["LOST", "worker-b", 1, 1], ["SUCCEEDED", "worker-c", 2, 2]]);
assert.equal(old.failure_kind, "LEASE_EXPIRED");
assert.equal(testJob.current_attempt_id, winner.attempt_id);
assert(recovery.jobs.some((j) => j.dependencies?.includes("test") && j.state === "SUCCEEDED"));
const workers = await read("/api/v1/workers");
for (const a of [old, winner]) assert(workers.some((w) =>
  w.worker_id === a.worker_id && w.worker_session_id === a.worker_session_id), "Persisted session missing");
const output = new Map();
for (const a of [old, winner]) {
  const chunks = await read(`/api/v1/attempts/${a.attempt_id}/logs?after=0`);
  const chunk = chunks.find((c) => c.stream === "STDOUT" && Buffer.from(c.payload, "base64").toString("utf8").trim());
  assert(chunk, "Retained stdout required for attempt capture");
  output.set(a.attempt_id, Buffer.from(chunk.payload, "base64").toString("utf8").trim());
}
function isDiamond(p) {
  const jobs = new Map(p.jobs.map((j) => [j.key, j]));
  return p.state === "SUCCEEDED" && p.jobs.length === 4 && p.jobs.every((j) => j.state === "SUCCEEDED")
    && jobs.has("build") && jobs.has("unit-test") && jobs.has("lint") && jobs.has("package")
    && (jobs.get("build").dependencies ?? []).length === 0
    && JSON.stringify(jobs.get("unit-test").dependencies) === '["build"]'
    && JSON.stringify(jobs.get("lint").dependencies) === '["build"]'
    && JSON.stringify([...jobs.get("package").dependencies].sort()) === '["lint","unit-test"]';
}
let dag;
const dagID = options.get("--dag-pipeline-id");
if (dagID) {
  assert(uuid.test(dagID), "Invalid DAG UUID");
  dag = await read(`/api/v1/pipelines/${dagID}`);
  assert(isDiamond(dag), "DAG must be the real succeeded four-job demo");
} else {
  const list = await read("/api/v1/pipelines");
  for (const p of list.items.filter((p) => p.state === "SUCCEEDED" && p.job_count === 4)) {
    const candidate = await read(`/api/v1/pipelines/${p.id}`);
    if (isDiamond(candidate)) { dag = candidate; break; }
  }
  assert(dag, "Run demo-pipeline.ps1 first, or supply --dag-pipeline-id");
}
const directory = path.join(root, "docs/assets/console");
await mkdir(directory, { recursive: true });
const browser = await chromium.launch();
const errors = [];
const captured = [];
try {
  const context = await browser.newContext({ viewport: { width: 1600, height: 1050 }, timezoneId: "UTC" });
  const page = await context.newPage();
  page.on("pageerror", (e) => errors.push(e.message));
  async function visit(route, heading) {
    await page.goto(new URL(route, base).href);
    await expect(page.getByRole("heading", { name: heading, exact: true })).toBeVisible();
    await expect(page.getByText(/Snapshot .*refresh every 2s/).first()).toBeVisible();
    await expect(page.getByRole("alert")).toHaveCount(0);
  }
  async function capture(name, fullPage = false) {
    assert.deepEqual(errors, [], "Console JavaScript error");
    await page.screenshot({ path: path.join(directory, name), fullPage });
    captured.push(name);
  }
  await visit("/", "System overview");
  await expect(page.getByText("Control Plane + PostgreSQL healthy", { exact: true })).toBeVisible();
  await expect(page.getByRole("row").filter({ hasText: old.attempt_id.slice(0, 8) })).toBeVisible();
  await capture("overview.png");
  await visit(`/pipelines/${dag.id}?job=${dag.jobs.find((j) => j.key === "unit-test").id}`, "Pipeline execution");
  for (const j of dag.jobs) await expect(page.getByRole("button", { name: `Select job ${j.key}: SUCCEEDED` })).toBeVisible();
  await capture("pipeline-dag.png", true);
  await visit("/workers", "Workers");
  for (const worker of ["worker-a", "worker-b", "worker-c"]) {
    await expect(page.getByRole("button", { name: `Inspect ${worker} execution history` })).toBeVisible();
  }
  await capture("workers.png");
  for (const [a, filename] of [[winner, "attempt-logs.png"], [old, "attempt-lost.png"]]) {
    await visit(`/attempts/${a.attempt_id}`, `Attempt #${a.attempt_number}`);
    await expect(page.getByLabel("Live stdout and stderr")).toContainText(output.get(a.attempt_id));
    await expect(page.getByText("Connected", { exact: true })).toBeVisible();
    await capture(filename, true);
  }
  await visit(`/pipelines/${recovery.id}?job=${testJob.id}`, "Pipeline execution");
  for (const title of ["Attempt #1 LOST", "Bounded retry #1", "Attempt #2 SUCCEEDED"]) {
    await expect(page.getByText(title, { exact: true })).toBeVisible();
  }
  await capture("recovery.png", true);
} finally {
  await browser.close();
}
assert.deepEqual(errors, []);
const manifest = `# Actual console capture provenance

Captured ${new Date().toISOString()} from ${base.origin}, Chromium ${browser.version()}, Node ${process.version}. Viewport 1600 × 1050, timezone UTC. Production console/API; no fixtures, response interception, database mutations or image editing. Counts/history reflect the retained local database, not benchmarks.

- Recovery pipeline: \`${recovery.id}\` (SUCCEEDED).
- Recovery job: \`${testJob.id}\`; current pointer \`${winner.attempt_id}\`.
- Old attempt: \`${old.attempt_id}\`, LOST / LEASE_EXPIRED, worker-b session \`${old.worker_session_id}\`, attempt 1 / fence 1.
- Winner: \`${winner.attempt_id}\`, SUCCEEDED, worker-c session \`${winner.worker_session_id}\`, attempt 2 / fence 2.
- Fan-out/fan-in pipeline: \`${dag.id}\` (all four jobs SUCCEEDED).
- Assets: ${captured.map((name) => `[${name}](${name})`).join(", ")}.

Reproduce from the root: \`node ./scripts/capture-portfolio.mjs --pipeline-id ${recovery.id} --dag-pipeline-id ${dag.id}\`. These IDs require this retained database; a fresh run must use new IDs from the real demos/test. See [workflow](../../screenshots.md).
`;
await writeFile(path.join(directory, "capture.md"), manifest, "utf8");
console.log(`PORTFOLIO_CAPTURE_PASS=${captured.length} real-state screenshots`);
console.log(`RECOVERY_PIPELINE=${recovery.id}`);
console.log(`DAG_PIPELINE=${dag.id}`);
