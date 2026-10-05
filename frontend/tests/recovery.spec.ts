import { test, expect } from "@playwright/test";
import { execFileSync } from "node:child_process";
import path from "node:path";
import type { Pipeline } from "../src/models";
const root = path.resolve(import.meta.dirname, "../..");
function compose(...args: string[]) {
  execFileSync("docker", ["compose", ...args], {
    cwd: root,
    stdio: "pipe",
    timeout: 60000,
  });
}
test("real worker-b loss becomes LOST, worker-c retry succeeds, and historical logs stay inspectable", async ({
  page,
  request,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  try {
    compose("stop", "worker-a", "worker-b", "worker-c");
    compose("up", "-d", "--wait", "worker-b");
    await expect
      .poll(async () => {
        const r = await request.get("/api/v1/workers");
        return (
          (await r.json()) as {
            worker_id: string;
            current: boolean;
            connected: boolean;
          }[]
        ).some((w) => w.worker_id === "worker-b" && w.current && w.connected);
      })
      .toBe(true);
    await page.goto("/pipelines/new");
    await page.getByLabel("Pipeline JSON", { exact: false }).fill(
      JSON.stringify({
        jobs: [
          {
            key: "build",
            image: "alpine:3.22",
            command: ["echo", "browser-build"],
            timeout_seconds: 60,
            max_attempts: 2,
          },
          {
            key: "test",
            image: "alpine:3.22",
            command: [
              "/bin/sh",
              "-c",
              "echo browser-recovery-log; sleep 12; echo completed",
            ],
            dependencies: ["build"],
            timeout_seconds: 60,
            max_attempts: 2,
          },
          {
            key: "package",
            image: "alpine:3.22",
            command: ["echo", "package"],
            dependencies: ["test"],
            timeout_seconds: 60,
            max_attempts: 2,
          },
        ],
      }),
    );
    await page
      .getByRole("button", { name: "Submit pipeline", exact: true })
      .click();
    await expect(page).toHaveURL(/\/pipelines\/[a-f0-9-]{36}/);
    const id = new URL(page.url()).pathname.split("/").at(-1)!;
    const read = async () =>
      (await (await request.get(`/api/v1/pipelines/${id}`)).json()) as Pipeline;
    await expect
      .poll(
        async () => (await read()).jobs.find((j) => j.key === "test")?.state,
        { timeout: 30000 },
      )
      .toBe("RUNNING");
    const running = (await read()).jobs.find((j) => j.key === "test")!;
    const old = running.attempts[0]!;
    await page
      .getByRole("button", { name: "Select job test: RUNNING" })
      .click();
    await page
      .getByRole("link", { name: `#1 ${old.attempt_id.slice(0, 8)}` })
      .click();
    await expect(page.getByText("browser-recovery-log")).toBeVisible();
    compose("kill", "-s", "SIGKILL", "worker-b");
    compose("up", "-d", "--wait", "worker-c");
    await expect
      .poll(
        async () => {
          const r = await request.get("/api/v1/workers");
          return (
            (await r.json()) as { worker_session_id: string; state: string }[]
          ).find((w) => w.worker_session_id === old.worker_session_id)?.state;
        },
        { timeout: 15000 },
      )
      .toBe("OFFLINE");
    await expect(page.getByText("Attempt #1 LOST")).toBeVisible({
      timeout: 25000,
    });
    await expect(page.getByText("Bounded retry #1")).toBeVisible({
      timeout: 15000,
    });
    await expect
      .poll(async () => (await read()).state, { timeout: 40000 })
      .toBe("SUCCEEDED");
    await expect(page.getByText("Attempt #2 SUCCEEDED")).toBeVisible({
      timeout: 10000,
    });
    const completed = (await read()).jobs.find((j) => j.key === "test")!;
    expect(
      completed.attempts.map((a) => [a.state, a.worker_id, a.fencing_token]),
    ).toEqual([
      ["LOST", "worker-b", 1],
      ["SUCCEEDED", "worker-c", 2],
    ]);
    await expect(page.getByText("LOST", { exact: true })).toHaveCount(2); // page badge and immutable history row
    await page
      .getByRole("link", {
        name: `#2 ${completed.attempts[1]!.attempt_id.slice(0, 8)}`,
      })
      .click();
    await expect(page.getByText("completed", { exact: true })).toBeVisible();
    await expect(page.getByText("Current authoritative pointer")).toBeVisible();
    await page.goto(`/pipelines/${id}`);
    await expect(
      page.getByRole("button", { name: "Select job package: SUCCEEDED" }),
    ).toBeVisible();
    await page
      .getByRole("button", { name: "Select job test: SUCCEEDED" })
      .click();
    await expect(page.getByText("Attempt #1 LOST")).toBeVisible();
    await expect(page.getByText("Attempt #2 SUCCEEDED")).toBeVisible();
    expect(errors).toEqual([]);
    console.log(
      `BROWSER_RECOVERY_PIPELINE=http://localhost:5173/pipelines/${id}`,
    );
    console.log(
      `BROWSER_LOST_ATTEMPT=http://localhost:5173/attempts/${old.attempt_id}`,
    );
  } finally {
    compose("up", "-d", "--wait", "worker-a", "worker-b", "worker-c");
  }
});
