import { test, expect } from "@playwright/test";
import {
  baseAttempt,
  fixturePipeline,
  oldWorker,
  recoveryJob,
} from "../src/fixtures";
test.beforeEach(async ({ page }) => {
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const json =
      path === "/api/v1/pipelines"
        ? {
            items: [
              {
                ...fixturePipeline,
                job_count: 2,
                terminal_count: 2,
                attempt_count: 3,
              },
            ],
            next_offset: null,
          }
        : path === "/api/v1/jobs"
          ? { items: [recoveryJob], next_offset: null }
          : path.startsWith("/api/v1/pipelines/")
            ? fixturePipeline
            : path === "/api/v1/workers"
              ? [oldWorker]
              : path === "/api/v1/overview"
                ? {
                    server_time: baseAttempt.finished_at,
                    jobs: { SUCCEEDED: 2 },
                    recent_activity: recoveryJob.attempts,
                    active_attempts: [],
                  }
                : path.includes("/logs/stream")
                  ? null
                  : recoveryJob;
    if (path.includes("/logs/stream")) {
      await route.fulfill({
        contentType: "text/event-stream",
        body: `id: 1\nevent: chunk\ndata: ${JSON.stringify({ sequence: 1, stream: "STDOUT", payload: Buffer.from("browser-log-proof").toString("base64") })}\n\n`,
      });
      return;
    }
    await route.fulfill({ json });
  });
  await page.route("**/healthz", (route) =>
    route.fulfill({ json: { status: "ok" } }),
  );
});
test("DAG selection exposes recovery, prior LOST attempt, fence and live logs", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await page.goto(`/pipelines/${fixturePipeline.id}`);
  await expect(
    page.getByRole("heading", { name: "Pipeline execution" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Select job test: SUCCEEDED" })
    .click();
  await expect(page.getByText("Attempt #1 LOST")).toBeVisible();
  await expect(page.getByText("Attempt #2 SUCCEEDED")).toBeVisible();
  await page.getByRole("link", { name: "#1 11111111" }).click();
  await expect(
    page.getByRole("heading", { name: "Attempt #1", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Historical / stale for finalization"),
  ).toBeVisible();
  await expect(page.getByText("browser-log-proof")).toBeVisible();
  await expect(
    page.getByRole("cell", { name: "LEASE_EXPIRED", exact: true }),
  ).toBeVisible();
  expect(errors).toEqual([]);
});
test("backend outage is honest while last successful data remains visible", async ({
  page,
}) => {
  await page.goto("/");
  await expect(
    page.getByText("Control Plane + PostgreSQL healthy"),
  ).toBeVisible();
  await page.route("**/api/v1/overview", (route) =>
    route.fulfill({ status: 503, body: "database unavailable" }),
  );
  await page.route("**/healthz", (route) =>
    route.fulfill({ status: 503, body: "database unavailable" }),
  );
  await expect(page.getByRole("alert")).toContainText("data may be stale");
  await expect(page.getByText("Control Plane unavailable")).toBeVisible();
});
test("mobile navigation and tables remain accessible without document overflow", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/workers");
  await expect(
    page.getByRole("heading", { name: "Workers", exact: true }),
  ).toBeVisible();
  await page
    .getByRole("checkbox", { name: "Include historical sessions" })
    .check();
  await expect(page.getByText("OFFLINE", { exact: true })).toBeVisible();
  await page
    .getByRole("button", { name: "Inspect worker-b execution history" })
    .click();
  await expect(
    page.getByRole("heading", { name: "worker-b · recent execution history" }),
  ).toBeVisible();
  await expect(
    page.getByRole("cell", { name: "LEASE_EXPIRED", exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.getByRole("link", { name: "Pipelines", exact: true }).click();
  await expect(
    page.getByRole("link", { name: "Submit pipeline" }),
  ).toBeVisible();
});
