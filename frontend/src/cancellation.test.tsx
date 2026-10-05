import { fireEvent, render, screen } from "@testing-library/react";
import { vi } from "vitest";
import { Cancel } from "./components";

afterEach(() => vi.unstubAllGlobals());

test("cancellation confirms the existing API and waits for backend state", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValue(
      new Response(JSON.stringify({ state: "CANCELLING" }), { status: 202 }),
    );
  vi.stubGlobal("fetch", fetch);
  const { rerender } = render(
    <Cancel
      path="/api/v1/jobs/job-id/cancel"
      label="Cancel job"
      disabled={false}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "Cancel job" }));
  expect(fetch).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Confirm Cancel job" }));
  expect(await screen.findByRole("status")).toHaveTextContent(
    "Awaiting authoritative state",
  );
  expect(fetch).toHaveBeenCalledWith(
    "/api/v1/jobs/job-id/cancel",
    expect.objectContaining({ method: "POST", body: "{}" }),
  );
  expect(screen.queryByText("CANCELLED")).not.toBeInTheDocument();
  rerender(
    <Cancel
      path="/api/v1/jobs/job-id/cancel"
      label="Cancel job"
      disabled={true}
    />,
  );
  expect(screen.getByRole("button", { name: "Cancel job" })).toBeDisabled();
});

test("failed cancellation exposes the API error without declaring success", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValue(new Response("database unavailable", { status: 503 })),
  );
  render(
    <Cancel
      path="/api/v1/pipelines/pipeline-id/cancel"
      label="Cancel pipeline"
      disabled={false}
    />,
  );
  fireEvent.click(screen.getByRole("button", { name: "Cancel pipeline" }));
  fireEvent.click(
    screen.getByRole("button", { name: "Confirm Cancel pipeline" }),
  );
  expect(await screen.findByRole("status")).toHaveTextContent(
    "503: database unavailable",
  );
  expect(screen.queryByText(/Cancellation requested/)).not.toBeInTheDocument();
});
