import { act, render, screen } from "@testing-library/react";
import { LiveLogs } from "./components";
import { baseAttempt } from "./fixtures";
class FakeSource {
  static CLOSED = 2;
  static sources: FakeSource[] = [];
  readyState = 0;
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  handler: ((event: MessageEvent<string>) => void) | null = null;
  closed = false;
  constructor(public url: string) {
    FakeSource.sources.push(this);
  }
  addEventListener(
    _type: string,
    handler: (event: MessageEvent<string>) => void,
  ) {
    this.handler = handler;
  }
  close() {
    this.closed = true;
  }
}

test("malformed SSE output preserves prior logs and cursor without crashing the console", () => {
  vi.stubGlobal("EventSource", FakeSource);
  try {
    render(<LiveLogs attempt={baseAttempt} />);
    const source = FakeSource.sources.at(-1)!;
    const emit = (data: unknown) =>
      act(() =>
        source.handler?.(
          new MessageEvent("chunk", { data: JSON.stringify(data) }),
        ),
      );
    emit({ sequence: 4, stream: "STDOUT", payload: btoa("retained-output") });
    for (const invalid of [
      null,
      { sequence: 5, stream: "STDERR", payload: "%%%" },
    ]) {
      expect(() => emit(invalid)).not.toThrow();
      expect(screen.getByText("Invalid log data")).toBeInTheDocument();
      expect(screen.getByText("retained-output")).toBeInTheDocument();
      expect(screen.getByText(/Cursor 4/)).toBeInTheDocument();
    }
    emit({ sequence: 5, stream: "STDOUT", payload: btoa("valid-next-chunk") });
    expect(screen.getByText("valid-next-chunk")).toBeInTheDocument();
    expect(screen.getByText(/Cursor 5/)).toBeInTheDocument();
  } finally {
    vi.unstubAllGlobals();
  }
});
test("SSE reconnect retains cursor, deduplicates replay, exposes failure and closes on selection change", () => {
  vi.stubGlobal("EventSource", FakeSource);
  const { rerender, unmount } = render(<LiveLogs attempt={baseAttempt} />);
  const source = FakeSource.sources.at(-1)!;
  expect(source.url).toContain("/logs/stream?after=0");
  act(() => {
    source.onopen?.();
    source.handler?.(
      new MessageEvent("chunk", {
        data: JSON.stringify({
          sequence: 4,
          stream: "STDOUT",
          payload: btoa("output-proof"),
        }),
      }),
    );
    source.onerror?.();
  });
  expect(screen.getByText("Reconnecting")).toBeInTheDocument();
  expect(screen.getByText(/Cursor 4/)).toBeInTheDocument();
  act(() =>
    source.handler?.(
      new MessageEvent("chunk", {
        data: JSON.stringify({
          sequence: 4,
          stream: "STDOUT",
          payload: btoa("output-proof"),
        }),
      }),
    ),
  );
  expect(screen.getAllByText("output-proof")).toHaveLength(1);
  rerender(
    <LiveLogs attempt={{ ...baseAttempt, attempt_id: "new-attempt" }} />,
  );
  expect(source.closed).toBe(true);
  expect(screen.getByText(/Cursor 0/)).toBeInTheDocument();
  unmount();
  expect(FakeSource.sources.at(-1)?.closed).toBe(true);
  vi.unstubAllGlobals();
});
