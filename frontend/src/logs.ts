import type { LogChunk } from "./models";
export interface LogLine {
  sequence: number;
  stream: string;
  text: string;
}
export interface LogBuffer {
  cursor: number;
  lines: LogLine[];
  bytes: number;
  discarded: number;
  carry: Record<string, number[]>;
}
export const emptyBuffer = (): LogBuffer => ({
  cursor: 0,
  lines: [],
  bytes: 0,
  discarded: 0,
  carry: {},
});
export function appendChunk(buffer: LogBuffer, chunk: LogChunk): LogBuffer {
  if (
    !Number.isSafeInteger(chunk.sequence) ||
    chunk.sequence <= buffer.cursor ||
    !["STDOUT", "STDERR"].includes(chunk.stream)
  )
    return buffer;
  const bytes = Uint8Array.from([
    ...(buffer.carry[chunk.stream] ?? []),
    ...Array.from(atob(chunk.payload), (c) => c.charCodeAt(0)),
  ]);
  let end = bytes.length;
  let lead = bytes.length - 1;
  while (
    lead >= 0 &&
    lead >= bytes.length - 4 &&
    (bytes[lead]! & 0xc0) === 0x80
  )
    lead--;
  if (lead >= 0) {
    const first = bytes[lead]!;
    const expected =
      first >= 0xf0 && first <= 0xf4
        ? 4
        : first >= 0xe0 && first <= 0xef
          ? 3
          : first >= 0xc2 && first <= 0xdf
            ? 2
            : 1;
    if (bytes.length - lead < expected) end = lead;
  }
  const text = new TextDecoder().decode(bytes.slice(0, end));
  const carry = {
    ...buffer.carry,
    [chunk.stream]: Array.from(bytes.slice(end)),
  };
  const lines = [
    ...buffer.lines,
    { sequence: chunk.sequence, stream: chunk.stream, text },
  ];
  let size = buffer.bytes + text.length;
  let discarded = buffer.discarded;
  while (lines.length > 200 || size > 131072) {
    const removed = lines.shift();
    if (!removed) break;
    size -= removed.text.length;
    discarded++;
  }
  return { cursor: chunk.sequence, lines, bytes: size, discarded, carry };
}
