export const encoder = new TextEncoder();

export const routes = {
  kv: "kv://bench/area/resource",
  notice: "notice://bench/area/resource",
  rpc: "rpc://bench/area/resource",
  reply: "rpc://bench/area/reply",
  queue: "queue://bench/area/resource",
  lease: "lease://bench/area/resource",
  schedule: "schedule://bench/area/resource",
  stream: "stream://bench/area/resource",
};

export function buildResponseFrame(index: number): Uint8Array {
  return encoder.encode(`response-${index}`);
}

export function buildCorrelationIds(count: number): Uint8Array[] {
  return Array.from({ length: count }, (_, index) => {
    const correlationId = new Uint8Array(16);
    let value = BigInt(index + 1);
    for (let offset = correlationId.length - 1; offset >= 0; offset -= 1) {
      correlationId[offset] = Number(value & 0xffn);
      value >>= 8n;
    }
    return correlationId;
  });
}

export function buildFrameBatch(frames: Uint8Array[]): Uint8Array {
  const totalLength = frames.reduce((sum, frame) => sum + frame.length, 0);
  const combined = new Uint8Array(totalLength);
  let offset = 0;
  for (const frame of frames) {
    combined.set(frame, offset);
    offset += frame.length;
  }
  return combined;
}

export function chunkBuffer(buffer: Uint8Array, chunkSize: number): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  for (let offset = 0; offset < buffer.length; offset += chunkSize) {
    chunks.push(buffer.subarray(offset, Math.min(buffer.length, offset + chunkSize)));
  }
  return chunks;
}

export function cycleFixture<T>(fixtures: readonly T[], index: number): T {
  return fixtures[index % fixtures.length];
}
