export const encoder = new TextEncoder();

export const routes = {
  kv: "kv://bench/area/resource",
  notice: "notice://bench/area/resource",
  rpc: "rpc://bench/area/resource",
  reply: "rpc://bench/area/reply",
  queue: "queue://bench/area/resource",
  lease: "lease://bench/area/resource",
  schedule: "schedule://bench/area/resource/run",
  stream: "stream://bench/area/resource",
};

export const payloads = {
  hotpath: encoder.encode("benchmark-payload"),
  subsystem: encoder.encode("subsystem-payload"),
  queue: encoder.encode("queue-payload"),
  rpc: encoder.encode("rpc-payload"),
  stream: encoder.encode("stream-payload"),
  schedule: encoder.encode("schedule-payload"),
  system: encoder.encode("system-payload"),
  integration: encoder.encode("integration-payload"),
  payloadA: encoder.encode("payload-a"),
  payloadB: encoder.encode("payload-b"),
};

export const benchKey = encoder.encode("bench-key");
export const metadata = encoder.encode("metadata");
export const streamMetadata = encoder.encode("meta");
export const scheduleCron = "0 0 * * *";
export const scheduleCronAtFive = "0 5 * * *";
export const hotpathScheduleCron = "*/5 * * * *";
export const durability = "Sync";
export const defaultTxId = 1n;
export const hotpathTxId = 42n;

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
