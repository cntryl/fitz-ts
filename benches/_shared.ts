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
