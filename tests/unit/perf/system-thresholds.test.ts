import { performance } from "node:perf_hooks";

import { describe, expect, it } from "vite-plus/test";

import { FrameCodec, createFrameParser } from "../../../src/frame/codec";
import { KvCodec } from "../../../src/domains/kv/codec";
import { NoticeCodec } from "../../../src/domains/notice/codec";
import { QueueCodec } from "../../../src/domains/queue/codec";
import { StreamCodec } from "../../../src/domains/stream/codec";
import { encoder, buildFrameBatch, routes } from "../../../benches/_shared";

const body = encoder.encode("system-threshold-payload");
const key = encoder.encode("bench-key");

const thresholdsMs = {
  frameBatchEncodeParse: 250,
  kvBeginFrameEncode: 150,
  mixedPayloadBatch: 320,
} as const;

const isWindows = process.platform === "win32";
const isCi = process.env.CI === "true" || process.env.GITHUB_ACTIONS === "true";

function adjustedThreshold(value: number): number {
  if (isWindows) return value * 2.5;
  return isCi ? value * 2 : value;
}

function measureSync(iterations: number, callback: () => void): number {
  for (let index = 0; index < Math.min(1_000, iterations); index += 1) {
    callback();
  }

  const samples: number[] = [];
  for (let sample = 0; sample < 3; sample += 1) {
    const startedAt = performance.now();
    for (let index = 0; index < iterations; index += 1) {
      callback();
    }
    samples.push(performance.now() - startedAt);
  }

  return Math.min(...samples);
}

describe("fitz-ts system perf thresholds", () => {
  it("keeps mixed protocol encode and parse costs within budget", () => {
    const frames = [
      FrameCodec.encodeFrame(401, NoticeCodec.encodePublish(routes.notice, body)),
      FrameCodec.encodeFrame(402, QueueCodec.encodeEnqueue(routes.queue, body)),
      FrameCodec.encodeFrame(403, StreamCodec.encodeBegin(routes.stream, encoder.encode("meta"))),
    ];
    const combined = buildFrameBatch(frames);

    expect(
      measureSync(10_000, () => {
        const parser = createFrameParser();
        parser.parseFrames(combined);
      }),
    ).toBeLessThan(adjustedThreshold(thresholdsMs.frameBatchEncodeParse));
  });

  it("keeps kv begin + frame encode within budget", () => {
    expect(
      measureSync(100_000, () => {
        const payload = KvCodec.encodeBegin(routes.kv, "ReadWrite", "Sync");
        FrameCodec.encodeFrame(101, payload);
      }),
    ).toBeLessThan(adjustedThreshold(thresholdsMs.kvBeginFrameEncode));
  });

  it("keeps mixed encode batch cost within budget", () => {
    expect(
      measureSync(50_000, () => {
        const noticePayload = NoticeCodec.encodePublish(routes.notice, body);
        const kvPayload = KvCodec.encodeGet(42n, routes.kv, key);
        FrameCodec.encodeFrame(401, noticePayload);
        FrameCodec.encodeFrame(102, kvPayload);
      }),
    ).toBeLessThan(adjustedThreshold(thresholdsMs.mixedPayloadBatch));
  });
});
