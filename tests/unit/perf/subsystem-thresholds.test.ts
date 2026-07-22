import { performance } from "node:perf_hooks";

import { describe, expect, it } from "vite-plus/test";

import { QueueCodec } from "../../../src/domains/queue/codec";
import { StreamCodec } from "../../../src/domains/stream/codec";
import { ScheduleCodec } from "../../../src/domains/schedule/codec";
import { RpcCodec } from "../../../src/domains/rpc/codec";

const encoder = new TextEncoder();
const queueRoute = "queue://bench/area/resource";
const scheduleRoute = "schedule://bench/area/resource";
const rpcRoute = "rpc://bench/area/resource";
const body = encoder.encode("subsystem-payload");
const scheduleCron = "0 0 * * *";

const thresholdsMs = {
  queueEnqueueEncode: 200,
  queueReserveEncode: 250,
  streamAppendEncode: 200,
  scheduleCreateEncode: 200,
  rpcEncodeRequest: 400,
  rpcDecodeInboundRequest: 250,
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

describe("fitz-ts subsystem perf thresholds", () => {
  it("keeps queue and domain encode cost within budget", () => {
    expect(
      measureSync(100_000, () => QueueCodec.encodeEnqueue(queueRoute, body, { delayMs: 1500 })),
    ).toBeLessThan(adjustedThreshold(thresholdsMs.queueEnqueueEncode));

    expect(measureSync(100_000, () => QueueCodec.encodeReserve(queueRoute, 60, 10))).toBeLessThan(
      adjustedThreshold(thresholdsMs.queueReserveEncode),
    );

    expect(
      measureSync(100_000, () =>
        StreamCodec.encodeAppend(1n, 0n, body, encoder.encode("meta"), "tag"),
      ),
    ).toBeLessThan(adjustedThreshold(thresholdsMs.streamAppendEncode));

    expect(
      measureSync(100_000, () =>
        ScheduleCodec.encodeCreate(scheduleRoute, scheduleCron, "broadcast", body),
      ),
    ).toBeLessThan(adjustedThreshold(thresholdsMs.scheduleCreateEncode));
  });

  it("keeps rpc codec encode/decode cost within budget", () => {
    const correlationId = RpcCodec.generateCorrelationId();
    const requestFrame = RpcCodec.encodeRequest(correlationId, rpcRoute, body);

    expect(
      measureSync(100_000, () =>
        RpcCodec.encodeRequest(RpcCodec.generateCorrelationId(), rpcRoute, body),
      ),
    ).toBeLessThan(adjustedThreshold(thresholdsMs.rpcEncodeRequest));

    expect(measureSync(100_000, () => RpcCodec.decodeInboundRequest(requestFrame))).toBeLessThan(
      adjustedThreshold(thresholdsMs.rpcDecodeInboundRequest),
    );
  });
});
