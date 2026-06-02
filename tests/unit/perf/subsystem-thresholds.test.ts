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
const replyRoute = "rpc://bench/area/reply";
const body = encoder.encode("subsystem-payload");
const scheduleCron = "0 0 * * *";

const thresholdsMs = {
  queueEnqueueEncode: 200,
  queueReserveEncode: 150,
  streamAppendEncode: 200,
  scheduleCreateEncode: 200,
  rpcEncodeRequest: 400,
  rpcDecodeInboundRequest: 250,
} as const;

function measureSync(iterations: number, callback: () => void): number {
  for (let index = 0; index < Math.min(1_000, iterations); index += 1) {
    callback();
  }

  const startedAt = performance.now();
  for (let index = 0; index < iterations; index += 1) {
    callback();
  }
  return performance.now() - startedAt;
}

describe("fitz-ts subsystem perf thresholds", () => {
  it("keeps queue and domain encode cost within budget", () => {
    expect(
      measureSync(100_000, () => QueueCodec.encodeEnqueue(queueRoute, body, { delayMs: 1500 })),
    ).toBeLessThan(thresholdsMs.queueEnqueueEncode);

    expect(measureSync(100_000, () => QueueCodec.encodeReserve(queueRoute, 60, 10))).toBeLessThan(
      thresholdsMs.queueReserveEncode,
    );

    expect(
      measureSync(100_000, () =>
        StreamCodec.encodeAppend(1n, 0n, body, encoder.encode("meta"), "tag"),
      ),
    ).toBeLessThan(thresholdsMs.streamAppendEncode);

    expect(
      measureSync(100_000, () => ScheduleCodec.encodeCreate(scheduleRoute, scheduleCron, body)),
    ).toBeLessThan(thresholdsMs.scheduleCreateEncode);
  });

  it("keeps rpc codec encode/decode cost within budget", () => {
    const correlationId = RpcCodec.generateCorrelationId();
    const requestFrame = RpcCodec.encodeRequest(correlationId, rpcRoute, replyRoute, body);

    expect(
      measureSync(100_000, () =>
        RpcCodec.encodeRequest(RpcCodec.generateCorrelationId(), rpcRoute, replyRoute, body),
      ),
    ).toBeLessThan(thresholdsMs.rpcEncodeRequest);

    expect(measureSync(100_000, () => RpcCodec.decodeInboundRequest(requestFrame))).toBeLessThan(
      thresholdsMs.rpcDecodeInboundRequest,
    );
  });
});
