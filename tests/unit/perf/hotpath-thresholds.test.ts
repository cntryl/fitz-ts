import { performance } from "node:perf_hooks";

import { describe, expect, it } from "vite-plus/test";

import { Multiplexer } from "../../../src/client/multiplexer";
import { FrameCodec, FrameParser } from "../../../src/frame/codec";
import { KvCodec } from "../../../src/domains/kv/codec";
import { LeaseCodec } from "../../../src/domains/lease/codec";
import { NoticeCodec } from "../../../src/domains/notice/codec";
import { RpcCodec } from "../../../src/domains/rpc/codec";

const encoder = new TextEncoder();

const route = "kv://bench/area/resource";
const noticeRoute = "notice://bench/area/resource";
const rpcRoute = "rpc://bench/area/resource";
const replyRoute = "rpc://bench/area/reply";
const body = encoder.encode("benchmark-payload");
const key = encoder.encode("bench-key");
const txId = 42n;
const leaseTtlSecs = 30;

const thresholdsMs = {
  frameEncode: 50,
  frameDecode: 50,
  noticePublishEncode: 150,
  kvGetEncode: 150,
  leaseAcquireEncode: 150,
  rpcCallEncode: 500,
  rpcCorrelationIdGeneration: 300,
  multiplexerRoundTrip: 200,
  multiplexerFifoDrain: 25,
  frameParserFragmentedStream: 150,
  noticePublishFrameEncodeThroughput: 150,
} as const;

const isWindows = process.platform === "win32";
const isCi = process.env.CI === "true" || process.env.GITHUB_ACTIONS === "true";
const perfDescribe = isWindows ? describe.skip : describe;

function adjustedThreshold(value: number): number {
  // CI runners tend to be slower than local developer machines, so relax the
  // perf budgets to avoid flaky threshold failures while preserving local checks.
  if (isWindows) return value * 2.5;
  return isCi ? value * 2 : value;
}

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

async function measureAsync(
  iterations: number,
  callback: () => void | Promise<void>,
): Promise<number> {
  for (let index = 0; index < Math.min(100, iterations); index += 1) {
    await callback();
  }

  const startedAt = performance.now();
  for (let index = 0; index < iterations; index += 1) {
    await callback();
  }
  return performance.now() - startedAt;
}

perfDescribe("fitz-ts hot-path thresholds", () => {
  it("keeps the basic codec hot paths within budget", () => {
    const encodedFrame = FrameCodec.encodeFrame(101, body);

    expect(measureSync(100_000, () => FrameCodec.encodeFrame(101, body))).toBeLessThan(
      thresholdsMs.frameEncode,
    );
    expect(measureSync(100_000, () => FrameCodec.decodeFrame(encodedFrame))).toBeLessThan(
      thresholdsMs.frameDecode,
    );
    expect(measureSync(100_000, () => NoticeCodec.encodePublish(noticeRoute, body))).toBeLessThan(
      adjustedThreshold(thresholdsMs.noticePublishEncode),
    );
    expect(measureSync(100_000, () => KvCodec.encodeGet(txId, route, key))).toBeLessThan(
      adjustedThreshold(thresholdsMs.kvGetEncode),
    );
    expect(measureSync(100_000, () => LeaseCodec.encodeAcquire(route, leaseTtlSecs))).toBeLessThan(
      adjustedThreshold(thresholdsMs.leaseAcquireEncode),
    );
    expect(
      measureSync(100_000, () =>
        RpcCodec.encodeRequest(RpcCodec.generateCorrelationId(), rpcRoute, replyRoute, body),
      ),
    ).toBeLessThan(adjustedThreshold(thresholdsMs.rpcCallEncode));
    expect(measureSync(100_000, () => RpcCodec.generateCorrelationId())).toBeLessThan(
      adjustedThreshold(thresholdsMs.rpcCorrelationIdGeneration),
    );
  });

  it("keeps the multiplexer hot paths within budget", async () => {
    const multiplexer = new Multiplexer();
    multiplexer.setConnected();

    const roundTripElapsed = await measureAsync(10_000, async () => {
      const pending = multiplexer.request(302, body, async () => undefined, 1_000);
      multiplexer.dispatch(302, body);
      await pending;
    });

    expect(roundTripElapsed).toBeLessThan(thresholdsMs.multiplexerRoundTrip);

    const drainElapsed = await measureAsync(1, async () => {
      const drainingMux = new Multiplexer();
      drainingMux.setConnected();

      const pending = Array.from({ length: 1_000 }, (_, index) =>
        drainingMux.request(302, encoder.encode(`response-${index}`), async () => undefined, 5_000),
      );

      for (let index = 0; index < 1_000; index += 1) {
        drainingMux.dispatch(302, encoder.encode(`response-${index}`));
      }

      await Promise.all(pending);
    });

    expect(drainElapsed).toBeLessThan(thresholdsMs.multiplexerFifoDrain);
  });

  it("keeps the frame parser and publish throughput within budget", () => {
    const frameA = FrameCodec.encodeFrame(302, encoder.encode("response-1"));
    const frameB = FrameCodec.encodeFrame(303, encoder.encode("response-2"));
    const combined = new Uint8Array(frameA.length + frameB.length);
    combined.set(frameA);
    combined.set(frameB, frameA.length);

    const parserElapsed = measureSync(10_000, () => {
      const parser = new FrameParser();
      for (let index = 0; index < combined.length; index += 3) {
        parser.parseFrames(combined.subarray(index, Math.min(combined.length, index + 3)));
      }
    });
    expect(parserElapsed).toBeLessThan(thresholdsMs.frameParserFragmentedStream);

    const publishElapsed = measureSync(100_000, () => {
      FrameCodec.encodeFrame(401, NoticeCodec.encodePublish(noticeRoute, body));
    });
    expect(publishElapsed).toBeLessThan(
      adjustedThreshold(thresholdsMs.noticePublishFrameEncodeThroughput),
    );
  });
});
