import { bench, describe } from "vitest";

import { Multiplexer } from "../src/client/multiplexer";
import { FrameCodec, FrameParser } from "../src/frame/codec";
import { NoticeCodec } from "../src/domains/notice/codec";
import { KvCodec } from "../src/domains/kv/codec";
import { LeaseCodec } from "../src/domains/lease/codec";
import { RpcCodec } from "../src/domains/rpc/codec";

const encoder = new TextEncoder();
const route = "kv://bench/area/resource";
const noticeRoute = "notice://bench/area/resource";
const rpcRoute = "rpc://bench/area/resource";
const replyRoute = "rpc://bench/area/reply";
const body = encoder.encode("benchmark-payload");
const key = encoder.encode("bench-key");
const txId = 42n;
const leaseTtlSecs = 30;

function buildResponseFrame(index: number): Uint8Array {
  return encoder.encode(`response-${index}`);
}

describe("fitz-ts hotpath benchmarks", () => {
  bench("frame encode (small payload)", () => {
    FrameCodec.encodeFrame(101, body);
  });

  const encodedFrame = FrameCodec.encodeFrame(101, body);
  bench("frame decode (small payload)", () => {
    FrameCodec.decodeFrame(encodedFrame);
  });

  bench("notice publish encode", () => {
    NoticeCodec.encodePublish(noticeRoute, body);
  });

  bench("kv get encode", () => {
    KvCodec.encodeGet(txId, route, key);
  });

  bench("lease acquire encode", () => {
    LeaseCodec.encodeAcquire(route, leaseTtlSecs);
  });

  bench("rpc call encode", () => {
    RpcCodec.encodeRequest(RpcCodec.generateCorrelationId(), rpcRoute, replyRoute, body);
  });

  bench("rpc correlation id generation", () => {
    RpcCodec.generateCorrelationId();
  });

  bench("multiplexer request/response round-trip", async () => {
    const multiplexer = new Multiplexer();
    multiplexer.setConnected();

    const pending = multiplexer.request(302, body, async () => undefined, 1000);
    multiplexer.dispatch(302, body);
    await pending;
  });

  bench("multiplexer 1k in-flight FIFO drain", async () => {
    const multiplexer = new Multiplexer();
    multiplexer.setConnected();

    const pending = Array.from({ length: 1000 }, (_, index) =>
      multiplexer.request(302, buildResponseFrame(index), async () => undefined, 5000),
    );

    for (let index = 0; index < 1000; index += 1) {
      multiplexer.dispatch(302, buildResponseFrame(index));
    }

    await Promise.all(pending);
  });

  bench("frame parser fragmented stream", () => {
    const frameA = FrameCodec.encodeFrame(302, buildResponseFrame(1));
    const frameB = FrameCodec.encodeFrame(303, buildResponseFrame(2));
    const combined = new Uint8Array(frameA.length + frameB.length);
    combined.set(frameA);
    combined.set(frameB, frameA.length);

    const parser = new FrameParser();

    for (let index = 0; index < combined.length; index += 3) {
      parser.parseFrames(combined.subarray(index, Math.min(combined.length, index + 3)));
    }
  });

  bench("notice publish frame encode throughput", () => {
    const payload = NoticeCodec.encodePublish(noticeRoute, body);
    FrameCodec.encodeFrame(401, payload);
  });
});
