import { describe } from "vitest";

import { Multiplexer } from "../../src/client/multiplexer";
import { FrameCodec, FrameParser } from "../../src/frame/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { KvCodec } from "../../src/domains/kv/codec";
import { LeaseCodec } from "../../src/domains/lease/codec";
import { RpcCodec } from "../../src/domains/rpc/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import {
  ASYNC_ROUND_TRIP_BATCH_SIZE,
  FIFO_DRAIN_BATCH_SIZE,
  SYNC_CODEC_BATCH_SIZE,
  benchAsync,
  benchBatch,
  benchMacro,
  consume,
} from "../_bench";
import {
  buildCorrelationIds,
  buildFrameBatch,
  buildResponseFrame,
  chunkBuffer,
  cycleFixture,
} from "../_shared";

const encoder = new TextEncoder();
const route = "kv://bench/area/resource";
const noticeRoute = "notice://bench/area/resource";
const rpcRoute = "rpc://bench/area/resource";
const replyRoute = "rpc://bench/area/reply";
const queueRoute = "queue://bench/area/resource";
const scheduleRoute = "schedule://bench/area/resource";
const body = encoder.encode("benchmark-payload");
const key = encoder.encode("bench-key");
const txId = 42n;
const leaseTtlSecs = 30;
const scheduleCron = "*/5 * * * *";
const rpcCorrelationIds = buildCorrelationIds(SYNC_CODEC_BATCH_SIZE);
const responseFrames = Array.from({ length: 1_000 }, (_, index) => buildResponseFrame(index));
const encodedFrame = FrameCodec.encodeFrame(101, body);
const parserFrameA = FrameCodec.encodeFrame(302, buildResponseFrame(1));
const parserFrameB = FrameCodec.encodeFrame(303, buildResponseFrame(2));
const parserChunks = chunkBuffer(buildFrameBatch([parserFrameA, parserFrameB]), 3);

describe("fitz-ts hotpath benchmarks", () => {
  benchBatch("frame encode (small payload)", SYNC_CODEC_BATCH_SIZE, () => {
    return FrameCodec.encodeFrame(101, body);
  });

  benchBatch("frame decode (small payload)", SYNC_CODEC_BATCH_SIZE, () => {
    return FrameCodec.decodeFrame(encodedFrame);
  });

  benchBatch("notice publish encode", SYNC_CODEC_BATCH_SIZE, () => {
    return NoticeCodec.encodePublish(noticeRoute, body);
  });

  benchBatch("kv get encode", SYNC_CODEC_BATCH_SIZE, () => {
    return KvCodec.encodeGet(txId, route, key);
  });

  benchBatch("lease acquire encode", SYNC_CODEC_BATCH_SIZE, () => {
    return LeaseCodec.encodeAcquire(route, leaseTtlSecs);
  });

  benchBatch("queue enqueue encode", SYNC_CODEC_BATCH_SIZE, () => {
    return QueueCodec.encodeEnqueue(queueRoute, body, { delayMs: 1500 });
  });

  benchBatch("schedule create encode", SYNC_CODEC_BATCH_SIZE, () => {
    return ScheduleCodec.encodeCreate(scheduleRoute, scheduleCron, body);
  });

  benchBatch("stream append encode", SYNC_CODEC_BATCH_SIZE, () => {
    return StreamCodec.encodeAppend(1n, 0n, body, undefined, "tag");
  });

  benchBatch("rpc call encode", SYNC_CODEC_BATCH_SIZE, (index) => {
    return RpcCodec.encodeRequest(
      cycleFixture(rpcCorrelationIds, index),
      rpcRoute,
      replyRoute,
      body,
    );
  });

  benchBatch("rpc correlation id generation", SYNC_CODEC_BATCH_SIZE, () => {
    return RpcCodec.generateCorrelationId();
  });

  benchAsync("multiplexer request/response round-trip", async () => {
    const multiplexer = new Multiplexer();
    multiplexer.setConnected();

    for (let index = 0; index < ASYNC_ROUND_TRIP_BATCH_SIZE; index += 1) {
      const frame = cycleFixture(responseFrames, index);
      const pending = multiplexer.request(302, frame, async () => undefined, 1000);
      multiplexer.dispatch(302, frame);
      consume(await pending);
    }
  });

  benchMacro("multiplexer 1k in-flight FIFO drain", async () => {
    let drained: unknown;
    for (let batch = 0; batch < FIFO_DRAIN_BATCH_SIZE; batch += 1) {
      const multiplexer = new Multiplexer();
      multiplexer.setConnected();

      const pending = Array.from({ length: 1000 }, (_, index) =>
        multiplexer.request(302, responseFrames[index], async () => undefined, 5000),
      );

      for (let index = 0; index < 1000; index += 1) {
        multiplexer.dispatch(302, responseFrames[index]);
      }

      drained = await Promise.all(pending);
    }

    consume(drained);
  });

  benchBatch("frame parser fragmented stream", SYNC_CODEC_BATCH_SIZE, () => {
    const parser = new FrameParser();
    let parsed: unknown;

    for (const chunk of parserChunks) {
      parsed = parser.parseFrames(chunk);
    }

    return parsed;
  });

  benchBatch("notice publish frame encode throughput", SYNC_CODEC_BATCH_SIZE, () => {
    const payload = NoticeCodec.encodePublish(noticeRoute, body);
    return FrameCodec.encodeFrame(401, payload);
  });
});
