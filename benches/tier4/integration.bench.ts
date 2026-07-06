import { describe } from "vitest";
import { FrameCodec } from "../../src/frame/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { KvCodec } from "../../src/domains/kv/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { COMPOSITE_SYNC_BATCH_SIZE, benchBatch } from "../_bench";
import { durability, payloads, routes, scheduleCron, streamMetadata } from "../_shared";

const body = payloads.integration;

describe("fitz-ts integration benchmarks", () => {
  benchBatch("encode frame + notice + kv begin", COMPOSITE_SYNC_BATCH_SIZE, () => {
    const noticePayload = NoticeCodec.encodePublish(routes.notice, body);
    FrameCodec.encodeFrame(401, noticePayload);
    return KvCodec.encodeBegin(routes.kv, "ReadWrite", durability);
  });

  benchBatch(
    "batch encode: queue enqueue + schedule create + stream begin",
    COMPOSITE_SYNC_BATCH_SIZE,
    () => {
      const queuePayload = QueueCodec.encodeEnqueue(routes.queue, body);
      const schedulePayload = ScheduleCodec.encodeCreate(routes.schedule, scheduleCron, body);
      const streamPayload = StreamCodec.encodeBegin(routes.stream, streamMetadata);
      FrameCodec.encodeFrame(502, queuePayload);
      FrameCodec.encodeFrame(503, schedulePayload);
      return FrameCodec.encodeFrame(504, streamPayload);
    },
  );
});
