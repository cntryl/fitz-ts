import { describe } from "vitest";
import { FrameCodec } from "../../src/frame/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { KvCodec } from "../../src/domains/kv/codec";
import type { DurabilityMode } from "../../src/domains/kv/types";
import { QueueCodec } from "../../src/domains/queue/codec";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { COMPOSITE_SYNC_BATCH_SIZE, benchBatch } from "../_bench";

const encoder = new TextEncoder();
const route = "kv://bench/area/resource";
const noticeRoute = "notice://bench/area/resource";
const queueRoute = "queue://bench/area/resource";
const scheduleRoute = "schedule://bench/area/resource";
const streamRoute = "stream://bench/area/resource";
const body = encoder.encode("integration-payload");
const durability: DurabilityMode = "Sync";

const scheduleCron = "0 0 * * *";
const streamMetadata = encoder.encode("meta");

describe("fitz-ts integration benchmarks", () => {
  benchBatch("encode frame + notice + kv begin", COMPOSITE_SYNC_BATCH_SIZE, () => {
    const noticePayload = NoticeCodec.encodePublish(noticeRoute, body);
    FrameCodec.encodeFrame(401, noticePayload);
    return KvCodec.encodeBegin(route, "ReadWrite", durability);
  });

  benchBatch(
    "batch encode: queue enqueue + schedule create + stream begin",
    COMPOSITE_SYNC_BATCH_SIZE,
    () => {
      const queuePayload = QueueCodec.encodeEnqueue(queueRoute, body);
      const schedulePayload = ScheduleCodec.encodeCreate(scheduleRoute, scheduleCron, body);
      const streamPayload = StreamCodec.encodeBegin(streamRoute, streamMetadata);
      FrameCodec.encodeFrame(502, queuePayload);
      FrameCodec.encodeFrame(503, schedulePayload);
      return FrameCodec.encodeFrame(504, streamPayload);
    },
  );
});
