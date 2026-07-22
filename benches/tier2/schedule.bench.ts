import { describe } from "vitest";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { SYNC_CODEC_BATCH_SIZE, benchBatch } from "../_bench";
import { payloads, routes, scheduleCronAtFive } from "../_shared";

const payload = payloads.schedule;

describe("fitz-ts schedule benchmarks", () => {
  benchBatch("schedule create encode", SYNC_CODEC_BATCH_SIZE, () => {
    return ScheduleCodec.encodeCreate(routes.schedule, scheduleCronAtFive, "broadcast", payload);
  });

  benchBatch("schedule list encode", SYNC_CODEC_BATCH_SIZE, () => {
    return ScheduleCodec.encodeList(0n, 250n);
  });

  benchBatch("schedule subscribe encode", SYNC_CODEC_BATCH_SIZE, () => {
    return ScheduleCodec.encodeSubscribe("schedule://bench/**");
  });

  benchBatch("schedule cancel encode", SYNC_CODEC_BATCH_SIZE, () => {
    return ScheduleCodec.encodeCancel(routes.schedule);
  });
});
