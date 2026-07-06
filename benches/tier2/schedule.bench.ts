import { describe } from "vitest";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { SYNC_CODEC_BATCH_SIZE, benchBatch } from "../_bench";
import { encoder, routes } from "../_shared";

const payload = encoder.encode("schedule-payload");
const cronExpr = "0 5 * * *";

describe("fitz-ts schedule benchmarks", () => {
  benchBatch("schedule create encode", SYNC_CODEC_BATCH_SIZE, () => {
    return ScheduleCodec.encodeCreate(routes.schedule, cronExpr, payload);
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
