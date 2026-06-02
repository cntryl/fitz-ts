import { bench, describe } from "vitest";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { encoder, routes } from "../_shared";

const payload = encoder.encode("schedule-payload");
const cronExpr = "0 5 * * *";

describe("fitz-ts schedule benchmarks", () => {
  bench("schedule create encode", () => {
    ScheduleCodec.encodeCreate(routes.schedule, cronExpr, payload);
  });

  bench("schedule list encode", () => {
    ScheduleCodec.encodeList(0n, 250n);
  });

  bench("schedule subscribe encode", () => {
    ScheduleCodec.encodeSubscribe("schedule://bench/**");
  });

  bench("schedule cancel encode", () => {
    ScheduleCodec.encodeCancel(routes.schedule);
  });
});
