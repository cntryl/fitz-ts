import { bench, describe } from "vitest";
import { FrameCodec, FrameParser } from "../../src/frame/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { KvCodec } from "../../src/domains/kv/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { encoder, routes, buildFrameBatch } from "../_shared";

const body = encoder.encode("integration-payload");
const scheduleCron = "0 0 * * *";

describe("fitz-ts integration benchmarks", () => {
  bench("end-to-end encode + parse batch", () => {
    const frames = [
      FrameCodec.encodeFrame(401, NoticeCodec.encodePublish(routes.notice, body)),
      FrameCodec.encodeFrame(102, KvCodec.encodeBegin(routes.kv, "ReadWrite", "Sync")),
      FrameCodec.encodeFrame(502, QueueCodec.encodeEnqueue(routes.queue, body)),
      FrameCodec.encodeFrame(503, ScheduleCodec.encodeCreate(routes.schedule, scheduleCron, body)),
      FrameCodec.encodeFrame(504, StreamCodec.encodeBegin(routes.stream, encoder.encode("meta"))),
    ];

    const combined = buildFrameBatch(frames);
    const parser = new FrameParser();
    parser.parseFrames(combined);
  });

  bench("frame round-trip with mixed domain payloads", () => {
    const bodyA = encoder.encode("payload-a");
    const bodyB = encoder.encode("payload-b");
    const frameA = FrameCodec.encodeFrame(401, NoticeCodec.encodePublish(routes.notice, bodyA));
    const frameB = FrameCodec.encodeFrame(502, QueueCodec.encodeEnqueue(routes.queue, bodyB));

    const parser = new FrameParser();
    parser.parseFrames(buildFrameBatch([frameA, frameB]));
  });
});
