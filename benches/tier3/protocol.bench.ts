import { bench, describe } from "vitest";
import { FrameCodec, FrameParser } from "../../src/frame/codec";
import { KvCodec } from "../../src/domains/kv/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { encoder, routes, buildFrameBatch } from "../_shared";

const body = encoder.encode("system-payload");

describe("fitz-ts protocol benchmarks", () => {
  bench("frame batch encode + parse", () => {
    const noticePayload = NoticeCodec.encodePublish(routes.notice, body);
    const queuePayload = QueueCodec.encodeEnqueue(routes.queue, body);
    const streamPayload = StreamCodec.encodeBegin(routes.stream, encoder.encode("meta"));
    const frames = [
      FrameCodec.encodeFrame(401, noticePayload),
      FrameCodec.encodeFrame(402, queuePayload),
      FrameCodec.encodeFrame(403, streamPayload),
    ];
    const parser = new FrameParser();
    parser.parseFrames(buildFrameBatch(frames));
  });

  bench("kv begin + frame encode path", () => {
    const payload = KvCodec.encodeBegin(routes.kv, "ReadWrite", "Sync");
    FrameCodec.encodeFrame(101, payload);
  });

  bench("mixed payload encode batch", () => {
    const noticePayload = NoticeCodec.encodePublish(routes.notice, body);
    const kvPayload = KvCodec.encodeGet(42n, routes.kv, encoder.encode("bench-key"));
    FrameCodec.encodeFrame(401, noticePayload);
    FrameCodec.encodeFrame(102, kvPayload);
  });
});
