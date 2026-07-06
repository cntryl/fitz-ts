import { describe } from "vitest";
import { FrameCodec, FrameParser } from "../../src/frame/codec";
import { KvCodec } from "../../src/domains/kv/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { COMPOSITE_SYNC_BATCH_SIZE, benchBatch } from "../_bench";
import { encoder, routes, buildFrameBatch } from "../_shared";

const body = encoder.encode("system-payload");
const metadata = encoder.encode("meta");
const key = encoder.encode("bench-key");

describe("fitz-ts protocol benchmarks", () => {
  benchBatch("frame batch encode + parse", COMPOSITE_SYNC_BATCH_SIZE, () => {
    const noticePayload = NoticeCodec.encodePublish(routes.notice, body);
    const queuePayload = QueueCodec.encodeEnqueue(routes.queue, body);
    const streamPayload = StreamCodec.encodeBegin(routes.stream, metadata);
    const frames = [
      FrameCodec.encodeFrame(401, noticePayload),
      FrameCodec.encodeFrame(402, queuePayload),
      FrameCodec.encodeFrame(403, streamPayload),
    ];
    const parser = new FrameParser();
    return parser.parseFrames(buildFrameBatch(frames));
  });

  benchBatch("kv begin + frame encode path", COMPOSITE_SYNC_BATCH_SIZE, () => {
    const payload = KvCodec.encodeBegin(routes.kv, "ReadWrite", "Sync");
    return FrameCodec.encodeFrame(101, payload);
  });

  benchBatch("mixed payload encode batch", COMPOSITE_SYNC_BATCH_SIZE, () => {
    const noticePayload = NoticeCodec.encodePublish(routes.notice, body);
    const kvPayload = KvCodec.encodeGet(42n, routes.kv, key);
    FrameCodec.encodeFrame(401, noticePayload);
    return FrameCodec.encodeFrame(102, kvPayload);
  });
});
