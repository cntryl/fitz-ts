import { bench, describe } from "vitest";
import { FrameCodec } from "../../src/frame/codec";
import { KvCodec } from "../../src/domains/kv/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { LeaseCodec } from "../../src/domains/lease/codec";

const encoder = new TextEncoder();
const route = "kv://bench/area/resource";
const noticeRoute = "notice://bench/area/resource";
const queueRoute = "queue://bench/area/resource";
const body = encoder.encode("system-payload");

describe("fitz-ts system benchmarks", () => {
  bench("frame encode + kv begin payload encode", () => {
    const payload = KvCodec.encodeBegin(route, "ReadWrite", "Sync");
    FrameCodec.encodeFrame(101, payload);
  });

  bench("frame encode + notice + queue enqueue", () => {
    const noticePayload = NoticeCodec.encodePublish(noticeRoute, body);
    const queuePayload = QueueCodec.encodeEnqueue(queueRoute, body);
    FrameCodec.encodeFrame(401, noticePayload);
    FrameCodec.encodeFrame(402, queuePayload);
  });

  bench("stream append + lease acquire encode", () => {
    StreamCodec.encodeAppend(1n, 0n, body, encoder.encode("meta"), "stream-tag");
    LeaseCodec.encodeAcquire(route, 15);
  });
});
