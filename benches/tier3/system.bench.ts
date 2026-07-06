import { describe } from "vitest";
import { FrameCodec } from "../../src/frame/codec";
import { KvCodec } from "../../src/domains/kv/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { LeaseCodec } from "../../src/domains/lease/codec";
import { COMPOSITE_SYNC_BATCH_SIZE, benchBatch } from "../_bench";

const encoder = new TextEncoder();
const route = "kv://bench/area/resource";
const noticeRoute = "notice://bench/area/resource";
const queueRoute = "queue://bench/area/resource";
const body = encoder.encode("system-payload");
const streamMetadata = encoder.encode("meta");

describe("fitz-ts system benchmarks", () => {
  benchBatch("frame encode + kv begin payload encode", COMPOSITE_SYNC_BATCH_SIZE, () => {
    const payload = KvCodec.encodeBegin(route, "ReadWrite", "Sync");
    return FrameCodec.encodeFrame(101, payload);
  });

  benchBatch("frame encode + notice + queue enqueue", COMPOSITE_SYNC_BATCH_SIZE, () => {
    const noticePayload = NoticeCodec.encodePublish(noticeRoute, body);
    const queuePayload = QueueCodec.encodeEnqueue(queueRoute, body);
    FrameCodec.encodeFrame(401, noticePayload);
    return FrameCodec.encodeFrame(402, queuePayload);
  });

  benchBatch("stream append + lease acquire encode", COMPOSITE_SYNC_BATCH_SIZE, () => {
    StreamCodec.encodeAppend(1n, 0n, body, streamMetadata, "stream-tag");
    return LeaseCodec.encodeAcquire(route, 15);
  });
});
