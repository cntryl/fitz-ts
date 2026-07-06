import { describe } from "vitest";
import { FrameCodec } from "../../src/frame/codec";
import { KvCodec } from "../../src/domains/kv/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { LeaseCodec } from "../../src/domains/lease/codec";
import { COMPOSITE_SYNC_BATCH_SIZE, benchBatch } from "../_bench";
import { payloads, routes, streamMetadata } from "../_shared";

const body = payloads.system;

describe("fitz-ts system benchmarks", () => {
  benchBatch("frame encode + kv begin payload encode", COMPOSITE_SYNC_BATCH_SIZE, () => {
    const payload = KvCodec.encodeBegin(routes.kv, "ReadWrite", "Sync");
    return FrameCodec.encodeFrame(101, payload);
  });

  benchBatch("frame encode + notice + queue enqueue", COMPOSITE_SYNC_BATCH_SIZE, () => {
    const noticePayload = NoticeCodec.encodePublish(routes.notice, body);
    const queuePayload = QueueCodec.encodeEnqueue(routes.queue, body);
    FrameCodec.encodeFrame(401, noticePayload);
    return FrameCodec.encodeFrame(402, queuePayload);
  });

  benchBatch("stream append + lease acquire encode", COMPOSITE_SYNC_BATCH_SIZE, () => {
    StreamCodec.encodeAppend(1n, 0n, body, streamMetadata, "stream-tag");
    return LeaseCodec.encodeAcquire(routes.lease, 15);
  });
});
