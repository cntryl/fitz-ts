import { describe } from "vitest";
import { KvCodec } from "../../src/domains/kv/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { LeaseCodec } from "../../src/domains/lease/codec";
import { SYNC_CODEC_BATCH_SIZE, benchBatch } from "../_bench";

const encoder = new TextEncoder();
const kvRoute = "kv://bench/area/resource";
const noticeRoute = "notice://bench/area/resource";
const queueRoute = "queue://bench/area/resource";
const leaseRoute = "lease://bench/area/resource";
const streamRoute = "stream://bench/area/resource";
const body = encoder.encode("subsystem-payload");
const key = encoder.encode("bench-key");
const txId = 1n;

describe("fitz-ts subsystem benchmarks", () => {
  benchBatch("kv get encode", SYNC_CODEC_BATCH_SIZE, () => {
    return KvCodec.encodeGet(txId, kvRoute, key);
  });

  benchBatch("notice publish encode", SYNC_CODEC_BATCH_SIZE, () => {
    return NoticeCodec.encodePublish(noticeRoute, body);
  });

  benchBatch("queue reserve encode", SYNC_CODEC_BATCH_SIZE, () => {
    return QueueCodec.encodeReserve(queueRoute, 60, 10);
  });

  benchBatch("schedule list encode", SYNC_CODEC_BATCH_SIZE, () => {
    return ScheduleCodec.encodeList(0n, 250n);
  });

  const streamMetadata = encoder.encode("meta");
  benchBatch("stream begin encode", SYNC_CODEC_BATCH_SIZE, () => {
    return StreamCodec.encodeBegin(streamRoute, streamMetadata);
  });

  benchBatch("lease query encode", SYNC_CODEC_BATCH_SIZE, () => {
    return LeaseCodec.encodeQuery(leaseRoute);
  });
});
