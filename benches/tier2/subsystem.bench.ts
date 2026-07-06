import { describe } from "vitest";
import { KvCodec } from "../../src/domains/kv/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { LeaseCodec } from "../../src/domains/lease/codec";
import { SYNC_CODEC_BATCH_SIZE, benchBatch } from "../_bench";
import { benchKey, defaultTxId, payloads, routes, streamMetadata } from "../_shared";

const body = payloads.subsystem;

describe("fitz-ts subsystem benchmarks", () => {
  benchBatch("kv get encode", SYNC_CODEC_BATCH_SIZE, () => {
    return KvCodec.encodeGet(defaultTxId, routes.kv, benchKey);
  });

  benchBatch("notice publish encode", SYNC_CODEC_BATCH_SIZE, () => {
    return NoticeCodec.encodePublish(routes.notice, body);
  });

  benchBatch("queue reserve encode", SYNC_CODEC_BATCH_SIZE, () => {
    return QueueCodec.encodeReserve(routes.queue, 60, 10);
  });

  benchBatch("schedule list encode", SYNC_CODEC_BATCH_SIZE, () => {
    return ScheduleCodec.encodeList(0n, 250n);
  });

  benchBatch("stream begin encode", SYNC_CODEC_BATCH_SIZE, () => {
    return StreamCodec.encodeBegin(routes.stream, streamMetadata);
  });

  benchBatch("lease query encode", SYNC_CODEC_BATCH_SIZE, () => {
    return LeaseCodec.encodeQuery(routes.lease);
  });
});
