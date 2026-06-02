import { bench, describe } from "vitest";
import { KvCodec } from "../../src/domains/kv/codec";
import { NoticeCodec } from "../../src/domains/notice/codec";
import { QueueCodec } from "../../src/domains/queue/codec";
import { ScheduleCodec } from "../../src/domains/schedule/codec";
import { StreamCodec } from "../../src/domains/stream/codec";
import { LeaseCodec } from "../../src/domains/lease/codec";

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
  bench("kv get encode", () => {
    KvCodec.encodeGet(txId, kvRoute, key);
  });

  bench("notice publish encode", () => {
    NoticeCodec.encodePublish(noticeRoute, body);
  });

  bench("queue reserve encode", () => {
    QueueCodec.encodeReserve(queueRoute, 60, 10);
  });

  bench("schedule list encode", () => {
    ScheduleCodec.encodeList(0n, 250n);
  });

  bench("stream begin encode", () => {
    StreamCodec.encodeBegin(streamRoute, encoder.encode("meta"));
  });

  bench("lease query encode", () => {
    LeaseCodec.encodeQuery(leaseRoute);
  });
});
