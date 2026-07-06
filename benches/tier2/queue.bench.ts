import { describe } from "vitest";
import { QueueCodec } from "../../src/domains/queue/codec";
import { SYNC_CODEC_BATCH_SIZE, benchBatch } from "../_bench";
import { encoder, routes } from "../_shared";

const body = encoder.encode("queue-payload");

describe("fitz-ts queue benchmarks", () => {
  benchBatch("queue enqueue encode", SYNC_CODEC_BATCH_SIZE, () => {
    return QueueCodec.encodeEnqueue(routes.queue, body, { delayMs: 1500 });
  });

  benchBatch("queue reserve encode", SYNC_CODEC_BATCH_SIZE, () => {
    return QueueCodec.encodeReserve(routes.queue, 60, 10);
  });

  benchBatch("queue complete encode", SYNC_CODEC_BATCH_SIZE, () => {
    return QueueCodec.encodeComplete(routes.queue, 123n, 456n);
  });

  benchBatch("queue extend encode", SYNC_CODEC_BATCH_SIZE, () => {
    return QueueCodec.encodeExtend(routes.queue, 123n, 456n, 30);
  });

  benchBatch("queue subscribe encode", SYNC_CODEC_BATCH_SIZE, () => {
    return QueueCodec.encodeSubscribe("queue://bench/**");
  });
});
