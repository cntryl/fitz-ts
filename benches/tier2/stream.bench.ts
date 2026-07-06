import { describe } from "vitest";
import { StreamCodec } from "../../src/domains/stream/codec";
import { SYNC_CODEC_BATCH_SIZE, benchBatch } from "../_bench";
import { metadata, payloads, routes } from "../_shared";

const payload = payloads.stream;

describe("fitz-ts stream benchmarks", () => {
  benchBatch("stream begin encode", SYNC_CODEC_BATCH_SIZE, () => {
    return StreamCodec.encodeBegin(routes.stream, metadata);
  });

  benchBatch("stream append encode", SYNC_CODEC_BATCH_SIZE, () => {
    return StreamCodec.encodeAppend(1n, 0n, payload, metadata, "test-tag");
  });

  benchBatch("stream commit encode", SYNC_CODEC_BATCH_SIZE, () => {
    return StreamCodec.encodeCommit(1n, "Sync");
  });

  benchBatch("stream read encode", SYNC_CODEC_BATCH_SIZE, () => {
    return StreamCodec.encodeRead(routes.stream, 0n, 100, { maxBytes: 1024n });
  });
});
