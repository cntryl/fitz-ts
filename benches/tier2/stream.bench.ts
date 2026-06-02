import { bench, describe } from "vitest";
import { StreamCodec } from "../../src/domains/stream/codec";
import { encoder, routes } from "../_shared";

const payload = encoder.encode("stream-payload");
const metadata = encoder.encode("metadata");

describe("fitz-ts stream benchmarks", () => {
  bench("stream begin encode", () => {
    StreamCodec.encodeBegin(routes.stream, metadata);
  });

  bench("stream append encode", () => {
    StreamCodec.encodeAppend(1n, 0n, payload, metadata, "test-tag");
  });

  bench("stream commit encode", () => {
    StreamCodec.encodeCommit(1n, "Sync");
  });

  bench("stream read encode", () => {
    StreamCodec.encodeRead(routes.stream, 0n, 100, { maxBytes: 1024n });
  });
});
