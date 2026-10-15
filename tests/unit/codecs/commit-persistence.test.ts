import { describe, expect, it } from "vite-plus/test";
import { KvCodec } from "../../../src/domains/kv/codec";
import { StreamCodec } from "../../../src/domains/stream/codec";
import { createBufferReader } from "../../../src/core/buffer";

describe("commit-time persistence", () => {
  it("encodes KV BEGIN with scope and mode only", () => {
    const payload = KvCodec.encodeBegin("kv://r/a/x", "ReadWrite");
    const reader = createBufferReader(payload);
    expect(reader.readString()).toBe("kv://r/a/x");
    expect(reader.readU8()).toBe(1);
    expect(reader.isEOF()).toBe(true);
  });
  for (const [mode, wire] of [
    ["Buffered", 0],
    ["Sync", 1],
  ] as const) {
    it(`encodes explicit ${mode} on KV COMMIT`, () => {
      const reader = createBufferReader(KvCodec.encodeCommit(7n, "kv://r/a/x", mode));
      expect(reader.readU64BE()).toBe(7n);
      expect(reader.readString()).toBe("kv://r/a/x");
      expect(reader.readU8()).toBe(wire);
      expect(reader.isEOF()).toBe(true);
    });
    it(`encodes explicit ${mode} on Stream COMMIT`, () => {
      const reader = createBufferReader(StreamCodec.encodeCommit(7n, mode));
      expect(reader.readU64BE()).toBe(7n);
      expect(reader.readU8()).toBe(wire);
      expect(reader.isEOF()).toBe(true);
    });
  }
  it("keeps ROLLBACK free of persistence", () => {
    const reader = createBufferReader(KvCodec.encodeRollback(7n, "kv://r/a/x"));
    expect(reader.readU64BE()).toBe(7n);
    expect(reader.readString()).toBe("kv://r/a/x");
    expect(reader.isEOF()).toBe(true);
  });
});
