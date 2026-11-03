import { readFileSync } from "node:fs";
import { describe, expect, it } from "vite-plus/test";
import { FrameCodec } from "../../../src/frame/codec";
import {
  decodeExtensions,
  encodeCapabilities,
  encodeExtensions,
} from "../../../src/frame/extensions";

describe("Wire 3", () => {
  const vectors = readFileSync(new URL("../../fixtures/wire3_vectors.txt", import.meta.url), "utf8")
    .trim()
    .split("\n");
  for (const line of vectors) {
    const [name, hex] = line.split(" ");
    it(`decodes shared vector ${name}`, () => {
      const frame = FrameCodec.decodeFrame(Buffer.from(hex!, "hex"));
      const decoded = decodeExtensions(frame.payload, frame.messageType, frame.flags);
      expect(decoded.core.byteLength).toBeGreaterThan(0);
      if (name === "hello_wire3_queue_capabilities") expect(decoded.capabilityIds).toEqual([1, 2]);
    });
  }
  it("preserves core-only payloads", () => {
    const core = new Uint8Array([1, 2]);
    expect(encodeExtensions(core, [])).toBe(core);
    expect(decodeExtensions(core, 104, 0).core).toBe(core);
  });

  it("rejects every truncated prefix of a Wire 3 HELLO envelope", () => {
    const line = vectors.find((value) => value.startsWith("hello_wire3_queue_capabilities "))!;
    const frame = FrameCodec.decodeFrame(Buffer.from(line.split(" ")[1]!, "hex"));
    for (let size = 0; size < frame.payload.length; size++) {
      expect(() => decodeExtensions(frame.payload.subarray(0, size), 2, 4)).toThrow();
    }
  });

  it("skips repeated unknown fields in any order", () => {
    const payload = Buffer.from("000000026869100100000000100000000000100000000000", "hex");
    expect(Buffer.from(decodeExtensions(payload, 104, 4).core).toString()).toBe("hi");
  });
  it.each([
    [],
    [0, 0, 0, 0],
    [0, 0, 0, 9],
    [0, 0, 0, 0, 1],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 9, 255, 255, 255, 255],
  ])("rejects malformed envelope %j", (...bytes) => {
    expect(() => decodeExtensions(new Uint8Array(bytes), 104, 4)).toThrow();
  });
  it("requires HELLO capabilities", () => {
    expect(() => decodeExtensions(new Uint8Array(22), 2, 0)).toThrow();
  });
  it("rejects duplicate HELLO capability fields", () => {
    const payload = new Uint8Array([0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0]);
    expect(() => decodeExtensions(payload, 2, 4)).toThrow();
  });
  it("rejects unordered capabilities", () => {
    expect(() => encodeCapabilities([2, 1])).toThrow();
    expect(() => encodeCapabilities([0])).toThrow();
  });
});
