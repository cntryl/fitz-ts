import { writeErrorCode } from "../helpers/wire-errors";
import { describe, expect, it } from "vite-plus/test";
import { StreamCodec } from "../../../src/domains/stream/codec";
import { createBufferWriter } from "../../../src/core/buffer";

function errorPayload(code: number, message: string): Uint8Array {
  const writer = createBufferWriter(100);
  writer.writeU8(1);
  writeErrorCode(writer, code);
  writer.writeString(message);
  return writer.getBuffer();
}

describe("versioned Stream error envelope", () => {
  it.each([600, 601, 602, 603, 604, 605, 606, 607, 608])(
    "preserves codes for operation %i",
    (operation) => {
      const decoded = StreamCodec.decodeResponse(
        errorPayload(2050, "unrelated wording"),
        operation,
      );
      expect(decoded).toMatchObject({
        status: 1,
        errorCode: 2050,
        errorMessage: "unrelated wording",
      });
    },
  );

  it("does not classify misleading wording as a conflict", () => {
    expect(
      StreamCodec.decodeResponse(errorPayload(2051, "concurrency conflict"), 601).errorCode,
    ).toBe(2051);
  });

  it("preserves an infrastructure code", () => {
    expect(
      StreamCodec.decodeResponse(errorPayload(2006, "backend unavailable"), 602).errorCode,
    ).toBe(2006);
  });

  it("rejects truncated structured errors", () => {
    expect(() => StreamCodec.decodeResponse(new Uint8Array([2, 0, 0, 7, 209]), 601)).toThrow();
  });

  it("rejects trailing structured error data", () => {
    expect(() =>
      StreamCodec.decodeResponse(new Uint8Array([...errorPayload(2050, "failure"), 0]), 602),
    ).toThrow();
  });
});
