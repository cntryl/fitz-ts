import { describe, expect, it } from "vitest";

import { ProtocolError } from "../../../src/core/errors";
import { assertSuccess, parseStandardResponse } from "../../../src/protocol/response";

describe("response helpers", () => {
  it("returns success payloads", () => {
    const payload = new Uint8Array([0, 7, 8, 9]);

    const result = parseStandardResponse(payload);

    expect(result).toEqual({
      success: true,
      data: new Uint8Array([7, 8, 9]),
    });
  });

  it("throws ProtocolError for empty payloads", () => {
    expect(() => parseStandardResponse(new Uint8Array())).toThrowError(ProtocolError);
  });

  it("throws ProtocolError for unknown status bytes", () => {
    expect(() => parseStandardResponse(new Uint8Array([2]))).toThrowError(ProtocolError);
  });

  it("throws ProtocolError with operation context for error responses", () => {
    const encoder = new TextEncoder();
    const message = encoder.encode("permission denied");
    const payload = new Uint8Array(1 + 4 + message.length);
    payload[0] = 1;
    new DataView(payload.buffer).setUint32(1, message.length, false);
    payload.set(message, 5);

    expect(() => assertSuccess(payload, "KV_GET")).toThrowError(ProtocolError);
    try {
      assertSuccess(payload, "KV_GET");
    } catch (error) {
      expect(error).toBeInstanceOf(ProtocolError);
      expect((error as ProtocolError).getContext()).toMatchObject({
        operation: "KV_GET",
        error: "permission denied",
      });
    }
  });
});
