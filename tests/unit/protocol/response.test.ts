import { describe, expect, it } from "vite-plus/test";

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
    const payload = new Uint8Array(1 + 4 + 4 + message.length);
    payload[0] = 1;
    new DataView(payload.buffer).setUint32(1, 9876, false);
    new DataView(payload.buffer).setUint32(5, message.length, false);
    payload.set(message, 9);

    expect(() => assertSuccess(payload, "KV_GET")).toThrowError(ProtocolError);
    try {
      assertSuccess(payload, "KV_GET");
    } catch (error) {
      expect(error).toBeInstanceOf(ProtocolError);
      expect((error as ProtocolError).getContext()).toMatchObject({
        operation: "KV_GET",
        error: "permission denied",
        errorCode: 9876,
      });
      expect((error as ProtocolError).domainCode).toBe(9876);
    }
  });

  it("preserves unknown broker error codes and messages", () => {
    const message = new TextEncoder().encode("future broker error");
    const payload = new Uint8Array(9 + message.length);
    payload[0] = 1;
    new DataView(payload.buffer).setUint32(1, 4_000_000_000, false);
    new DataView(payload.buffer).setUint32(5, message.length, false);
    payload.set(message, 9);

    expect(parseStandardResponse(payload)).toEqual({
      success: false,
      data: new Uint8Array(0),
      error: "future broker error",
      errorCode: 4_000_000_000,
    });
  });
});
