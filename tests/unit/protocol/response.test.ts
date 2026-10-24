import { errorPayload } from "../helpers/wire-errors";
import { describe, expect, it } from "vite-plus/test";

import { ProtocolError } from "../../../src/core/errors";
import {
  assertPlainSuccess,
  assertSuccess,
  parsePlainResponse,
  parseStandardResponse,
} from "../../../src/protocol/response";

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
    const payload = errorPayload(9876, "permission denied");

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
    const payload = errorPayload(65000, "future broker error");

    expect(parseStandardResponse(payload)).toEqual({
      success: false,
      data: new Uint8Array(0),
      error: "future broker error",
      errorCode: 65000,
      errorClass: 0,
      effect: 0,
    });
  });

  it("rejects legacy string-only domain errors", () => {
    const message = new TextEncoder().encode("lease wait timed out");
    const payload = new Uint8Array(5 + message.length);
    payload[0] = 1;
    new DataView(payload.buffer).setUint32(1, message.length, false);
    payload.set(message, 5);

    expect(() => parsePlainResponse(payload)).toThrow();
    expect(() => assertPlainSuccess(payload, "LEASE_ACQUIRE")).toThrow();
  });
});
