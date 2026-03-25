/**
 * RPC Codec unit tests
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import { RpcCodec } from "../../../src/domains/rpc/codec";
import { BufferWriter } from "../../../src/core/buffer";
import { ProtocolError } from "../../../src/core/errors";
import { testData } from "../helpers/test-utils";

describe("RpcCodec", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe("REQUEST encoding", () => {
    it("should_encode_call_with_route_and_payload", () => {
      // Arrange
      const route = "rpc://acme/services/auth";
      const replyRoute = "rpc://reply/temp/123";
      const payload = testData('{"user": "alice"}');
      const correlationId = new Uint8Array(16).fill(0x42);

      // Act
      const encoded = RpcCodec.encodeRequest(
        correlationId,
        route,
        replyRoute,
        payload,
      );

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_call_with_empty_payload", () => {
      // Arrange
      const correlationId = new Uint8Array(16);

      // Act
      const encoded = RpcCodec.encodeRequest(
        correlationId,
        "rpc://test/svc",
        "rpc://reply/temp",
        new Uint8Array(0),
      );

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("REQUEST decoding", () => {
    it("should_decode_call_response_with_payload", () => {
      // Arrange
      const writer = new BufferWriter(256);
      writer.writeU8(0); // status = success
      const response = writer.getBuffer();

      // Act
      const decoded = RpcCodec.decodeRequestResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
    });

    it("should_decode_call_response_error", () => {
      // Arrange
      const response = new Uint8Array([1]); // error status

      // Act
      const decoded = RpcCodec.decodeRequestResponse(response);

      // Assert
      expect(decoded.status).toBe(1);
    });

    it("throws ProtocolError for empty request responses", () => {
      expect(() =>
        RpcCodec.decodeRequestResponse(new Uint8Array()),
      ).toThrowError(ProtocolError);
    });
  });

  describe("SUBSCRIBE_WORKER encoding", () => {
    it("should_encode_subscribe_with_pattern", () => {
      // Arrange
      const pattern = "rpc://acme/services/*";

      // Act
      const encoded = RpcCodec.encodeSubscribeWorker(pattern);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE_WORKER decoding", () => {
    it("should_decode_subscribe_response_with_sub_id", () => {
      // Arrange
      const writer = new BufferWriter(16);
      writer.writeU8(0); // status
      const response = writer.getBuffer();

      // Act
      const decoded = RpcCodec.decodeSubscribeWorkerResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
    });
  });

  describe("Correlation ID handling", () => {
    it("generates 16-byte correlation IDs using cryptographic randomness", () => {
      const getRandomValues = vi.fn((buffer: Uint8Array) => {
        for (let i = 0; i < buffer.length; i += 1) {
          buffer[i] = i + 1;
        }
        return buffer;
      });
      vi.stubGlobal("crypto", { getRandomValues });

      const correlationId = RpcCodec.generateCorrelationId();

      expect(correlationId).toHaveLength(16);
      expect(Array.from(correlationId)).toEqual([
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
      ]);
      expect(getRandomValues).toHaveBeenCalledTimes(1);
    });

    it("throws ProtocolError when cryptographic randomness is unavailable", () => {
      vi.stubGlobal("crypto", undefined);

      expect(() => RpcCodec.generateCorrelationId()).toThrowError(
        ProtocolError,
      );
    });

    it("should_include_correlation_id_in_encoded_call", () => {
      // Arrange
      const correlationId = new Uint8Array(16);
      for (let i = 0; i < 16; i++) {
        correlationId[i] = i;
      }

      // Act
      const encoded = RpcCodec.encodeRequest(
        correlationId,
        "rpc://test/svc",
        "rpc://reply/temp",
        testData("test"),
      );

      // Assert: Correlation ID should be in payload
      expect(encoded).toContain(0); // First byte
      expect(encoded).toContain(15); // Last byte
    });

    it("throws ProtocolError for invalid response correlation lengths", () => {
      const writer = new BufferWriter(64);
      writer.writeU32BE(8);
      writer.writeBytes(new Uint8Array(8));
      writer.writeU64BE(1n);
      writer.writeU32BE(0);

      expect(() => RpcCodec.decodeResponse(writer.getBuffer())).toThrowError(
        ProtocolError,
      );
    });
  });
});
