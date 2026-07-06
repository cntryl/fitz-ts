/**
 * RPC Codec unit tests
 */

import { describe, it, expect, vi, afterEach } from "vite-plus/test";
import { RpcCodec } from "../../../src/domains/rpc/codec";
import { createBufferReader, createBufferWriter } from "../../../src/core/buffer";
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
      const payload = testData('{"user": "alice"}');
      const correlationId = new Uint8Array(16).fill(0x42);

      // Act
      const encoded = RpcCodec.encodeRequest(correlationId, route, payload);

      // Assert
      const reader = createBufferReader(encoded);
      expect(reader.readBytes(16)).toEqual(correlationId);
      expect(reader.readString()).toBe(route);
      expect(reader.readU32BE()).toBe(payload.length);
      expect(reader.readBytes(payload.length)).toEqual(payload);
      expect(reader.isEOF()).toBe(true);
    });

    it("should_encode_call_with_empty_payload", () => {
      // Arrange
      const correlationId = new Uint8Array(16);

      // Act
      const encoded = RpcCodec.encodeRequest(correlationId, "rpc://test/svc", new Uint8Array(0));

      // Assert
      const decoded = RpcCodec.decodeInboundRequest(encoded);
      expect(decoded.correlationId).toEqual(correlationId);
      expect(decoded.route).toBe("rpc://test/svc");
      expect(decoded.body).toEqual(new Uint8Array(0));
    });
  });

  describe("REQUEST decoding", () => {
    it("should_decode_call_response_with_payload", () => {
      // Arrange
      const writer = createBufferWriter(256);
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
      expect(() => RpcCodec.decodeRequestResponse(new Uint8Array())).toThrowError(ProtocolError);
    });

    it("throws ProtocolError for inbound requests with trailing bytes", () => {
      const request = RpcCodec.encodeRequest(
        new Uint8Array(16),
        "rpc://test/svc",
        testData("test"),
      );
      const malformed = new Uint8Array(request.length + 1);
      malformed.set(request);
      malformed[malformed.length - 1] = 0xff;

      expect(() => RpcCodec.decodeInboundRequest(malformed)).toThrowError(ProtocolError);
    });
  });

  describe("SUBSCRIBE_WORKER encoding", () => {
    it("should_encode_subscribe_with_pattern", () => {
      // Arrange
      const pattern = "rpc://acme/services/*";

      // Act
      const encoded = RpcCodec.encodeSubscribeWorker(pattern, 32);

      // Assert
      const reader = createBufferReader(encoded);
      expect(reader.readString()).toBe(pattern);
      expect(reader.readU32BE()).toBe(32);
      expect(reader.isEOF()).toBe(true);
    });
  });

  describe("SUBSCRIBE_WORKER decoding", () => {
    it("should_decode_subscribe_response_with_sub_id", () => {
      // Arrange
      const writer = createBufferWriter(16);
      writer.writeU8(0); // status
      const response = writer.getBuffer();

      // Act
      const decoded = RpcCodec.decodeSubscribeWorkerResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
    });
  });

  describe("error body decoding", () => {
    it("should_decode_rpc_error_body", () => {
      const writer = createBufferWriter(64);
      writer.writeU8(1);
      writer.writeU32BE(6002);
      writer.writeString("Worker disconnected or unregistered");

      expect(RpcCodec.decodeErrorBody(writer.getBuffer())).toEqual({
        code: 6002,
        message: "Worker disconnected or unregistered",
      });
      expect(RpcCodec.tryDecodeTerminalErrorBody(writer.getBuffer())).toEqual({
        code: 6002,
        message: "Worker disconnected or unregistered",
      });
    });

    it("does not classify malformed or unknown terminal bodies as rpc errors", () => {
      const unknownCode = createBufferWriter(64);
      unknownCode.writeU8(1);
      unknownCode.writeU32BE(7000);
      unknownCode.writeString("application payload");

      const malformed = new Uint8Array([1, 0, 0, 0, 0x17, 0xff]);

      expect(RpcCodec.tryDecodeTerminalErrorBody(unknownCode.getBuffer())).toBeNull();
      expect(RpcCodec.tryDecodeTerminalErrorBody(malformed)).toBeNull();
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

      expect(() => RpcCodec.generateCorrelationId()).toThrowError(ProtocolError);
    });

    it("should_include_correlation_id_in_encoded_call", () => {
      // Arrange
      const correlationId = new Uint8Array(16);
      for (let i = 0; i < 16; i++) {
        correlationId[i] = i;
      }

      // Act
      const encoded = RpcCodec.encodeRequest(correlationId, "rpc://test/svc", testData("test"));

      expect(Array.from(encoded.subarray(0, 16))).toEqual(Array.from(correlationId));
    });

    it("throws ProtocolError for invalid response payload length", () => {
      expect(() => RpcCodec.decodeResponse(new Uint8Array(16))).toThrowError(ProtocolError);
    });

    it("encodes and decodes response flags before the body", () => {
      const correlationId = new Uint8Array(16).fill(0x7a);
      const body = new Uint8Array([1, 2, 3]);
      const encoded = RpcCodec.encodeResponse(correlationId, 5n, body, true);

      expect(encoded[16 + 8]).toBe(0x01);
      expect(RpcCodec.decodeResponse(encoded)).toEqual({
        correlationId,
        sequence: 5n,
        body,
        streamEnd: true,
      });
    });
  });
});
