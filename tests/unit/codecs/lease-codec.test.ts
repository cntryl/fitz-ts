/**
 * Lease Codec unit tests
 */

import { describe, it, expect } from "vite-plus/test";
import { LeaseCodec } from "../../../src/domains/lease/codec";
import { createBufferWriter } from "../../../src/core/buffer";
import { LeaseError, ProtocolError } from "../../../src/core/errors";
import { testData as _testData } from "../helpers/test-utils";

describe("LeaseCodec", () => {
  describe("ACQUIRE encoding", () => {
    it("should_encode_acquire_with_route_and_ttl", () => {
      // Arrange
      const route = "lease://acme/resources/db_connection";
      const ttlSecs = 60;

      // Act
      const encoded = LeaseCodec.encodeAcquire(route, ttlSecs);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_acquire_with_different_ttl_values", () => {
      const testCases = [1, 60, 3600, 86400];
      for (const ttl of testCases) {
        const encoded = LeaseCodec.encodeAcquire("lease://test/resource", ttl);
        expect(encoded).toBeInstanceOf(Uint8Array);
      }
    });
  });

  describe("ACQUIRE decoding", () => {
    it("should preserve domain code given typed error when decoding acquire", () => {
      const writer = createBufferWriter(64);
      writer.writeU8(1);
      writer.writeU32BE(5001);
      writer.writeString("lease held by another owner");

      try {
        LeaseCodec.decodeAcquireResponse(writer.getBuffer());
        throw new Error("expected ACQUIRE decoding to fail");
      } catch (error) {
        expect(error).toBeInstanceOf(LeaseError);
        expect((error as LeaseError).domainCode).toBe(5001);
        expect((error as LeaseError).message).toContain("lease held by another owner");
      }
    });

    it("should_decode_acquire_response_with_token", () => {
      // Arrange
      const writer = createBufferWriter(32);
      writer.writeU8(0); // status = success
      writer.writeU8(0); // response_type = Acquired
      writer.writeU64BE(888n); // token
      const response = writer.getBuffer();

      // Act
      const decoded = LeaseCodec.decodeAcquireResponse(response);

      // Assert
      expect(decoded.token).toBe(888n);
      expect(decoded.expiresAt).toBeUndefined(); // Computed by client
    });

    it("throws ProtocolError for short acquire responses", () => {
      expect(() => LeaseCodec.decodeAcquireResponse(new Uint8Array([0, 0]))).toThrowError(
        ProtocolError,
      );
    });

    it.each([2, 3] as const)(
      "decodes queued response type %i for deferred completion",
      (responseType) => {
        const writer = createBufferWriter(16);
        writer.writeU8(0);
        writer.writeU8(responseType);
        writer.writeU64BE(0n);

        expect(LeaseCodec.decodeAcquireResponse(writer.getBuffer())).toEqual({
          responseType,
          token: 0n,
        });
      },
    );
  });

  describe("RENEW encoding", () => {
    it("should_encode_renew_with_token_and_new_ttl", () => {
      // Arrange
      const route = "lease://test/resource";
      const token = 888n;
      const newTtl = 120;

      // Act
      const encoded = LeaseCodec.encodeRenew(route, token, newTtl);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("RELEASE encoding", () => {
    it("should_encode_release_with_route_and_token", () => {
      // Arrange
      const route = "lease://test/resource";
      const token = 888n;

      // Act
      const encoded = LeaseCodec.encodeRelease(route, token);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE encoding", () => {
    it("should_encode_subscribe_with_pattern", () => {
      // Arrange
      const route = "lease://acme/resources/database";

      // Act
      const encoded = LeaseCodec.encodeSubscribe(route);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("QUERY decoding", () => {
    it("should_decode_query_response_with_ttl_remaining_secs", () => {
      // Arrange
      const writer = createBufferWriter(64);
      writer.writeU8(0); // status = success
      writer.writeU8(1); // has_holder = yes
      writer.writeRoute("lease://owner/test");
      writer.writeU64BE(42n); // ttl_remaining_secs
      writer.writeU32BE(0); // pending_waiters
      const response = writer.getBuffer();

      // Act
      const decoded = LeaseCodec.decodeQueryResponse(response);

      // Assert
      expect(decoded.isHeld).toBe(true);
      expect(decoded.owner).toBe("lease://owner/test");
      expect(decoded.ttlRemainingSecs).toBe(42n);
      expect(decoded.expiresAt).toBeDefined();
    });
  });

  describe("NOTIFY decoding", () => {
    it("should_decode_notification_payload", () => {
      // Arrange
      const writer = createBufferWriter(32);
      writer.writeU64BE(222n); // subId
      writer.writeString("lease://acme/resources/db_connection");
      writer.writeU32BE(0);
      const payload = writer.getBuffer();

      // Act
      const decoded = LeaseCodec.decodeNotification(payload);

      // Assert
      expect(decoded.subId).toBe(222n);
      expect(decoded.route).toBe("lease://acme/resources/db_connection");
    });
  });

  describe("LIST encoding", () => {
    it("encodes a cursor-less request with the default limit", () => {
      const encoded = LeaseCodec.encodeList("lease://acme/renderers/*");
      const reader = createBufferReaderCompat(encoded);
      expect(reader.readString()).toBe("lease://acme/renderers/*");
      expect(reader.readU8()).toBe(0); // has_cursor = false
      expect(reader.readU32BE()).toBe(0); // limit = 0 (server default)
      expect(reader.isEOF()).toBe(true);
    });

    it("encodes a request carrying a cursor and an explicit limit", () => {
      const encoded = LeaseCodec.encodeList("lease://acme/**", {
        cursor: { snapshotId: 777n, offset: 200 },
        limit: 50,
      });
      const reader = createBufferReaderCompat(encoded);
      expect(reader.readString()).toBe("lease://acme/**");
      expect(reader.readU8()).toBe(1); // has_cursor = true
      expect(reader.readU64BE()).toBe(777n);
      expect(reader.readU32BE()).toBe(200);
      expect(reader.readU32BE()).toBe(50); // limit
      expect(reader.isEOF()).toBe(true);
    });

    it("rejects a negative limit instead of bit-coercing it onto the wire", () => {
      expect(() => LeaseCodec.encodeList("lease://acme/**", { limit: -1 })).toThrow(LeaseError);
    });

    it("rejects a fractional limit", () => {
      expect(() => LeaseCodec.encodeList("lease://acme/**", { limit: 1.5 })).toThrow(LeaseError);
    });

    it("rejects a non-finite limit", () => {
      expect(() => LeaseCodec.encodeList("lease://acme/**", { limit: Infinity })).toThrow(
        LeaseError,
      );
      expect(() => LeaseCodec.encodeList("lease://acme/**", { limit: Number.NaN })).toThrow(
        LeaseError,
      );
    });

    it("rejects a limit above the u32 range", () => {
      expect(() => LeaseCodec.encodeList("lease://acme/**", { limit: 0x1_0000_0000 })).toThrow(
        LeaseError,
      );
    });

    it("rejects an invalid cursor offset", () => {
      expect(() =>
        LeaseCodec.encodeList("lease://acme/**", {
          cursor: { snapshotId: 1n, offset: -1 },
        }),
      ).toThrow(LeaseError);
      expect(() =>
        LeaseCodec.encodeList("lease://acme/**", {
          cursor: { snapshotId: 1n, offset: 1.5 },
        }),
      ).toThrow(LeaseError);
      expect(() =>
        LeaseCodec.encodeList("lease://acme/**", {
          cursor: { snapshotId: 1n, offset: 0x1_0000_0000 },
        }),
      ).toThrow(LeaseError);
    });

    it("accepts the full valid u32 range at the boundary", () => {
      expect(() => LeaseCodec.encodeList("lease://acme/**", { limit: 0xffff_ffff })).not.toThrow();
      expect(() =>
        LeaseCodec.encodeList("lease://acme/**", {
          cursor: { snapshotId: 1n, offset: 0xffff_ffff },
        }),
      ).not.toThrow();
    });
  });

  describe("LIST decoding", () => {
    it("decodes a page with items and no further cursor", () => {
      const writer = createBufferWriter(128);
      writer.writeU8(0); // status = success
      writer.writeU32BE(1); // item_count
      writer.writeString("lease://acme/renderers/one"); // route
      writer.writeString("worker-42"); // owner_id
      writer.writeU64BE(9001n); // holder_incarnation
      writer.writeString("2026-08-29T00:00:00Z"); // acquired_at
      writer.writeU64BE(120n); // expires_in_secs
      writer.writeU32BE(3); // renewals
      writer.writeU8(0); // has_next = false

      const decoded = LeaseCodec.decodeListResponse(writer.getBuffer());
      expect(decoded.items).toHaveLength(1);
      expect(decoded.items[0]).toEqual({
        route: "lease://acme/renderers/one",
        ownerId: "worker-42",
        holderIncarnation: 9001n,
        acquiredAt: "2026-08-29T00:00:00Z",
        expiresInSecs: 120n,
        renewals: 3,
      });
      expect(decoded.nextCursor).toBeUndefined();
    });

    it("decodes an empty page carrying a continuation cursor", () => {
      const writer = createBufferWriter(32);
      writer.writeU8(0);
      writer.writeU32BE(0); // item_count
      writer.writeU8(1); // has_next = true
      writer.writeU64BE(555n); // snapshot_id
      writer.writeU32BE(100); // offset

      const decoded = LeaseCodec.decodeListResponse(writer.getBuffer());
      expect(decoded.items).toHaveLength(0);
      expect(decoded.nextCursor).toEqual({ snapshotId: 555n, offset: 100 });
    });

    it("throws a LeaseError carrying the domain code for ERR_INVALID_LIST_CURSOR", () => {
      const writer = createBufferWriter(64);
      writer.writeU8(1);
      writer.writeU32BE(5011);
      writer.writeString("unknown cursor snapshot");

      try {
        LeaseCodec.decodeListResponse(writer.getBuffer());
        throw new Error("expected LIST decoding to fail");
      } catch (error) {
        expect(error).toBeInstanceOf(LeaseError);
        expect((error as LeaseError).domainCode).toBe(5011);
      }
    });

    it("throws a LeaseError carrying the domain code for ERR_INVALID_LIST_PATTERN", () => {
      const writer = createBufferWriter(64);
      writer.writeU8(1);
      writer.writeU32BE(5012);
      writer.writeString("malformed list pattern");

      try {
        LeaseCodec.decodeListResponse(writer.getBuffer());
        throw new Error("expected LIST decoding to fail");
      } catch (error) {
        expect(error).toBeInstanceOf(LeaseError);
        expect((error as LeaseError).domainCode).toBe(5012);
      }
    });
  });
});

// Minimal local reader used only to assert exact wire byte layout in these
// tests without depending on LeaseCodec's own (equally-under-test) reader.
function createBufferReaderCompat(buffer: Uint8Array) {
  let offset = 0;
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  return {
    readU8(): number {
      return buffer[offset++]!;
    },
    readU32BE(): number {
      const value = view.getUint32(offset);
      offset += 4;
      return value;
    },
    readU64BE(): bigint {
      const value = view.getBigUint64(offset);
      offset += 8;
      return value;
    },
    readString(): string {
      const len = this.readU32BE();
      const bytes = buffer.subarray(offset, offset + len);
      offset += len;
      return new TextDecoder().decode(bytes);
    },
    isEOF(): boolean {
      return offset >= buffer.length;
    },
  };
}
