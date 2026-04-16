/**
 * Stream Codec unit tests
 */

import { describe, it, expect } from "vite-plus/test";
import { StreamCodec } from "../../../src/domains/stream/codec";
import { BufferWriter } from "../../../src/core/buffer";
import { testData } from "../helpers/test-utils";

function writeOptionalU64(writer: BufferWriter, value: bigint | undefined): void {
  if (value === undefined) {
    writer.writeU8(0);
    return;
  }

  writer.writeU8(1);
  writer.writeU64BE(value);
}

function writeOptionalBytes(writer: BufferWriter, value: Uint8Array | undefined): void {
  if (!value) {
    writer.writeU8(0);
    return;
  }

  writer.writeU8(1);
  writer.writeU32BE(value.length);
  writer.writeBytes(value);
}

function encodeStreamRecord(options: {
  offset: bigint;
  areaOffset?: bigint;
  realmOffset?: bigint;
  body: Uint8Array;
  metadata?: Uint8Array;
  timestamp: bigint;
}): Uint8Array {
  const writer = new BufferWriter(256);
  writer.writeU64BE(options.offset);
  writeOptionalU64(writer, options.areaOffset);
  writeOptionalU64(writer, options.realmOffset);
  writer.writeU32BE(options.body.length);
  writer.writeBytes(options.body);
  writeOptionalBytes(writer, options.metadata);
  writer.writeU64BE(options.timestamp);
  return writer.getBuffer();
}

function encodeReadResponse(
  records: Uint8Array[],
  cursor: {
    lastResourceOffset: bigint;
    lastAreaOffset?: bigint;
    lastRealmOffset?: bigint;
    hasMore: boolean;
  },
): Uint8Array {
  const data = new BufferWriter(512);
  data.writeU32BE(records.length);
  for (const record of records) {
    data.writeBytes(record);
  }
  data.writeU64BE(cursor.lastResourceOffset);
  writeOptionalU64(data, cursor.lastAreaOffset);
  writeOptionalU64(data, cursor.lastRealmOffset);
  data.writeU8(cursor.hasMore ? 1 : 0);

  const writer = new BufferWriter(560);
  writer.writeU8(0);
  writer.writeU8(0);
  writer.writeU32BE(data.getLength());
  writer.writeBytes(data.getBuffer());
  return writer.getBuffer();
}

function encodeLastResponse(record: Uint8Array): Uint8Array {
  const writer = new BufferWriter(320);
  writer.writeU8(0);
  writer.writeU8(0);
  writer.writeU32BE(record.length);
  writer.writeBytes(record);
  return writer.getBuffer();
}

function encodeMetadataResponse(metadata: {
  firstResourceOffset?: bigint;
  lastResourceOffset?: bigint;
  resourceCount: bigint;
  maxBatchEvents: bigint;
  maxBatchBytes: bigint;
  ttlSeconds?: bigint;
  areaWatermark: bigint;
  realmWatermark: bigint;
}): Uint8Array {
  const data = new BufferWriter(256);
  writeOptionalU64(data, metadata.firstResourceOffset);
  writeOptionalU64(data, metadata.lastResourceOffset);
  data.writeU64BE(metadata.resourceCount);
  data.writeU64BE(metadata.maxBatchEvents);
  data.writeU64BE(metadata.maxBatchBytes);
  writeOptionalU64(data, metadata.ttlSeconds);
  data.writeU64BE(metadata.areaWatermark);
  data.writeU64BE(metadata.realmWatermark);

  const writer = new BufferWriter(320);
  writer.writeU8(0);
  writer.writeU8(0);
  writer.writeU32BE(data.getLength());
  writer.writeBytes(data.getBuffer());
  return writer.getBuffer();
}

describe("StreamCodec", () => {
  describe("BEGIN encoding", () => {
    it("should_encode_begin_with_route", () => {
      // Arrange
      const route = "stream://acme/events/orders";

      // Act
      const encoded = StreamCodec.encodeBegin(route);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_begin_with_empty_metadata", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeBegin("stream://test/data");

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("BEGIN decoding", () => {
    it("should_decode_begin_response_with_session_id", () => {
      // Arrange
      const writer = new BufferWriter(24);
      writer.writeU8(0); // status = success
      writer.writeU8(1); // has_session_id = 1
      writer.writeU64BE(456n); // sessionId
      writer.writeU32BE(0); // empty data
      const response = writer.getBuffer();

      // Act
      const decoded = StreamCodec.decodeBeginResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.sessionId).toBe(456n);
    });

    it("should_decode_begin_response_error", () => {
      // Arrange
      const response = new Uint8Array([1]); // error status

      // Act
      const decoded = StreamCodec.decodeBeginResponse(response);

      // Assert
      expect(decoded.status).toBe(1);
    });
  });

  describe("APPEND encoding", () => {
    it("should_encode_append_with_records", () => {
      // Arrange
      const sessionId = 456n;
      const expectedOffset = 100n;
      const body = testData("record1");

      // Act
      const encoded = StreamCodec.encodeAppend(sessionId, expectedOffset, body);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_append_with_empty_records", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeAppend(456n, 0n, new Uint8Array(0));

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("COMMIT encoding", () => {
    it("should_encode_commit_with_explicit_mode", () => {
      const encoded = StreamCodec.encodeCommit(456n, "Sync");

      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded[8]).toBe(1);
    });
  });

  describe("READ encoding", () => {
    it("should_encode_read_with_offset_and_limit", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeRead("stream://test/events", 0n, 100);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it("should_encode_read_from_middle_with_custom_limit", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeRead("stream://test/events", 50n, 25);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("READ decoding", () => {
    it("should_decode_read_response_with_records", () => {
      // Arrange
      const response = encodeReadResponse(
        [
          encodeStreamRecord({
            offset: 100n,
            areaOffset: 200n,
            realmOffset: 300n,
            body: testData("record1"),
            metadata: testData("meta1"),
            timestamp: 111n,
          }),
          encodeStreamRecord({
            offset: 101n,
            areaOffset: 201n,
            realmOffset: 301n,
            body: testData("record2"),
            metadata: testData("meta2"),
            timestamp: 222n,
          }),
        ],
        {
          lastResourceOffset: 101n,
          lastAreaOffset: 201n,
          lastRealmOffset: 301n,
          hasMore: false,
        },
      );

      // Act
      const decoded = StreamCodec.decodeReadResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.records).toHaveLength(2);
      expect(decoded.records[0].offset).toBe(100n);
      expect(decoded.records[1].offset).toBe(101n);
      expect(decoded.records[0].timestamp).toBe(111n);
      expect(Buffer.from(decoded.records[0].body).toString()).toBe("record1");
    });

    it("should_decode_read_response_empty", () => {
      // Arrange
      const response = encodeReadResponse([], {
        lastResourceOffset: 0n,
        hasMore: false,
      });

      // Act
      const decoded = StreamCodec.decodeReadResponse(response);

      // Assert
      expect(decoded.records).toHaveLength(0);
    });
  });

  describe("LAST decoding", () => {
    it("should_decode_last_response_with_record", () => {
      // Arrange
      const response = encodeLastResponse(
        encodeStreamRecord({
          offset: 500n,
          areaOffset: 501n,
          realmOffset: 502n,
          body: testData("last-record"),
          metadata: undefined,
          timestamp: 999n,
        }),
      );

      // Act
      const decoded = StreamCodec.decodeLastResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.record?.offset).toBe(500n);
      expect(decoded.record?.timestamp).toBe(999n);
      expect(Buffer.from(decoded.record?.body ?? new Uint8Array()).toString()).toBe("last-record");
    });
  });

  describe("METADATA decoding", () => {
    it("should_decode_metadata_response_with_counts", () => {
      // Arrange
      const response = encodeMetadataResponse({
        firstResourceOffset: 10n,
        lastResourceOffset: 20n,
        resourceCount: 3n,
        maxBatchEvents: 1000n,
        maxBatchBytes: 4096n,
        ttlSeconds: 60n,
        areaWatermark: 30n,
        realmWatermark: 40n,
      });

      // Act
      const decoded = StreamCodec.decodeMetadataResponse(response);

      // Assert
      expect(decoded.status).toBe(0);
      expect(decoded.metadata?.firstOffset).toBe(10n);
      expect(decoded.metadata?.lastOffset).toBe(20n);
      expect(decoded.metadata?.recordCount).toBe(3n);
    });
  });

  describe("LAST encoding", () => {
    it("should_encode_last", () => {
      // Arrange/Act
      const encoded = StreamCodec.encodeLast("stream://test/events");

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });
});
