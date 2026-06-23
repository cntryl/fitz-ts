/// <reference types="node" />

/**
 * Stream Codec unit tests
 */

import { describe, it, expect } from "vite-plus/test";
import { StreamCodec } from "../../../src/domains/stream/codec";
import { BufferReader, BufferWriter } from "../../../src/core/buffer";
import type {
  StreamFilterClause,
  StreamFilterSet,
  StreamReadItem,
} from "../../../src/domains/stream/types";
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
  items: StreamReadItem[],
  cursor: {
    lastResourceOffset: bigint;
    lastAreaOffset?: bigint;
    lastRealmOffset?: bigint;
    hasMore: boolean;
  },
): Uint8Array {
  const data = new BufferWriter(512);
  data.writeU32BE(items.length);
  for (const item of items) {
    switch (item.kind) {
      case "event":
        data.writeU8(0);
        data.writeBytes(encodeStreamRecord(item.record));
        break;
      case "filtered":
        data.writeU8(1);
        data.writeU64BE(item.offset);
        writeFilteredReason(data, item.reason);
        break;
      case "filtered_range":
        data.writeU8(2);
        data.writeU64BE(item.fromOffset);
        data.writeU64BE(item.toOffset);
        writeFilteredReason(data, item.reason);
        break;
    }
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

function writeFilteredReason(
  writer: BufferWriter,
  reason?: "server_filter" | "permission" | "projection",
): void {
  switch (reason) {
    case undefined:
      writer.writeU8(0);
      return;
    case "server_filter":
      writer.writeU8(1);
      return;
    case "permission":
      writer.writeU8(2);
      return;
    case "projection":
      writer.writeU8(3);
      return;
  }
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

function readFilterU8(bytes: Uint8Array, offset: number): [number, number] {
  if (offset + 1 > bytes.length) {
    throw new Error("buffer underflow");
  }

  return [bytes[offset], offset + 1];
}

function readFilterU32BE(bytes: Uint8Array, offset: number): [number, number] {
  if (offset + 4 > bytes.length) {
    throw new Error("buffer underflow");
  }

  const value =
    (bytes[offset] << 24) |
    (bytes[offset + 1] << 16) |
    (bytes[offset + 2] << 8) |
    bytes[offset + 3];
  return [value >>> 0, offset + 4];
}

function readFilterString(bytes: Uint8Array, offset: number): [string, number] {
  const [length, afterLength] = readFilterU32BE(bytes, offset);
  const end = afterLength + length;
  if (end > bytes.length) {
    throw new Error("buffer underflow");
  }

  return [Buffer.from(bytes.slice(afterLength, end)).toString("utf8"), end];
}

function decodeStreamFilterSet(bytes: Uint8Array): StreamFilterSet {
  const [version0, afterVersion0] = readFilterU8(bytes, 0);
  const [version1, afterVersion1] = readFilterU8(bytes, afterVersion0);
  if (version0 !== 0 || version1 !== 0xf1) {
    throw new Error("unsupported filter version");
  }

  const [count, start] = readFilterU32BE(bytes, afterVersion1);
  let offset = start;
  const clauses: StreamFilterClause[] = [];

  for (let index = 0; index < count; index += 1) {
    const [variant, afterVariant] = readFilterU8(bytes, offset);
    offset = afterVariant;

    switch (variant) {
      case 0: {
        const [value, afterValue] = readFilterString(bytes, offset);
        offset = afterValue;
        clauses.push({ kind: "Equals", value });
        break;
      }
      case 1: {
        const [value, afterValue] = readFilterString(bytes, offset);
        offset = afterValue;
        clauses.push({ kind: "NotEquals", value });
        break;
      }
      case 2: {
        const [value, afterValue] = readFilterString(bytes, offset);
        offset = afterValue;
        clauses.push({ kind: "StartsWith", value });
        break;
      }
      case 3: {
        const [valueCount, afterCount] = readFilterU32BE(bytes, offset);
        offset = afterCount;
        const values: string[] = [];
        for (let valueIndex = 0; valueIndex < valueCount; valueIndex += 1) {
          const [value, afterValue] = readFilterString(bytes, offset);
          offset = afterValue;
          values.push(value);
        }
        clauses.push({ kind: "AnyOf", values });
        break;
      }
      default:
        throw new Error(`unexpected clause variant ${variant}`);
    }
  }

  if (offset !== bytes.length) {
    throw new Error("buffer has trailing bytes");
  }

  return { clauses };
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

    it("should_reject_wrapped_response_with_trailing_bytes", () => {
      const writer = new BufferWriter(16);
      writer.writeU8(0);
      writer.writeU8(0);
      writer.writeU32BE(0);
      writer.writeU8(0xff);

      expect(() => StreamCodec.decodeBeginResponse(writer.getBuffer())).toThrow("trailing bytes");
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

    it("should_encode_append_with_discriminator", () => {
      const encoded = StreamCodec.encodeAppend(
        456n,
        100n,
        testData("record1"),
        undefined,
        "proj.alpha",
      );
      const reader = new BufferReader(encoded);

      expect(reader.readU64BE()).toBe(456n);
      expect(reader.readU64BE()).toBe(100n);
      expect(reader.readU32BE()).toBe(testData("record1").length);
      expect(Buffer.from(reader.readBytes(testData("record1").length)).toString()).toBe("record1");
      expect(reader.readU8()).toBe(0);
      expect(reader.readU8()).toBe(1);
      expect(reader.readString()).toBe("proj.alpha");
      expect(reader.isEOF()).toBe(true);
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

    it("should_encode_read_with_filter", () => {
      const filter: StreamFilterSet = {
        clauses: [{ kind: "Equals", value: "proj.alpha" }],
      };

      const encoded = StreamCodec.encodeRead("stream://test/events", 0n, 10, { filter });
      const reader = new BufferReader(encoded);

      expect(reader.readRoute()).toBe("stream://test/events");
      expect(reader.readU64BE()).toBe(0n);
      expect(reader.readU64BE()).toBe(10n);
      expect(reader.readU8()).toBe(0);
      expect(reader.readU8()).toBe(1);
      const filterLength = reader.readU32BE();
      const filterBytes = reader.readBytes(filterLength);
      expect(decodeStreamFilterSet(filterBytes)).toEqual(filter);
      expect(reader.isEOF()).toBe(true);
    });
  });

  describe("READ decoding", () => {
    it("should_decode_read_response_with_records", () => {
      // Arrange
      const response = encodeReadResponse(
        [
          {
            kind: "event",
            record: {
              offset: 100n,
              areaOffset: 200n,
              realmOffset: 300n,
              body: testData("record1"),
              metadata: testData("meta1"),
              timestamp: 111n,
            },
          },
          {
            kind: "event",
            record: {
              offset: 101n,
              areaOffset: 201n,
              realmOffset: 301n,
              body: testData("record2"),
              metadata: testData("meta2"),
              timestamp: 222n,
            },
          },
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
      expect(decoded.items).toHaveLength(2);
      expect(decoded.items[0].kind).toBe("event");
      if (decoded.items[0].kind !== "event") {
        throw new Error("expected event item");
      }
      expect(decoded.items[0].record.offset).toBe(100n);
      expect(decoded.items[1].kind).toBe("event");
      if (decoded.items[1].kind !== "event") {
        throw new Error("expected event item");
      }
      expect(decoded.items[1].record.offset).toBe(101n);
      expect(decoded.items[0].record.timestamp).toBe(111n);
      expect(Buffer.from(decoded.items[0].record.body).toString()).toBe("record1");
      expect(decoded.cursor).toMatchObject({
        lastResourceOffset: 101n,
        lastAreaOffset: 201n,
        lastRealmOffset: 301n,
        hasMore: false,
      });
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
      expect(decoded.items).toHaveLength(0);
      expect(decoded.cursor).toMatchObject({
        lastResourceOffset: 0n,
        hasMore: false,
      });
    });

    it("should_decode_read_response_with_filtered_marker", () => {
      const response = encodeReadResponse(
        [
          {
            kind: "filtered",
            offset: 44n,
            reason: "server_filter",
          },
          {
            kind: "filtered_range",
            fromOffset: 45n,
            toOffset: 48n,
            reason: "server_filter",
          },
        ],
        {
          lastResourceOffset: 48n,
          hasMore: true,
        },
      );

      const decoded = StreamCodec.decodeReadResponse(response);

      expect(decoded.items).toHaveLength(2);
      expect(decoded.items[0]).toEqual({
        kind: "filtered",
        offset: 44n,
        reason: "server_filter",
      });
      expect(decoded.items[1]).toEqual({
        kind: "filtered_range",
        fromOffset: 45n,
        toOffset: 48n,
        reason: "server_filter",
      });
      expect(decoded.cursor).toMatchObject({
        lastResourceOffset: 48n,
        hasMore: true,
      });
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
