/**
 * Stream domain codec for encoding/decoding messages
 * Per CLIENT_SPEC.md and fitz-go/internal/domains/stream/protocol.go
 */

import {
  BufferWriter,
  BufferReader,
  getRouteEncoding,
  utf8Decoder,
  utf8Encoder,
  writeU32BEAt,
  writeU64BEAt,
  writeU64BENumberAt,
} from "../../core/buffer";
import {
  StreamCommitMode,
  StreamCommitPayload,
  StreamDiscriminator,
  StreamFilterClause,
  StreamFilterSet,
  StreamFilteredReason,
  StreamMetadata,
  StreamRecord,
  StreamReadCursor,
  StreamReadItem,
  StreamReadOptions,
} from "./types";

export const StreamCodec = {
  /**
   * Encode BEGIN request
   * Payload: [route: string][has_ingest_metadata: u8][ingest_metadata?: bytes]
   */
  encodeBegin(route: string, ingestMetadata?: Uint8Array): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    if (ingestMetadata && ingestMetadata.length > 0) {
      writer.writeU8(1);
      writer.writeU32BE(ingestMetadata.length);
      writer.writeBytes(ingestMetadata);
    } else {
      writer.writeU8(0);
    }
    return writer.getBufferView();
  },

  /**
   * Decode BEGIN response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  decodeBeginResponse(payload: Uint8Array): {
    status: number;
    sessionId?: bigint;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    return { status: decoded.status, sessionId: decoded.sessionId };
  },

  /**
   * Encode APPEND request
   * Payload: [session_id: u64][expected_offset: u64][body: bytes][has_metadata: u8][metadata?: bytes][has_discriminator: u8][discriminator?: string]
   */
  encodeAppend(
    sessionId: bigint,
    expectedOffset: bigint,
    body: Uint8Array,
    metadata?: Uint8Array,
    discriminator?: StreamDiscriminator,
  ): Uint8Array {
    const hasMetadata = metadata !== undefined && metadata.length > 0;
    const hasDiscriminator = discriminator !== undefined && discriminator.length > 0;
    const discriminatorBytes = hasDiscriminator ? utf8Encoder.encode(discriminator) : undefined;
    const buffer = new Uint8Array(
      8 +
        8 +
        4 +
        body.length +
        1 +
        (hasMetadata && metadata !== undefined ? 4 + metadata.length : 0) +
        1 +
        (discriminatorBytes ? 4 + discriminatorBytes.length : 0),
    );
    let offset = 0;

    offset = writeU64BEAt(buffer, offset, sessionId);
    offset = writeU64BEAt(buffer, offset, expectedOffset);
    offset = writeU32BEAt(buffer, offset, body.length);
    buffer.set(body, offset);
    offset += body.length;

    buffer[offset++] = hasMetadata ? 1 : 0;
    if (hasMetadata && metadata !== undefined) {
      offset = writeU32BEAt(buffer, offset, metadata.length);
      buffer.set(metadata, offset);
      offset += metadata.length;
    }

    buffer[offset++] = hasDiscriminator ? 1 : 0;
    if (discriminatorBytes) {
      offset = writeU32BEAt(buffer, offset, discriminatorBytes.length);
      buffer.set(discriminatorBytes, offset);
    }

    return buffer;
  },

  /**
   * Decode APPEND response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  decodeAppendResponse(payload: Uint8Array): {
    status: number;
    offset?: bigint;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    if (decoded.status !== 0 || decoded.data.length < 8) {
      return { status: decoded.status };
    }

    const reader = new BufferReader(decoded.data);
    return { status: decoded.status, offset: reader.readU64BE() };
  },

  /**
   * Encode COMMIT request
   * Payload: [session_id: u64][mode: u8]
   */
  encodeCommit(sessionId: bigint, mode: StreamCommitMode): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeU64BE(sessionId);
    writer.writeU8(mode === "Sync" ? 1 : 0);
    return writer.getBufferView();
  },

  /**
   * Decode COMMIT response
   * Payload: [status: u8]
   */
  decodeCommitResponse(payload: Uint8Array): { status: number } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  },

  /**
   * Encode ROLLBACK request
   * Payload: [session_id: u64]
   */
  encodeRollback(sessionId: bigint): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeU64BE(sessionId);
    return writer.getBufferView();
  },

  /**
   * Decode ROLLBACK response
   * Payload: [status: u8]
   */
  decodeRollbackResponse(payload: Uint8Array): { status: number } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  },

  /**
   * Encode READ request
   * Payload: [route: string][start_offset: u64][limit: u64][has_max_bytes: u8][max_bytes?: u64][has_filter: u8][filter_length?: u32_be][filter?: custom]
   */
  encodeRead(
    route: string,
    startOffset: bigint,
    limit: number,
    options?: StreamReadOptions,
  ): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const hasMaxBytes = options?.maxBytes !== undefined;
    const filter = options?.filter;
    const hasFilter = filter !== undefined && filter.clauses.length > 0;
    let filterBytes: Uint8Array | undefined;

    if (hasFilter) {
      const filterWriter = new BufferWriter(64);
      encodeStreamFilterSet(filter, filterWriter);
      filterBytes = filterWriter.getBufferView();
    }

    const buffer = new Uint8Array(
      routeBytes.length +
        8 +
        8 +
        1 +
        (hasMaxBytes ? 8 : 0) +
        1 +
        (filterBytes ? 4 + filterBytes.length : 0),
    );
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    offset = writeU64BEAt(buffer, offset, startOffset);
    offset = writeU64BENumberAt(buffer, offset, limit);
    buffer[offset++] = hasMaxBytes ? 1 : 0;
    if (options?.maxBytes !== undefined) {
      offset = writeU64BEAt(buffer, offset, options.maxBytes);
    }

    buffer[offset++] = hasFilter ? 1 : 0;
    if (filterBytes) {
      offset = writeU32BEAt(buffer, offset, filterBytes.length);
      buffer.set(filterBytes, offset);
    }
    return buffer;
  },

  /**
   * Decode READ response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  decodeReadResponse(payload: Uint8Array): {
    status: number;
    items: StreamReadItem[];
    cursor?: StreamReadCursor;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    if (decoded.status !== 0) {
      return { status: decoded.status, items: [] };
    }

    if (decoded.data.length === 0) {
      return { status: decoded.status, items: [] };
    }

    const reader = new BufferReader(decoded.data);
    const count = reader.readU32BE();
    const items: StreamReadItem[] = [];

    for (let i = 0; i < count; i++) {
      items.push(this.decodeStreamReadItem(reader));
    }

    const cursor: StreamReadCursor = {
      lastResourceOffset: reader.readU64BE(),
      lastAreaOffset: reader.readOptionalU64() ?? undefined,
      lastRealmOffset: reader.readOptionalU64() ?? undefined,
      hasMore: reader.readU8() === 1,
    };

    if (!reader.isEOF()) {
      throw new Error("READ response has trailing bytes");
    }

    return { status: decoded.status, items, cursor };
  },

  /**
   * Encode LAST request
   * Payload: [route: string]
   */
  encodeLast(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBufferView();
  },

  /**
   * Decode LAST response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  decodeLastResponse(payload: Uint8Array): {
    status: number;
    record?: StreamRecord;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    if (decoded.status !== 0 || decoded.data.length === 0) {
      return { status: decoded.status };
    }

    const reader = new BufferReader(decoded.data);
    const record = this.decodeStreamRecord(reader);

    return { status: decoded.status, record };
  },

  /**
   * Encode METADATA request.
   * Payload: [route: string]
   */
  encodeMetadata(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBufferView();
  },

  /**
   * Decode METADATA response.
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  decodeMetadataResponse(payload: Uint8Array): {
    status: number;
    metadata?: StreamMetadata;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    if (decoded.status !== 0 || decoded.data.length === 0) {
      return { status: decoded.status };
    }

    const reader = new BufferReader(decoded.data);
    const firstResourceOffset = reader.readOptionalU64();
    const lastResourceOffset = reader.readOptionalU64();
    const recordCount = reader.readU64BE();
    const maxBatchEvents = reader.readU64BE();
    const maxBatchBytes = reader.readU64BE();
    const ttlSeconds = reader.readOptionalU64();
    const areaWatermark = reader.readU64BE();
    const realmWatermark = reader.readU64BE();

    return {
      status: decoded.status,
      metadata: {
        firstOffset: firstResourceOffset ?? 0n,
        lastOffset: lastResourceOffset ?? 0n,
        recordCount,
        maxBatchEvents,
        maxBatchBytes,
        ttlSeconds,
        areaWatermark,
        realmWatermark,
      },
    };
  },

  encodeSubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(pattern);
    return writer.getBufferView();
  },

  decodeSubscribeResponse(payload: Uint8Array): {
    status: number;
    subId?: bigint;
  } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    if (status !== 0 || reader.isEOF()) {
      return { status };
    }

    const hasValue = reader.readU8();
    if (hasValue !== 1 || reader.isEOF()) {
      return { status };
    }

    return { status, subId: reader.readU64BE() };
  },

  encodeUnsubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(pattern);
    return writer.getBufferView();
  },

  decodeUnsubscribeResponse(payload: Uint8Array): { status: number } {
    const reader = new BufferReader(payload);
    return { status: reader.readU8() };
  },

  decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
    rawPayload: Uint8Array;
    parsedPayload: StreamCommitPayload;
  } {
    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();
    const rawPayload = reader.readBytes(reader.readU32BE());
    let parsedPayload: StreamCommitPayload = {};
    if (rawPayload.length > 0) {
      try {
        parsedPayload = JSON.parse(utf8Decoder.decode(rawPayload)) as StreamCommitPayload;
      } catch {
        parsedPayload = {};
      }
    }
    return {
      subId,
      route,
      rawPayload,
      parsedPayload,
    };
  },

  decodeStreamRecord(reader: BufferReader): StreamRecord {
    const offset = reader.readU64BE();
    const areaOffset = reader.readOptionalU64();
    const realmOffset = reader.readOptionalU64();
    const body = reader.readBytes(reader.readU32BE());
    const metadata = this.readOptionalBytes(reader);
    const timestamp = reader.readU64BE();

    return {
      offset,
      timestamp,
      body,
      areaOffset,
      realmOffset,
      metadata,
    };
  },

  flattenStreamReadItems(items: StreamReadItem[]): StreamRecord[] {
    return items.flatMap((item) => (item.kind === "event" ? [item.record] : []));
  },

  decodeStreamReadItem(reader: BufferReader): StreamReadItem {
    const tag = reader.readU8();
    switch (tag) {
      case 0:
        return { kind: "event", record: this.decodeStreamRecord(reader) };
      case 1:
        return {
          kind: "filtered",
          offset: reader.readU64BE(),
          reason: this.decodeStreamFilteredReason(reader),
        };
      case 2:
        return {
          kind: "filtered_range",
          fromOffset: reader.readU64BE(),
          toOffset: reader.readU64BE(),
          reason: this.decodeStreamFilteredReason(reader),
        };
      default:
        throw new Error(`unknown stream read item tag: ${tag}`);
    }
  },

  decodeStreamFilteredReason(reader: BufferReader): StreamFilteredReason | undefined {
    const tag = reader.readU8();
    switch (tag) {
      case 0:
        return undefined;
      case 1:
        return "server_filter";
      case 2:
        return "permission";
      case 3:
        return "projection";
      default:
        throw new Error(`unknown stream filtered reason tag: ${tag}`);
    }
  },

  readOptionalBytes(reader: BufferReader): Uint8Array | undefined {
    const hasValue = reader.readU8();
    if (hasValue !== 1) {
      return undefined;
    }

    const length = reader.readU32BE();
    return reader.readBytes(length);
  },

  decodeWrappedResponse(payload: Uint8Array): {
    status: number;
    sessionId?: bigint;
    data: Uint8Array;
  } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      return { status, data: new Uint8Array(0) };
    }

    let sessionId: bigint | undefined;
    if (!reader.isEOF()) {
      const hasSessionId = reader.readU8();
      if (hasSessionId === 1 && reader.remainingBytes() >= 8) {
        sessionId = reader.readU64BE();
      }
    }

    if (reader.isEOF()) {
      return { status, sessionId, data: new Uint8Array(0) };
    }

    const dataLength = reader.readU32BE();
    const data = reader.readBytes(dataLength);
    if (!reader.isEOF()) {
      throw new Error("wrapped response has trailing bytes");
    }

    return { status, sessionId, data };
  },
};

function encodeStreamFilterSet(filter: StreamFilterSet, writer: BufferWriter): void {
  writer.writeU8(0);
  writer.writeU8(0xf1);
  writer.writeU32BE(filter.clauses.length);

  for (const clause of filter.clauses) {
    encodeStreamFilterClause(writer, clause);
  }
}

function encodeStreamFilterClause(writer: BufferWriter, clause: StreamFilterClause): void {
  switch (clause.kind) {
    case "Equals":
      writer.writeU8(0);
      writer.writeString(clause.value);
      return;
    case "NotEquals":
      writer.writeU8(1);
      writer.writeString(clause.value);
      return;
    case "StartsWith":
      writer.writeU8(2);
      writer.writeString(clause.value);
      return;
    case "AnyOf":
      writer.writeU8(3);
      writer.writeU32BE(clause.values.length);
      for (const value of clause.values) {
        writer.writeString(value);
      }
      return;
  }
}
