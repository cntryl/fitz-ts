/**
 * Stream domain codec for encoding/decoding messages
 * Per CLIENT_SPEC.md and fitz-go/internal/domains/stream/protocol.go
 */

import { BufferWriter, BufferReader, utf8Decoder } from "../../core/buffer";
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
    const writer = new BufferWriter(512);
    writer.writeU64BE(sessionId);
    writer.writeU64BE(expectedOffset);
    writer.writeU32BE(body.length);
    writer.writeBytes(body);
    if (metadata && metadata.length > 0) {
      writer.writeU8(1);
      writer.writeU32BE(metadata.length);
      writer.writeBytes(metadata);
    } else {
      writer.writeU8(0);
    }

    if (discriminator !== undefined && discriminator.length > 0) {
      writer.writeU8(1);
      writer.writeString(discriminator);
    } else {
      writer.writeU8(0);
    }

    return writer.getBufferView();
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
   * Payload: [route: string][start_offset: u64][limit: u64][has_max_bytes: u8][max_bytes?: u64][has_filter: u8][filter_length?: u32][filter?: bincode]
   */
  encodeRead(
    route: string,
    startOffset: bigint,
    limit: number,
    options?: StreamReadOptions,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU64BE(startOffset);
    writer.writeU64BE(BigInt(limit));
    if (options?.maxBytes !== undefined) {
      writer.writeU8(1);
      writer.writeU64BE(options.maxBytes);
    } else {
      writer.writeU8(0);
    }

    const filter = options?.filter;
    if (filter && filter.clauses.length > 0) {
      writer.writeU8(1);
      const filterLengthOffset = writer.getLength();
      writer.writeU32BE(0);
      const filterStart = writer.getLength();
      encodeStreamFilterSet(filter, writer);
      writer.overwriteU32BE(filterLengthOffset, writer.getLength() - filterStart);
    } else {
      writer.writeU8(0);
    }
    return writer.getBufferView();
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
    return { status, sessionId, data };
  },
};

function encodeStreamFilterSet(filter: StreamFilterSet, writer: BufferWriter): void {
  writer.writeU64LE(BigInt(filter.clauses.length));

  for (const clause of filter.clauses) {
    encodeStreamFilterClause(writer, clause);
  }
}

function encodeStreamFilterClause(writer: BufferWriter, clause: StreamFilterClause): void {
  switch (clause.kind) {
    case "Equals":
      writer.writeU32LE(0);
      writer.writeStringU64LE(clause.value);
      return;
    case "NotEquals":
      writer.writeU32LE(1);
      writer.writeStringU64LE(clause.value);
      return;
    case "StartsWith":
      writer.writeU32LE(2);
      writer.writeStringU64LE(clause.value);
      return;
    case "AnyOf":
      writer.writeU32LE(3);
      writer.writeU64LE(BigInt(clause.values.length));
      for (const value of clause.values) {
        writer.writeStringU64LE(value);
      }
      return;
  }
}
