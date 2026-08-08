/**
 * Stream domain codec for encoding/decoding messages
 * Per CLIENT_SPEC.md and fitz-go/internal/domains/stream/protocol.go
 */

import {
  createBufferWriter,
  createBufferReader,
  getRouteEncoding,
  utf8Decoder,
  utf8Encoder,
  writeU32BEAt,
  writeU64BEAt,
  writeU64BENumberAt,
  type BufferReader,
  type BufferWriter,
} from "../../core/buffer";
import { StreamError } from "../../core/errors";
import { isRouteShape, isStreamSelectorShape } from "../_routes";
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
    const routeBytes = getRouteEncoding(route);
    if (ingestMetadata && ingestMetadata.length > 0) {
      const buffer = new Uint8Array(routeBytes.length + 1 + 4 + ingestMetadata.length);
      let offset = 0;

      buffer.set(routeBytes, offset);
      offset += routeBytes.length;
      buffer[offset++] = 1;
      offset = writeU32BEAt(buffer, offset, ingestMetadata.length);
      buffer.set(ingestMetadata, offset);
      return buffer;
    }

    const buffer = new Uint8Array(routeBytes.length + 1);
    buffer.set(routeBytes, 0);
    buffer[routeBytes.length] = 0;
    return buffer;
  },

  /** Decode BEGIN response: [status][session_id] on success. */
  decodeBeginResponse(payload: Uint8Array): {
    status: number;
    sessionId?: bigint;
    errorCode?: number;
    errorMessage?: string;
  } {
    const decoded = this.decodePlainWrappedResponse(payload);
    if (decoded.status !== 0) return decoded;
    if (decoded.data.length < 8) return { status: decoded.status };
    return { status: decoded.status, sessionId: createBufferReader(decoded.data).readU64BE() };
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

  /** Decode APPEND response: [status][assigned_offset] on success. */
  decodeAppendResponse(payload: Uint8Array): {
    status: number;
    offset?: bigint;
    errorCode?: number;
    errorMessage?: string;
  } {
    const decoded = this.decodePlainWrappedResponse(payload);
    if (decoded.status !== 0 || decoded.data.length < 12) {
      return decoded;
    }

    const reader = createBufferReader(decoded.data);
    const length = reader.readU32BE();
    if (length !== 8 || reader.remainingBytes() !== length) {
      throw new StreamError(
        "APPEND response has invalid payload length",
        "APPEND_INVALID_RESPONSE",
      );
    }
    return { status: decoded.status, offset: reader.readU64BE() };
  },

  /**
   * Encode COMMIT request
   * Payload: [session_id: u64][mode: u8]
   */
  encodeCommit(sessionId: bigint, mode: StreamCommitMode): Uint8Array {
    const buffer = new Uint8Array(9);
    writeU64BEAt(buffer, 0, sessionId);
    buffer[8] = mode === "Sync" ? 1 : 0;
    return buffer;
  },

  /**
   * Decode COMMIT response
   * Payload: [status: u8]
   */
  decodeCommitResponse(payload: Uint8Array): {
    status: number;
    errorCode?: number;
    errorMessage?: string;
  } {
    return this.decodeWrappedResponse(payload);
  },

  /**
   * Encode ROLLBACK request
   * Payload: [session_id: u64]
   */
  encodeRollback(sessionId: bigint): Uint8Array {
    const buffer = new Uint8Array(8);
    writeU64BEAt(buffer, 0, sessionId);
    return buffer;
  },

  /**
   * Decode ROLLBACK response
   * Payload: [status: u8]
   */
  decodeRollbackResponse(payload: Uint8Array): {
    status: number;
    errorCode?: number;
    errorMessage?: string;
  } {
    return this.decodeWrappedResponse(payload);
  },

  /**
   * Encode READ request
   * Payload: [route][start][limit][optional max_bytes][optional filter][optional cursor_fingerprint][optional captured_watermark]
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
      const filterWriter = createBufferWriter(64);
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
        (filterBytes ? 4 + filterBytes.length : 0) +
        1 +
        (options?.cursorFingerprint === undefined ? 0 : 8) +
        1 +
        (options?.capturedWatermark === undefined ? 0 : 8),
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
      offset += filterBytes.length;
    }
    buffer[offset++] = options?.cursorFingerprint === undefined ? 0 : 1;
    if (options?.cursorFingerprint !== undefined)
      offset = writeU64BEAt(buffer, offset, options.cursorFingerprint);
    buffer[offset++] = options?.capturedWatermark === undefined ? 0 : 1;
    if (options?.capturedWatermark !== undefined)
      writeU64BEAt(buffer, offset, options.capturedWatermark);
    return buffer;
  },

  /**
   * Decode READ response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  decodeReadResponse(
    payload: Uint8Array,
    selector: string,
  ): {
    status: number;
    items: StreamReadItem[];
    cursor?: StreamReadCursor;
    errorCode?: number;
    errorMessage?: string;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    if (decoded.status !== 0) {
      return { ...decoded, items: [] };
    }

    if (!isStreamSelector(selector)) {
      throw new StreamError(
        "READ response requires a canonical stream selector",
        "READ_INVALID_RESPONSE",
      );
    }

    if (decoded.data.length === 0) {
      return { status: decoded.status, items: [] };
    }

    const envelope = createBufferReader(decoded.data);
    if (envelope.readU8() !== 0) {
      throw new StreamError("READ response has invalid session flag", "READ_INVALID_RESPONSE");
    }
    const dataLength = envelope.readU32BE();
    if (dataLength !== envelope.remainingBytes()) {
      throw new StreamError("READ response has invalid payload length", "READ_INVALID_RESPONSE");
    }
    const reader = createBufferReader(envelope.readBytes(dataLength));
    const extended = isGlobalSelector(selector);
    const count = reader.readU32BE();
    const items: StreamReadItem[] = [];

    for (let i = 0; i < count; i++) {
      const concreteRoute = reader.readRoute();
      if (!isRouteShape(concreteRoute, "stream", 3)) {
        throw new StreamError(
          `READ response contains invalid concrete stream route: ${concreteRoute}`,
          "READ_INVALID_RESPONSE",
        );
      }
      items.push(this.decodeStreamReadItem(reader, concreteRoute, extended));
    }

    const hasGlobal = extended;
    const lastResourceOffset = reader.readU64BE();
    const lastAreaOffset = reader.readOptionalU64() ?? undefined;
    const lastRealmOffset = reader.readOptionalU64() ?? undefined;
    const lastGlobalOffset = extended ? (reader.readOptionalU64() ?? undefined) : undefined;
    const hasMoreFlag = reader.readU8();
    if (hasMoreFlag !== 0 && hasMoreFlag !== 1) {
      throw new StreamError("READ response has an invalid hasMore flag", "READ_INVALID_RESPONSE");
    }
    const cursor: StreamReadCursor = {
      lastResourceOffset,
      lastAreaOffset,
      lastRealmOffset,
      lastGlobalOffset,
      hasMore: hasMoreFlag === 1,
      cursorFingerprint: hasGlobal ? (reader.readOptionalU64() ?? undefined) : undefined,
      capturedWatermark: hasGlobal ? (reader.readOptionalU64() ?? undefined) : undefined,
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
    return getRouteEncoding(route).slice();
  },

  /**
   * Decode LAST response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  decodeLastResponse(
    payload: Uint8Array,
    route: string,
  ): {
    status: number;
    record?: StreamRecord;
    errorCode?: number;
    errorMessage?: string;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    if (decoded.status !== 0 || decoded.data.length === 0) {
      return decoded;
    }

    const reader = createBufferReader(decoded.data);
    const record = this.decodeStreamRecord(reader, route, false);
    if (!reader.isEOF()) {
      throw new StreamError("LAST response has trailing bytes", "LAST_INVALID_RESPONSE");
    }

    return { status: decoded.status, record };
  },

  /**
   * Encode METADATA request.
   * Payload: [route: string]
   */
  encodeMetadata(route: string): Uint8Array {
    return getRouteEncoding(route).slice();
  },

  /**
   * Decode METADATA response.
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  decodeMetadataResponse(payload: Uint8Array): {
    status: number;
    metadata?: StreamMetadata;
    errorCode?: number;
    errorMessage?: string;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    if (decoded.status !== 0 || decoded.data.length === 0) {
      return decoded;
    }

    const reader = createBufferReader(decoded.data);
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
    const writer = createBufferWriter(128);
    writer.writeRoute(pattern);
    return writer.getBufferView();
  },

  encodeUnsubscribe(pattern: string): Uint8Array {
    const writer = createBufferWriter(128);
    writer.writeRoute(pattern);
    return writer.getBufferView();
  },

  decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
    rawPayload: Uint8Array;
    parsedPayload: StreamCommitPayload;
  } {
    const reader = createBufferReader(payload);
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

  decodeStreamRecord(reader: BufferReader, route: string, extended = false): StreamRecord {
    const offset = reader.readU64BE();
    const areaOffset = reader.readOptionalU64();
    const realmOffset = reader.readOptionalU64();
    const globalOffset = extended ? reader.readOptionalU64() : undefined;
    const body = reader.readBytes(reader.readU32BE());
    const metadata = this.readOptionalBytes(reader);
    const timestamp = reader.readU64BE();

    return {
      route,
      offset,
      timestamp,
      body,
      areaOffset,
      realmOffset,
      globalOffset,
      metadata,
    };
  },

  flattenStreamReadItems(items: StreamReadItem[]): StreamRecord[] {
    return items.flatMap((item) => (item.kind === "event" ? [item.record] : []));
  },

  decodeStreamReadItem(reader: BufferReader, route: string, extended = false): StreamReadItem {
    const tag = reader.readU8();
    switch (tag) {
      case 0:
        return {
          kind: "event",
          route,
          record: this.decodeStreamRecord(reader, route, extended),
        };
      case 1:
        return {
          kind: "filtered",
          route,
          offset: reader.readU64BE(),
          reason: this.decodeStreamFilteredReason(reader),
        };
      case 2:
        return {
          kind: "filtered_range",
          route,
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
    errorCode?: number;
    errorMessage?: string;
  } {
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      if (reader.remainingBytes() < 8) return { status, data: new Uint8Array(0) };
      const errorCode = reader.readU32BE();
      const errorMessage = reader.readString();
      return { status, data: new Uint8Array(0), errorCode, errorMessage };
    }
    return { status, data: reader.remaining() };
  },

  decodePlainWrappedResponse(payload: Uint8Array): {
    status: number;
    data: Uint8Array;
    errorMessage?: string;
  } {
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      return { status, data: new Uint8Array(0), errorMessage: reader.readString() };
    }
    return { status, data: reader.remaining() };
  },
};

export function isGlobalSelector(selector: string): boolean {
  return selector === "stream://**" || selector === "stream://*/*/*";
}

function isStreamSelector(selector: string): boolean {
  return isStreamSelectorShape(selector);
}

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
