/**
 * Stream domain codec for encoding/decoding messages
 * Per CLIENT_SPEC.md and fitz-go/internal/domains/stream/protocol.go
 */

import { BufferWriter, BufferReader, utf8Decoder, utf8Encoder } from "../../core/buffer";
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
  StreamReadPage,
  StreamReadOptions,
} from "./types";

class BincodeWriter {
  private bytes: number[] = [];

  writeU8(value: number): void {
    this.bytes.push(value & 0xff);
  }

  writeU32(value: number): void {
    const unsigned = value >>> 0;
    this.bytes.push(
      unsigned & 0xff,
      (unsigned >>> 8) & 0xff,
      (unsigned >>> 16) & 0xff,
      (unsigned >>> 24) & 0xff,
    );
  }

  writeU64(value: bigint): void {
    const masked = BigInt.asUintN(64, value);
    this.bytes.push(
      Number(masked & 0xffn),
      Number((masked >> 8n) & 0xffn),
      Number((masked >> 16n) & 0xffn),
      Number((masked >> 24n) & 0xffn),
      Number((masked >> 32n) & 0xffn),
      Number((masked >> 40n) & 0xffn),
      Number((masked >> 48n) & 0xffn),
      Number((masked >> 56n) & 0xffn),
    );
  }

  writeBytes(data: Uint8Array): void {
    for (const byte of data) {
      this.bytes.push(byte);
    }
  }

  writeString(value: string): void {
    const encoded = utf8Encoder.encode(value);
    this.writeU64(BigInt(encoded.length));
    this.writeBytes(encoded);
  }

  finish(): Uint8Array {
    return Uint8Array.from(this.bytes);
  }
}

export class StreamCodec {
  /**
   * Encode BEGIN request
   * Payload: [route: string][has_ingest_metadata: u8][ingest_metadata?: bytes]
   */
  static encodeBegin(route: string, ingestMetadata?: Uint8Array): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    if (ingestMetadata && ingestMetadata.length > 0) {
      writer.writeU8(1);
      writer.writeU32BE(ingestMetadata.length);
      writer.writeBytes(ingestMetadata);
    } else {
      writer.writeU8(0);
    }
    return writer.getBuffer();
  }

  /**
   * Decode BEGIN response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  static decodeBeginResponse(payload: Uint8Array): {
    status: number;
    sessionId?: bigint;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    return { status: decoded.status, sessionId: decoded.sessionId };
  }

  /**
   * Encode APPEND request
   * Payload: [session_id: u64][expected_offset: u64][body: bytes][has_metadata: u8][metadata?: bytes][has_discriminator: u8][discriminator?: string]
   */
  static encodeAppend(
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

    return writer.getBuffer();
  }

  /**
   * Decode APPEND response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  static decodeAppendResponse(payload: Uint8Array): {
    status: number;
    offset?: bigint;
  } {
    const decoded = this.decodeWrappedResponse(payload);
    if (decoded.status !== 0 || decoded.data.length < 8) {
      return { status: decoded.status };
    }

    const reader = new BufferReader(decoded.data);
    return { status: decoded.status, offset: reader.readU64BE() };
  }

  /**
   * Encode COMMIT request
   * Payload: [session_id: u64][mode: u8]
   */
  static encodeCommit(sessionId: bigint, mode: StreamCommitMode): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeU64BE(sessionId);
    writer.writeU8(mode === "Sync" ? 1 : 0);
    return writer.getBuffer();
  }

  /**
   * Decode COMMIT response
   * Payload: [status: u8]
   */
  static decodeCommitResponse(payload: Uint8Array): { status: number } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode ROLLBACK request
   * Payload: [session_id: u64]
   */
  static encodeRollback(sessionId: bigint): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeU64BE(sessionId);
    return writer.getBuffer();
  }

  /**
   * Decode ROLLBACK response
   * Payload: [status: u8]
   */
  static decodeRollbackResponse(payload: Uint8Array): { status: number } {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode READ request
   * Payload: [route: string][start_offset: u64][limit: u64][has_max_bytes: u8][max_bytes?: u64][has_filter: u8][filter_length?: u32][filter?: bincode]
   */
  static encodeRead(
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
    const filterBytes =
      filter && filter.clauses.length > 0 ? encodeStreamFilterSet(filter) : undefined;
    if (filterBytes !== undefined) {
      writer.writeU8(1);
      writer.writeU32BE(filterBytes.length);
      writer.writeBytes(filterBytes);
    } else {
      writer.writeU8(0);
    }
    return writer.getBuffer();
  }

  /**
   * Decode READ response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  static decodeReadResponse(payload: Uint8Array): {
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
  }

  /**
   * Encode LAST request
   * Payload: [route: string]
   */
  static encodeLast(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode LAST response
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  static decodeLastResponse(payload: Uint8Array): {
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
  }

  /**
   * Encode METADATA request.
   * Payload: [route: string]
   */
  static encodeMetadata(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode METADATA response.
   * Payload: [status: u8][has_session_id: u8][session_id?: u64][data: bytes]
   */
  static decodeMetadataResponse(payload: Uint8Array): {
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
  }

  static encodeSubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(pattern);
    return writer.getBuffer();
  }

  static decodeSubscribeResponse(payload: Uint8Array): {
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
  }

  static encodeUnsubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(pattern);
    return writer.getBuffer();
  }

  static decodeUnsubscribeResponse(payload: Uint8Array): { status: number } {
    const reader = new BufferReader(payload);
    return { status: reader.readU8() };
  }

  static decodeNotification(payload: Uint8Array): {
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
  }

  private static decodeStreamRecord(reader: BufferReader): StreamRecord {
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
  }

  static flattenStreamReadItems(items: StreamReadItem[]): StreamRecord[] {
    return items.flatMap((item) => (item.kind === "event" ? [item.record] : []));
  }

  private static decodeStreamReadItem(reader: BufferReader): StreamReadItem {
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
  }

  private static decodeStreamFilteredReason(reader: BufferReader): StreamFilteredReason | undefined {
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
  }

  private static readOptionalBytes(reader: BufferReader): Uint8Array | undefined {
    const hasValue = reader.readU8();
    if (hasValue !== 1) {
      return undefined;
    }

    const length = reader.readU32BE();
    return reader.readBytes(length);
  }

  private static decodeWrappedResponse(payload: Uint8Array): {
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
  }
}

function encodeStreamFilterSet(filter: StreamFilterSet): Uint8Array {
  const writer = new BincodeWriter();
  writer.writeU64(BigInt(filter.clauses.length));

  for (const clause of filter.clauses) {
    encodeStreamFilterClause(writer, clause);
  }

  return writer.finish();
}

function encodeStreamFilterClause(writer: BincodeWriter, clause: StreamFilterClause): void {
  switch (clause.kind) {
    case "Equals":
      writer.writeU32(0);
      writer.writeString(clause.value);
      return;
    case "NotEquals":
      writer.writeU32(1);
      writer.writeString(clause.value);
      return;
    case "StartsWith":
      writer.writeU32(2);
      writer.writeString(clause.value);
      return;
    case "AnyOf":
      writer.writeU32(3);
      writer.writeU64(BigInt(clause.values.length));
      for (const value of clause.values) {
        writer.writeString(value);
      }
      return;
  }
}
