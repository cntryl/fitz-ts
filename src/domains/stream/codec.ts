/**
 * Stream domain codec for encoding/decoding messages
 * Per CLIENT_SPEC.md and fitz-go/internal/domains/stream/protocol.go
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import { StreamRecord, StreamMetadata } from "./types";

export class StreamCodec {
  /**
   * Encode BEGIN request
   * Payload: [route: string][expected_offset: u64][has_ingest_metadata: u8][ingest_metadata?: bytes]
   */
  static encodeBegin(
    route: string,
    expectedOffset: bigint,
    ingestMetadata?: Uint8Array,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU64BE(expectedOffset);
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
   * Payload: [session_id: u64][body: bytes][has_metadata: u8][metadata?: bytes]
   */
  static encodeAppend(
    sessionId: bigint,
    body: Uint8Array,
    metadata?: Uint8Array,
  ): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeU64BE(sessionId);
    writer.writeU32BE(body.length);
    writer.writeBytes(body);
    if (metadata && metadata.length > 0) {
      writer.writeU8(1);
      writer.writeU32BE(metadata.length);
      writer.writeBytes(metadata);
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
  static encodeCommit(sessionId: bigint, mode: number = 0): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeU64BE(sessionId);
    writer.writeU8(mode);
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
   * Payload: [route: string][start_offset: u64][limit: u64][has_max_bytes: u8][max_bytes?: u64]
   */
  static encodeRead(
    route: string,
    startOffset: bigint,
    limit: number,
    maxBytes?: bigint,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU64BE(startOffset);
    writer.writeU64BE(BigInt(limit));
    if (maxBytes !== undefined) {
      writer.writeU8(1);
      writer.writeU64BE(maxBytes);
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
    records: StreamRecord[];
  } {
    const decoded = this.decodeWrappedResponse(payload);
    if (decoded.status !== 0 || decoded.data.length === 0) {
      return { status: decoded.status, records: [] };
    }

    const reader = new BufferReader(decoded.data);
    const count = reader.readU32BE();
    const records: StreamRecord[] = [];

    for (let i = 0; i < count; i++) {
      const offset = reader.readU64BE();
      const bodyLen = reader.readU32BE();
      const body = reader.readBytes(bodyLen);

      records.push({ offset, timestamp: 0n, body });
    }

    return { status: decoded.status, records };
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
    const offset = reader.readU64BE();
    const bodyLen = reader.readU32BE();
    const body = reader.readBytes(bodyLen);

    return { status: decoded.status, record: { offset, timestamp: 0n, body } };
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
    const firstOffset = reader.readU64BE();
    const lastOffset = reader.readU64BE();
    const recordCount = reader.readU64BE();

    return {
      status: decoded.status,
      metadata: { firstOffset, lastOffset, recordCount },
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
    parsedPayload: unknown;
  } {
    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();
    const rawPayload = reader.readBytes(reader.readU32BE());
    let parsedPayload: unknown = undefined;
    if (rawPayload.length > 0) {
      parsedPayload = rawPayload;
    }
    return {
      subId,
      route,
      rawPayload,
      parsedPayload,
    };
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
