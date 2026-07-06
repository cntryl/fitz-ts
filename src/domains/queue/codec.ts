/**
 * Queue domain codec for encoding and decoding protocol messages.
 */

import {
  createBufferReader,
  getRouteEncoding,
  utf8Decoder,
  writeU32BEAt,
  writeU64BEAt,
  writeU64BENumberAt,
  type BufferReader,
} from "../../core/buffer";
import {
  QueueEnqueueResponse,
  QueueReserveResponse,
  QueueCompleteResponse,
  QueueExtendResponse,
  QueueSubscribeResponse,
  QueueUnsubscribeResponse,
  EnqueueOptions,
} from "./types";

export const QueueCodec = {
  /**
   * Encode ENQUEUE request.
   * Payload: [route: string][body_len: u32][body: bytes][has_delay: u8][delay_seconds: u64 if has_delay]
   */
  encodeEnqueue(route: string, body: Uint8Array, options?: EnqueueOptions): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const delaySeconds = options?.delayMs ? Math.floor(options.delayMs / 1000) : 0;
    const hasDelay = delaySeconds > 0 ? 1 : 0;

    const buffer = new Uint8Array(routeBytes.length + 4 + body.length + 1 + (hasDelay ? 8 : 0));
    let offset = 0;
    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    offset = writeU32BEAt(buffer, offset, body.length);
    buffer.set(body, offset);
    offset += body.length;
    buffer[offset++] = hasDelay;
    if (hasDelay) {
      writeU64BENumberAt(buffer, offset, delaySeconds);
    }

    return buffer;
  },

  /**
   * Decode ENQUEUE response.
   * Payload: [status: u8][message_id: u64]
   */
  decodeEnqueueResponse(payload: Uint8Array): QueueEnqueueResponse {
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      return { status, ...this.decodeErrorResponse(reader) };
    }

    let messageId: bigint | undefined;
    if (reader.remainingBytes() >= 8) {
      messageId = reader.readU64BE();
    }
    return { status, messageId };
  },

  /**
   * Encode RESERVE request.
   * Payload: [route: string][lease_seconds: u64][has_batch_size: u8][batch_size: u32]
   *
   * Long polling is handled client-side by QueueClient.reserve().
   */
  encodeReserve(route: string, leaseSeconds: number, batchSize?: number): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const hasBatchSize = batchSize !== undefined && batchSize > 0 ? 1 : 0;
    const buffer = new Uint8Array(routeBytes.length + 8 + 1 + (hasBatchSize ? 4 : 0));
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    offset = writeU64BENumberAt(buffer, offset, leaseSeconds);
    buffer[offset++] = hasBatchSize;
    if (hasBatchSize && batchSize !== undefined) {
      writeU32BEAt(buffer, offset, batchSize);
    }

    return buffer;
  },

  /**
   * Decode RESERVE response.
   * Payload: [status: u8][lease_count: u32]([message_id: u64][lease_token: u64][body_len: u32][body: bytes] ...)
   */
  decodeReserveResponse(payload: Uint8Array): QueueReserveResponse {
    const reader = createBufferReader(payload);
    const status = reader.readU8();

    if (status !== 0) {
      return { status, ...this.decodeErrorResponse(reader) };
    }

    if (reader.isEOF() || reader.remainingBytes() < 4) {
      return { status, items: [] };
    }

    const leaseCount = reader.readU32BE();
    const items: Array<{ id: bigint; token: bigint; body: Uint8Array }> = [];

    for (let i = 0; i < leaseCount; i++) {
      const id = reader.readU64BE();
      const token = reader.readU64BE();
      const bodyLen = reader.readU32BE();
      const body = reader.readBytes(bodyLen);

      items.push({ id, token, body });
    }

    return { status, items };
  },

  /**
   * Encode COMPLETE request.
   * Payload: [route: string][message_id: u64][lease_token: u64]
   */
  encodeComplete(route: string, messageId: bigint, leaseToken: bigint): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const buffer = new Uint8Array(routeBytes.length + 16);
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    offset = writeU64BEAt(buffer, offset, messageId);
    writeU64BEAt(buffer, offset, leaseToken);
    return buffer;
  },

  /**
   * Decode COMPLETE response.
   * Payload: [status: u8]
   */
  decodeCompleteResponse(payload: Uint8Array): QueueCompleteResponse {
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      return { status, ...this.decodeErrorResponse(reader) };
    }

    return { status };
  },

  /**
   * Encode EXTEND request.
   * Payload: [route: string][message_id: u64][lease_token: u64][lease_seconds: u64]
   */
  encodeExtend(
    route: string,
    messageId: bigint,
    leaseToken: bigint,
    leaseSeconds: number,
  ): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const buffer = new Uint8Array(routeBytes.length + 24);
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    offset = writeU64BEAt(buffer, offset, messageId);
    offset = writeU64BEAt(buffer, offset, leaseToken);
    writeU64BENumberAt(buffer, offset, leaseSeconds);
    return buffer;
  },

  /**
   * Decode EXTEND response.
   * Payload: [status: u8]
   */
  decodeExtendResponse(payload: Uint8Array): QueueExtendResponse {
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      return { status, ...this.decodeErrorResponse(reader) };
    }

    return { status };
  },

  /**
   * Encode SUBSCRIBE request.
   * Payload: [pattern: string]
   */
  encodeSubscribe(pattern: string): Uint8Array {
    return getRouteEncoding(pattern).slice();
  },

  /**
   * Decode SUBSCRIBE response.
   * Payload: [status: u8][sub_id: u64]
   */
  decodeSubscribeResponse(payload: Uint8Array): QueueSubscribeResponse {
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      return { status, ...this.decodeErrorResponse(reader) };
    }

    if (reader.remainingBytes() === 8) {
      return { status, subId: reader.readU64BE() };
    }

    if (reader.remainingBytes() >= 9) {
      const hasSubId = reader.readU8();
      if (hasSubId === 1 && reader.remainingBytes() >= 8) {
        return { status, subId: reader.readU64BE() };
      }
    }

    if (reader.isEOF()) {
      return { status };
    }

    return { status };
  },

  /**
   * Encode UNSUBSCRIBE request.
   * Payload: [pattern: string]
   */
  encodeUnsubscribe(pattern: string): Uint8Array {
    return getRouteEncoding(pattern).slice();
  },

  /**
   * Decode UNSUBSCRIBE response.
   * Payload: [status: u8]
   */
  decodeUnsubscribeResponse(payload: Uint8Array): QueueUnsubscribeResponse {
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      return { status, ...this.decodeErrorResponse(reader) };
    }

    return { status };
  },

  /**
   * Decode notification payload.
   * Payload: [sub_id: u64][route: string]
   */
  decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
  } {
    const reader = createBufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();

    return { subId, route };
  },

  decodeErrorResponse(reader: BufferReader): {
    errorCode?: number;
    errorMessage?: string;
  } {
    if (reader.remainingBytes() === 1) {
      return { errorCode: reader.readU8() };
    }

    if (reader.remainingBytes() >= 4) {
      const messageLength = reader.readU32BE();
      if (messageLength <= reader.remainingBytes()) {
        return {
          errorMessage: utf8Decoder.decode(reader.readBytes(messageLength)),
        };
      }
    }

    return {};
  },
};
