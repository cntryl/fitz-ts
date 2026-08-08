/**
 * Queue domain codec for encoding and decoding protocol messages.
 */

import {
  createBufferReader,
  getRouteEncoding,
  writeU32BEAt,
  writeU64BEAt,
  writeU64BENumberAt,
  type BufferReader,
} from "../../core/buffer";
import { parseStandardResponse } from "../../protocol/response";
import { QueueError } from "../../core/errors";
import { isRouteShape, routeMatchesPattern } from "../_routes";
import {
  QueueEnqueueResponse,
  QueueReserveResponse,
  QueueCompleteResponse,
  QueueExtendResponse,
  QueueSubscribeResponse,
  QueueUnsubscribeResponse,
  EnqueueOptions,
} from "./types";

type ParsedQueueResponse =
  | { status: 0; reader: BufferReader }
  | { status: 1; errorCode?: number; errorMessage?: string };

function parseQueueResponse(payload: Uint8Array): ParsedQueueResponse {
  const parsed = parseStandardResponse(payload);
  if (!parsed.success) {
    return {
      status: 1,
      errorCode: parsed.errorCode,
      errorMessage: parsed.error,
    };
  }

  return { status: 0, reader: createBufferReader(parsed.data) };
}

function parsePlainQueueResponse(payload: Uint8Array): ParsedQueueResponse {
  const reader = createBufferReader(payload);
  const status = reader.readU8();
  if (status === 0) return { status: 0, reader };
  if (status !== 1) throw new RangeError(`Unknown Queue response status: ${status}`);
  return { status: 1, errorMessage: reader.readString() };
}

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
    const response = parseQueueResponse(payload);
    if (response.status !== 0) {
      return response;
    }
    const { reader } = response;

    let messageId: bigint | undefined;
    if (reader.remainingBytes() >= 8) {
      messageId = reader.readU64BE();
    }
    return { status: 0, messageId };
  },

  /**
   * Encode RESERVE request.
   * Payload: [route: string][lease_seconds: u64][has_batch_size: u8][batch_size: u32]
   * [has_wait_seconds: u8][wait_seconds: u64]
   *
   */
  encodeReserve(
    route: string,
    leaseSeconds: number,
    batchSize?: number,
    waitSeconds?: number,
  ): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const hasBatchSize = batchSize !== undefined && batchSize > 0 ? 1 : 0;
    const hasWaitSeconds = waitSeconds !== undefined && waitSeconds > 0 ? 1 : 0;
    const buffer = new Uint8Array(
      routeBytes.length + 8 + 1 + (hasBatchSize ? 4 : 0) + 1 + (hasWaitSeconds ? 8 : 0),
    );
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    offset = writeU64BENumberAt(buffer, offset, leaseSeconds);
    buffer[offset++] = hasBatchSize;
    if (hasBatchSize && batchSize !== undefined) {
      offset = writeU32BEAt(buffer, offset, batchSize);
    }
    buffer[offset++] = hasWaitSeconds;
    if (hasWaitSeconds && waitSeconds !== undefined)
      writeU64BENumberAt(buffer, offset, waitSeconds);

    return buffer;
  },

  /**
   * Decode RESERVE response.
   * Payload: [status: u8][lease_count: u32]([route: string][message_id: u64][lease_token: u64][body_len: u32][body: bytes] ...)
   */
  decodeReserveResponse(payload: Uint8Array, selector?: string): QueueReserveResponse {
    const response = parseQueueResponse(payload);
    if (response.status !== 0) {
      return response;
    }
    const { reader } = response;

    if (reader.isEOF() || reader.remainingBytes() < 4) {
      return { status: 0, items: [] };
    }

    const leaseCount = reader.readU32BE();
    const items: Array<{ route: string; id: bigint; token: bigint; body: Uint8Array }> = [];

    const wildcard = selector?.includes("*") === true;
    for (let i = 0; i < leaseCount; i++) {
      const route = wildcard ? reader.readRoute() : selector;
      if (!route)
        throw new QueueError("RESERVE response missing concrete route", "RESERVE_INVALID_RESPONSE");
      if (!isRouteShape(route, "queue", 3)) {
        throw new QueueError(
          `RESERVE response contains invalid concrete queue route: ${route}`,
          "RESERVE_INVALID_RESPONSE",
        );
      }
      if (wildcard && selector && !routeMatchesPattern(route, selector)) {
        throw new QueueError(
          `RESERVE response route does not match selector: ${route}`,
          "RESERVE_INVALID_RESPONSE",
        );
      }
      const id = reader.readU64BE();
      const token = reader.readU64BE();
      const bodyLen = reader.readU32BE();
      const body = reader.readBytes(bodyLen);

      items.push({ route, id, token, body });
    }

    if (!reader.isEOF())
      throw new QueueError("RESERVE response has trailing bytes", "RESERVE_INVALID_RESPONSE");
    return { status: 0, items };
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
    const response = parsePlainQueueResponse(payload);
    if (response.status !== 0) {
      return response;
    }

    return { status: 0 };
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
    const response = parsePlainQueueResponse(payload);
    if (response.status !== 0) {
      return response;
    }

    return { status: 0 };
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
    const response = parsePlainQueueResponse(payload);
    if (response.status !== 0) {
      return response;
    }
    const { reader } = response;

    const hasSubId = reader.readU8();
    if (hasSubId !== 1) throw new RangeError("Queue SUBSCRIBE response is missing subscription id");
    const subId = reader.readU64BE();
    if (!reader.isEOF()) throw new RangeError("Queue SUBSCRIBE response has trailing bytes");
    return { status: 0, subId };
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
    const response = parsePlainQueueResponse(payload);
    if (response.status !== 0) {
      return response;
    }

    return { status: 0 };
  },

  /**
   * Decode notification payload.
   * Payload: [sub_id: u64][route: string]
   */
  decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
    readyMessages: bigint;
    delayedMessages: bigint;
    inflightMessages: bigint;
  } {
    const reader = createBufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();
    const readyMessages = reader.readU64BE();
    const delayedMessages = reader.readU64BE();
    const inflightMessages = reader.readU64BE();
    if (!reader.isEOF()) {
      throw new RangeError("Queue notification has trailing bytes");
    }

    return { subId, route, readyMessages, delayedMessages, inflightMessages };
  },
};
