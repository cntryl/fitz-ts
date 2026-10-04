/**
 * RPC domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/rpc/protocol.go
 */

import {
  createBufferReader,
  getRouteEncoding,
  readU128BEAt,
  readU32BEAt,
  utf8Decoder,
} from "../../core/buffer";
import { ProtocolError } from "../../core/errors";
import { SubscribeResponse, UnsubscribeResponse } from "./types";

const CORRELATION_ID_LENGTH = 16;
const RPC_LIFECYCLE_EXTENSION_VERSION = 1;
const RPC_WORKER_CANCELLATION_SUPPORTED = 0x01;
const RPC_REMAINING_BUDGET_PRESENT = 0x01;
export const MAX_RPC_BUDGET_MS = 86_400_000;
const RPC_RESPONSE_FLAG_STREAM_END = 0x01;
const RPC_RESPONSE_FLAGS_SUPPORTED = RPC_RESPONSE_FLAG_STREAM_END;
const RPC_ERROR_CODE_MIN = 6001;
const RPC_ERROR_CODE_MAX = 6013;
const getCryptoProvider = (): Crypto | undefined => globalThis.crypto as Crypto | undefined;
const correlationIdPool: Uint8Array[] = [];
const maxCorrelationIdPoolSize = 64;

const secureRandomValues = (buffer: Uint8Array): Uint8Array => {
  const provider = getCryptoProvider();
  if (!provider?.getRandomValues) {
    throw new ProtocolError(
      "Cryptographic randomness is required for RPC correlation IDs",
      undefined,
      { operation: "RPC_GENERATE_CORRELATION_ID" },
    );
  }

  provider.getRandomValues(buffer as unknown as ArrayBufferView<ArrayBuffer>);
  return buffer;
};

export const acquirePooledCorrelationId = (): Uint8Array => {
  const correlationId = correlationIdPool.pop() ?? new Uint8Array(CORRELATION_ID_LENGTH);
  return secureRandomValues(correlationId);
};

export const releasePooledCorrelationId = (correlationId: Uint8Array): void => {
  if (
    correlationId.length !== CORRELATION_ID_LENGTH ||
    correlationIdPool.length >= maxCorrelationIdPoolSize
  ) {
    return;
  }
  correlationIdPool.push(correlationId);
};

const readCorrelationKey = (payload: Uint8Array, offset: number): bigint => {
  return readU128BEAt(payload, offset);
};

const readU32BE = (payload: Uint8Array, offset: number): number | undefined => {
  if (offset + 4 > payload.length) {
    return undefined;
  }

  return readU32BEAt(payload, offset);
};

const readLengthPrefixedEnd = (payload: Uint8Array, offset: number): number | undefined => {
  const length = readU32BE(payload, offset);
  if (length === undefined) {
    return undefined;
  }

  const end = offset + 4 + length;
  return end > payload.length ? undefined : end;
};

const readRemainingBudgetExtension = (
  payload: Uint8Array,
  offset: number,
): { remainingBudgetMs?: number; nextOffset: number } | undefined => {
  if (offset === payload.length) {
    return { nextOffset: offset };
  }
  if (offset + 6 !== payload.length) {
    return undefined;
  }
  if (
    payload[offset] !== RPC_LIFECYCLE_EXTENSION_VERSION ||
    payload[offset + 1] !== RPC_REMAINING_BUDGET_PRESENT
  ) {
    return undefined;
  }
  const remainingBudgetMs = readU32BE(payload, offset + 2);
  if (remainingBudgetMs === undefined || remainingBudgetMs > MAX_RPC_BUDGET_MS) {
    return undefined;
  }
  return { remainingBudgetMs, nextOffset: payload.length };
};

const looksLikeInboundRequestPayload = (payload: Uint8Array): boolean => {
  let offset = CORRELATION_ID_LENGTH;
  if (payload.length < offset) {
    return false;
  }

  const routeEnd = readLengthPrefixedEnd(payload, offset);
  if (routeEnd === undefined) {
    return false;
  }
  offset = routeEnd;

  const bodyEnd = readLengthPrefixedEnd(payload, offset);
  return bodyEnd !== undefined && readRemainingBudgetExtension(payload, bodyEnd) !== undefined;
};

const looksLikeStreamResponsePayload = (payload: Uint8Array): boolean => {
  let offset = CORRELATION_ID_LENGTH + 8;
  if (payload.length < offset + 1) {
    return false;
  }

  const flags = payload[offset]!;
  if (flags & ~RPC_RESPONSE_FLAGS_SUPPORTED) {
    return false;
  }
  offset += 1;

  const bodyEnd = readLengthPrefixedEnd(payload, offset);
  return bodyEnd === payload.length;
};

const readStringFromPayload = (
  payload: Uint8Array,
  offset: number,
): { value: string; nextOffset: number } => {
  if (offset + 4 > payload.length) {
    throw new ProtocolError("Unexpected end of buffer while reading string length", undefined, {
      operation: "RPC_DECODE",
    });
  }

  const length = readU32BEAt(payload, offset);
  offset += 4;

  if (length === 0) {
    return { value: "", nextOffset: offset };
  }

  if (offset + length > payload.length) {
    throw new ProtocolError("Unexpected end of buffer while reading string contents", undefined, {
      operation: "RPC_DECODE",
    });
  }

  const value = utf8Decoder.decode(payload.subarray(offset, offset + length));
  return { value, nextOffset: offset + length };
};

const assertCorrelationIdLength = (correlationId: Uint8Array): void => {
  if (correlationId.length !== CORRELATION_ID_LENGTH) {
    throw new ProtocolError(
      `Invalid correlation ID length: ${correlationId.length}, expected 16`,
      undefined,
      {
        correlationLength: correlationId.length,
        expectedLength: CORRELATION_ID_LENGTH,
      },
    );
  }
};

export const RpcCodec = {
  isInboundRequestPayload(payload: Uint8Array): boolean {
    return looksLikeInboundRequestPayload(payload);
  },

  isStreamResponsePayload(payload: Uint8Array): boolean {
    return looksLikeStreamResponsePayload(payload);
  },

  /**
   * Generate a random 16-byte correlation ID
   * Per fitz-go rpc.go: crypto/rand.Read(correlationID[:])
   */
  generateCorrelationId(): Uint8Array {
    const correlationId = new Uint8Array(CORRELATION_ID_LENGTH);
    secureRandomValues(correlationId);
    return correlationId;
  },

  /**
   * Encode RPC_REQUEST (302)
   * Payload: [uuid16 correlation_id][string route][bytes body]
   */
  encodeRequest(
    correlationId: Uint8Array,
    route: string,
    body: Uint8Array,
    remainingBudgetMs?: number,
  ): Uint8Array {
    assertCorrelationIdLength(correlationId);
    validateRemainingBudget(remainingBudgetMs);
    const routeBytes = getRouteEncoding(route);
    const payloadLength =
      CORRELATION_ID_LENGTH +
      routeBytes.length +
      4 +
      body.length +
      (remainingBudgetMs === undefined ? 0 : 6);

    const buffer = new Uint8Array(payloadLength);
    let offset = 0;

    buffer.set(correlationId, offset);
    offset += CORRELATION_ID_LENGTH;
    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    buffer[offset++] = (body.length >> 24) & 0xff;
    buffer[offset++] = (body.length >> 16) & 0xff;
    buffer[offset++] = (body.length >> 8) & 0xff;
    buffer[offset++] = body.length & 0xff;
    buffer.set(body, offset);
    offset += body.length;
    if (remainingBudgetMs !== undefined) {
      buffer[offset++] = RPC_LIFECYCLE_EXTENSION_VERSION;
      buffer[offset++] = RPC_REMAINING_BUDGET_PRESENT;
      buffer[offset++] = (remainingBudgetMs >>> 24) & 0xff;
      buffer[offset++] = (remainingBudgetMs >>> 16) & 0xff;
      buffer[offset++] = (remainingBudgetMs >>> 8) & 0xff;
      buffer[offset] = remainingBudgetMs & 0xff;
    }

    return buffer;
  },

  encodeCallRequest(
    correlationId: Uint8Array,
    route: string,
    body: Uint8Array,
    remainingBudgetMs?: number,
  ): Uint8Array {
    return this.encodeRequest(correlationId, route, body, remainingBudgetMs);
  },

  /**
   * Decode RPC_REQUEST response (standard [u8 status][payload])
   * Just validates OK status; no additional payload expected
   */
  decodeRequestResponse(payload: Uint8Array): { status: number } {
    if (payload.length === 0) {
      throw new ProtocolError("Empty REQUEST response", undefined, {
        operation: "RPC_REQUEST",
      });
    }
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    return { status };
  },

  /**
   * Encode RPC_RESPONSE (303)
   * Payload: [uuid16 correlation_id][u64 sequence][u8 flags][bytes body]
   */
  encodeResponse(
    correlationId: Uint8Array,
    sequence: bigint,
    body: Uint8Array,
    streamEnd: boolean,
  ): Uint8Array {
    assertCorrelationIdLength(correlationId);
    const payloadLength = CORRELATION_ID_LENGTH + 8 + 1 + 4 + body.length;
    const buffer = new Uint8Array(payloadLength);
    let offset = 0;

    buffer.set(correlationId, offset);
    offset += CORRELATION_ID_LENGTH;
    buffer[offset++] = Number((sequence >> 56n) & 0xffn);
    buffer[offset++] = Number((sequence >> 48n) & 0xffn);
    buffer[offset++] = Number((sequence >> 40n) & 0xffn);
    buffer[offset++] = Number((sequence >> 32n) & 0xffn);
    buffer[offset++] = Number((sequence >> 24n) & 0xffn);
    buffer[offset++] = Number((sequence >> 16n) & 0xffn);
    buffer[offset++] = Number((sequence >> 8n) & 0xffn);
    buffer[offset++] = Number(sequence & 0xffn);
    buffer[offset++] = streamEnd ? RPC_RESPONSE_FLAG_STREAM_END : 0;
    buffer[offset++] = (body.length >> 24) & 0xff;
    buffer[offset++] = (body.length >> 16) & 0xff;
    buffer[offset++] = (body.length >> 8) & 0xff;
    buffer[offset++] = body.length & 0xff;
    buffer.set(body, offset);

    return buffer;
  },

  decodeResponse(payload: Uint8Array): {
    correlationId: Uint8Array;
    sequence: bigint;
    body: Uint8Array;
    streamEnd: boolean;
  } {
    if (payload.length < CORRELATION_ID_LENGTH + 8 + 1 + 4) {
      throw new ProtocolError("Invalid RPC response payload length", undefined, {
        operation: "RPC_DECODE_RESPONSE",
      });
    }

    const correlationId = payload.subarray(0, CORRELATION_ID_LENGTH);
    let offset = CORRELATION_ID_LENGTH;

    const sequence =
      (BigInt(readU32BEAt(payload, offset)) << 32n) | BigInt(readU32BEAt(payload, offset + 4));
    offset += 8;

    const flags = payload[offset]!;
    offset += 1;
    if (flags & ~RPC_RESPONSE_FLAGS_SUPPORTED) {
      throw new ProtocolError(
        `Unsupported RPC response flags: 0x${flags.toString(16)}`,
        undefined,
        {
          operation: "RPC_DECODE_RESPONSE",
          flags,
        },
      );
    }

    const bodyLen = readU32BEAt(payload, offset);
    offset += 4;
    if (offset + bodyLen > payload.length) {
      throw new ProtocolError("Invalid RPC response body length", undefined, {
        operation: "RPC_DECODE_RESPONSE",
      });
    }

    const body = payload.subarray(offset, offset + bodyLen);
    offset += bodyLen;

    const streamEnd = (flags & RPC_RESPONSE_FLAG_STREAM_END) !== 0;

    if (offset !== payload.length) {
      throw new ProtocolError("Invalid RPC response payload structure", undefined, {
        operation: "RPC_DECODE_RESPONSE",
      });
    }

    return { correlationId, sequence, body, streamEnd };
  },

  decodeResponseKey(payload: Uint8Array): {
    correlationKey: bigint;
    sequence: bigint;
    body: Uint8Array;
    streamEnd: boolean;
  } {
    if (payload.length < CORRELATION_ID_LENGTH + 8 + 1 + 4) {
      throw new ProtocolError("Invalid RPC response payload length", undefined, {
        operation: "RPC_DECODE_RESPONSE_KEY",
      });
    }

    const correlationIdOffset = 0;
    const correlationKey = readCorrelationKey(payload, correlationIdOffset);
    let offset = correlationIdOffset + CORRELATION_ID_LENGTH;

    const sequence =
      (BigInt(readU32BEAt(payload, offset)) << 32n) | BigInt(readU32BEAt(payload, offset + 4));
    offset += 8;

    const flags = payload[offset]!;
    offset += 1;
    if (flags & ~RPC_RESPONSE_FLAGS_SUPPORTED) {
      throw new ProtocolError(
        `Unsupported RPC response flags: 0x${flags.toString(16)}`,
        undefined,
        {
          operation: "RPC_DECODE_RESPONSE_KEY",
          flags,
        },
      );
    }

    const bodyLen = readU32BEAt(payload, offset);
    offset += 4;
    if (offset + bodyLen > payload.length) {
      throw new ProtocolError("Invalid RPC response body length", undefined, {
        operation: "RPC_DECODE_RESPONSE_KEY",
      });
    }

    const body = payload.subarray(offset, offset + bodyLen);
    offset += bodyLen;

    const streamEnd = (flags & RPC_RESPONSE_FLAG_STREAM_END) !== 0;

    if (offset !== payload.length) {
      throw new ProtocolError("Invalid RPC response payload structure", undefined, {
        operation: "RPC_DECODE_RESPONSE_KEY",
      });
    }

    return { correlationKey, sequence, body, streamEnd };
  },

  /**
   * Encode RPC_SUBSCRIBE_WORKER (300)
   * Payload: [string worker_route][u32 max_concurrent]
   */
  encodeSubscribeWorker(
    route: string,
    maxConcurrent: number,
    supportsCancellation = false,
  ): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const buffer = new Uint8Array(routeBytes.length + 4 + (supportsCancellation ? 2 : 0));
    buffer.set(routeBytes, 0);
    let offset = routeBytes.length;
    buffer[offset++] = (maxConcurrent >> 24) & 0xff;
    buffer[offset++] = (maxConcurrent >> 16) & 0xff;
    buffer[offset++] = (maxConcurrent >> 8) & 0xff;
    buffer[offset] = maxConcurrent & 0xff;
    if (supportsCancellation) {
      buffer[offset + 1] = RPC_LIFECYCLE_EXTENSION_VERSION;
      buffer[offset + 2] = RPC_WORKER_CANCELLATION_SUPPORTED;
    }
    return buffer;
  },

  /**
   * Decode SUBSCRIBE_WORKER response (standard [u8 status])
   */
  decodeSubscribeWorkerResponse(payload: Uint8Array): SubscribeResponse {
    if (payload.length === 0) {
      throw new ProtocolError("Empty SUBSCRIBE_WORKER response", undefined, {
        operation: "RPC_SUBSCRIBE_WORKER",
      });
    }
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    return { status };
  },

  /**
   * Encode RPC_UNSUBSCRIBE_WORKER (301)
   * Payload: [string worker_route]
   */
  encodeUnsubscribeWorker(route: string): Uint8Array {
    return getRouteEncoding(route).slice();
  },

  /**
   * Decode UNSUBSCRIBE_WORKER response (standard [u8 status])
   */
  decodeUnsubscribeWorkerResponse(payload: Uint8Array): UnsubscribeResponse {
    if (payload.length === 0) {
      throw new ProtocolError("Empty UNSUBSCRIBE_WORKER response", undefined, {
        operation: "RPC_UNSUBSCRIBE_WORKER",
      });
    }
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    return { status };
  },

  tryDecodeTerminalErrorBody(payload: Uint8Array): { code: number; message: string } | null {
    if (payload.length < 5) {
      return null;
    }

    try {
      const reader = createBufferReader(payload);
      if (reader.readU8() !== 1) {
        return null;
      }

      const code = reader.readU32BE();
      if (code < RPC_ERROR_CODE_MIN || code > RPC_ERROR_CODE_MAX) {
        return null;
      }

      const message = reader.readString();
      if (!reader.isEOF()) {
        return null;
      }

      return { code, message };
    } catch {
      return null;
    }
  },

  /**
   * Decode a standard RPC error body.
   * Format: [u8 status=1][u32 error_code][string message]
   */
  decodeErrorBody(payload: Uint8Array): { code: number; message: string } | null {
    return this.tryDecodeTerminalErrorBody(payload);
  },

  /**
   * Decode incoming RPC_REQUEST (302) for worker mode
   * Payload: [uuid16 correlation_id][string route][bytes body]
   */
  decodeInboundRequest(payload: Uint8Array): {
    correlationId: Uint8Array;
    route: string;
    body: Uint8Array;
    remainingBudgetMs?: number;
  } {
    let offset = 0;
    if (offset + CORRELATION_ID_LENGTH > payload.length) {
      throw new ProtocolError("Invalid RPC request payload length", undefined, {
        operation: "RPC_DECODE_INBOUND_REQUEST",
      });
    }

    const correlationId = payload.subarray(offset, offset + CORRELATION_ID_LENGTH);
    offset += CORRELATION_ID_LENGTH;

    const routeResult = readStringFromPayload(payload, offset);
    const route = routeResult.value;
    offset = routeResult.nextOffset;

    if (offset + 4 > payload.length) {
      throw new ProtocolError("Invalid RPC request body length", undefined, {
        operation: "RPC_DECODE_INBOUND_REQUEST",
      });
    }

    const bodyLen = readU32BEAt(payload, offset);
    offset += 4;
    if (offset + bodyLen > payload.length) {
      throw new ProtocolError("Invalid RPC request body length", undefined, {
        operation: "RPC_DECODE_INBOUND_REQUEST",
      });
    }
    const body = payload.subarray(offset, offset + bodyLen);
    offset += bodyLen;

    const extension = readRemainingBudgetExtension(payload, offset);
    if (!extension || extension.nextOffset !== payload.length) {
      throw new ProtocolError("Invalid RPC request payload structure", undefined, {
        operation: "RPC_DECODE_INBOUND_REQUEST",
      });
    }

    return { correlationId, route, body, remainingBudgetMs: extension.remainingBudgetMs };
  },

  encodeCallerCancellation(correlationId: Uint8Array, reason: 1 | 2): Uint8Array {
    assertCorrelationIdLength(correlationId);
    const payload = new Uint8Array(1 + CORRELATION_ID_LENGTH + 1);
    payload[0] = 1;
    payload.set(correlationId, 1);
    payload[payload.length - 1] = reason;
    return payload;
  },

  encodeWorkerCleanupAck(correlationId: Uint8Array): Uint8Array {
    assertCorrelationIdLength(correlationId);
    const payload = new Uint8Array(1 + CORRELATION_ID_LENGTH);
    payload[0] = 3;
    payload.set(correlationId, 1);
    return payload;
  },

  isLifecycleControlPayload(payload: Uint8Array): boolean {
    if (payload.length !== 18 || (payload[0] !== 2 && payload[0] !== 4)) {
      return false;
    }
    if (payload[0] === 2) {
      return payload[17]! >= 1 && payload[17]! <= 4;
    }
    return payload[17]! >= 1 && payload[17]! <= 6;
  },

  decodeLifecycleControl(payload: Uint8Array):
    | { kind: "worker_cancellation"; correlationId: Uint8Array; reason: 1 | 2 | 3 | 4 }
    | {
        kind: "cancellation_result";
        correlationId: Uint8Array;
        status: 1 | 2 | 3 | 4 | 5 | 6;
      } {
    if (!this.isLifecycleControlPayload(payload)) {
      throw new ProtocolError("Invalid RPC lifecycle control", undefined, {
        operation: "RPC_DECODE_LIFECYCLE_CONTROL",
      });
    }
    const correlationId = payload.subarray(1, 1 + CORRELATION_ID_LENGTH);
    const value = payload[payload.length - 1]!;
    if (payload[0] === 2) {
      return {
        kind: "worker_cancellation",
        correlationId,
        reason: value as 1 | 2 | 3 | 4,
      };
    }
    return {
      kind: "cancellation_result",
      correlationId,
      status: value as 1 | 2 | 3 | 4 | 5 | 6,
    };
  },
};

function validateRemainingBudget(remainingBudgetMs: number | undefined): void {
  if (
    remainingBudgetMs !== undefined &&
    (!Number.isInteger(remainingBudgetMs) ||
      remainingBudgetMs < 0 ||
      remainingBudgetMs > MAX_RPC_BUDGET_MS)
  ) {
    throw new ProtocolError(
      `RPC remaining budget must be an integer in 0..=${MAX_RPC_BUDGET_MS}`,
      undefined,
      {
        operation: "RPC_ENCODE_REQUEST",
        remainingBudgetMs,
      },
    );
  }
}
