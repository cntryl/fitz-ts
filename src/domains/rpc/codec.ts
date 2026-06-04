/**
 * RPC domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/rpc/protocol.go
 */

import {
  BufferReader,
  getRouteEncoding,
  readU128BEAt,
  readU32BEAt,
  utf8Decoder,
} from "../../core/buffer";
import { ProtocolError } from "../../core/errors";
import { SubscribeResponse, UnsubscribeResponse } from "./types";

const CORRELATION_ID_LENGTH = 16;
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

export const RpcCodec = {
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
   * Payload: [bytes correlation_id][string route][string reply_route][bytes body]
   * Where bytes/string = [u32 BE len][data]
   */
  encodeRequest(
    correlationId: Uint8Array,
    route: string,
    replyRoute: string,
    body: Uint8Array,
  ): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const replyRouteBytes = getRouteEncoding(replyRoute);
    const payloadLength =
      4 + CORRELATION_ID_LENGTH + routeBytes.length + replyRouteBytes.length + 4 + body.length;

    const buffer = new Uint8Array(payloadLength);
    let offset = 0;

    buffer[offset++] = 0;
    buffer[offset++] = 0;
    buffer[offset++] = 0;
    buffer[offset++] = CORRELATION_ID_LENGTH;
    buffer.set(correlationId, offset);
    offset += CORRELATION_ID_LENGTH;
    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    buffer.set(replyRouteBytes, offset);
    offset += replyRouteBytes.length;
    buffer[offset++] = (body.length >> 24) & 0xff;
    buffer[offset++] = (body.length >> 16) & 0xff;
    buffer[offset++] = (body.length >> 8) & 0xff;
    buffer[offset++] = body.length & 0xff;
    buffer.set(body, offset);

    return buffer;
  },

  encodeCallRequest(correlationId: Uint8Array, route: string, body: Uint8Array): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const payloadLength = 4 + CORRELATION_ID_LENGTH + routeBytes.length + 4 + 4 + body.length;

    const buffer = new Uint8Array(payloadLength);
    let offset = 0;

    buffer[offset++] = 0;
    buffer[offset++] = 0;
    buffer[offset++] = 0;
    buffer[offset++] = CORRELATION_ID_LENGTH;
    buffer.set(correlationId, offset);
    offset += CORRELATION_ID_LENGTH;
    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    buffer[offset++] = 0;
    buffer[offset++] = 0;
    buffer[offset++] = 0;
    buffer[offset++] = 0;
    buffer[offset++] = (body.length >> 24) & 0xff;
    buffer[offset++] = (body.length >> 16) & 0xff;
    buffer[offset++] = (body.length >> 8) & 0xff;
    buffer[offset++] = body.length & 0xff;
    buffer.set(body, offset);

    return buffer;
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
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  },

  /**
   * Encode RPC_RESPONSE (303)
   * Payload: [bytes correlation_id][u64 sequence][bytes body][u8 stream_end]
   */
  encodeResponse(
    correlationId: Uint8Array,
    sequence: bigint,
    body: Uint8Array,
    streamEnd: boolean,
  ): Uint8Array {
    const payloadLength = 4 + CORRELATION_ID_LENGTH + 8 + 4 + body.length + 1;
    const buffer = new Uint8Array(payloadLength);
    let offset = 0;

    buffer[offset++] = 0;
    buffer[offset++] = 0;
    buffer[offset++] = 0;
    buffer[offset++] = CORRELATION_ID_LENGTH;
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
    buffer[offset++] = (body.length >> 24) & 0xff;
    buffer[offset++] = (body.length >> 16) & 0xff;
    buffer[offset++] = (body.length >> 8) & 0xff;
    buffer[offset++] = body.length & 0xff;
    buffer.set(body, offset);
    offset += body.length;
    buffer[offset] = streamEnd ? 1 : 0;

    return buffer;
  },

  decodeResponse(payload: Uint8Array): {
    correlationId: Uint8Array;
    sequence: bigint;
    body: Uint8Array;
    streamEnd: boolean;
  } {
    if (payload.length < 4 + CORRELATION_ID_LENGTH + 8 + 4) {
      throw new ProtocolError("Invalid RPC response payload length", undefined, {
        operation: "RPC_DECODE_RESPONSE",
      });
    }

    const corrLen = readU32BEAt(payload, 0);
    if (corrLen !== CORRELATION_ID_LENGTH) {
      throw new ProtocolError(`Invalid correlation ID length: ${corrLen}, expected 16`, undefined, {
        correlationLength: corrLen,
        expectedLength: CORRELATION_ID_LENGTH,
      });
    }

    const correlationId = payload.subarray(4, 4 + corrLen);
    let offset = 4 + corrLen;

    const sequence =
      (BigInt(readU32BEAt(payload, offset)) << 32n) | BigInt(readU32BEAt(payload, offset + 4));
    offset += 8;

    const bodyLen = readU32BEAt(payload, offset);
    offset += 4;
    if (offset + bodyLen > payload.length) {
      throw new ProtocolError("Invalid RPC response body length", undefined, {
        operation: "RPC_DECODE_RESPONSE",
      });
    }

    const body = payload.subarray(offset, offset + bodyLen);
    offset += bodyLen;

    let streamEnd = false;
    if (offset < payload.length) {
      streamEnd = payload[offset] === 1;
      offset += 1;
    }

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
    if (payload.length < 4 + CORRELATION_ID_LENGTH + 8 + 4) {
      throw new ProtocolError("Invalid RPC response payload length", undefined, {
        operation: "RPC_DECODE_RESPONSE_KEY",
      });
    }

    const corrLen = readU32BEAt(payload, 0);
    if (corrLen !== CORRELATION_ID_LENGTH) {
      throw new ProtocolError(`Invalid correlation ID length: ${corrLen}, expected 16`, undefined, {
        correlationLength: corrLen,
        expectedLength: CORRELATION_ID_LENGTH,
      });
    }

    const correlationIdOffset = 4;
    const correlationKey = readCorrelationKey(payload, correlationIdOffset);
    let offset = correlationIdOffset + corrLen;

    const sequence =
      (BigInt(readU32BEAt(payload, offset)) << 32n) | BigInt(readU32BEAt(payload, offset + 4));
    offset += 8;

    const bodyLen = readU32BEAt(payload, offset);
    offset += 4;
    if (offset + bodyLen > payload.length) {
      throw new ProtocolError("Invalid RPC response body length", undefined, {
        operation: "RPC_DECODE_RESPONSE_KEY",
      });
    }

    const body = payload.subarray(offset, offset + bodyLen);
    offset += bodyLen;

    let streamEnd = false;
    if (offset < payload.length) {
      streamEnd = payload[offset] === 1;
      offset += 1;
    }

    if (offset !== payload.length) {
      throw new ProtocolError("Invalid RPC response payload structure", undefined, {
        operation: "RPC_DECODE_RESPONSE_KEY",
      });
    }

    return { correlationKey, sequence, body, streamEnd };
  },

  /**
   * Encode RPC_SUBSCRIBE_WORKER (304)
   * Payload: [string worker_route]
   */
  encodeSubscribeWorker(route: string): Uint8Array {
    return getRouteEncoding(route).slice();
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
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  },

  /**
   * Encode RPC_UNSUBSCRIBE_WORKER (305)
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
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  },

  /**
   * Decode a standard RPC error body.
   * Format: [u8 status=1][u32 error_code][string message]
   */
  decodeErrorBody(payload: Uint8Array): { code: number; message: string } | null {
    if (payload.length < 5) {
      return null;
    }

    const reader = new BufferReader(payload);
    if (reader.readU8() !== 1) {
      return null;
    }

    const code = reader.readU32BE();
    const message = reader.readString();
    if (!reader.isEOF()) {
      return null;
    }

    return { code, message };
  },

  /**
   * Decode incoming RPC_REQUEST (302) for worker mode
   * Payload: [u32 corrLen=16][16 bytes correlation_id][string route][string reply_route][bytes body]
   */
  decodeInboundRequest(payload: Uint8Array): {
    correlationId: Uint8Array;
    route: string;
    replyRoute: string;
    body: Uint8Array;
  } {
    let offset = 0;
    if (offset + 4 > payload.length) {
      throw new ProtocolError("Invalid RPC request payload length", undefined, {
        operation: "RPC_DECODE_INBOUND_REQUEST",
      });
    }

    const corrLen = readU32BEAt(payload, offset);
    offset += 4;
    if (corrLen !== CORRELATION_ID_LENGTH) {
      throw new ProtocolError(`Invalid correlation ID length: ${corrLen}, expected 16`, undefined, {
        correlationLength: corrLen,
        expectedLength: CORRELATION_ID_LENGTH,
      });
    }

    if (offset + corrLen > payload.length) {
      throw new ProtocolError("Invalid RPC request payload length", undefined, {
        operation: "RPC_DECODE_INBOUND_REQUEST",
      });
    }
    const correlationId = payload.subarray(offset, offset + corrLen);
    offset += corrLen;

    const routeResult = readStringFromPayload(payload, offset);
    const route = routeResult.value;
    offset = routeResult.nextOffset;

    const replyRouteResult = readStringFromPayload(payload, offset);
    const replyRoute = replyRouteResult.value;
    offset = replyRouteResult.nextOffset;

    const bodyLen = readU32BEAt(payload, offset);
    offset += 4;
    if (offset + bodyLen > payload.length) {
      throw new ProtocolError("Invalid RPC request body length", undefined, {
        operation: "RPC_DECODE_INBOUND_REQUEST",
      });
    }
    const body = payload.subarray(offset, offset + bodyLen);

    return { correlationId, route, replyRoute, body };
  },
};
