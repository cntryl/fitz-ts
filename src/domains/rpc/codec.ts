/**
 * RPC domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/rpc/protocol.go
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import { ProtocolError } from "../../core/errors";
import { InboundRequest, SubscribeResponse, UnsubscribeResponse } from "./types";

export class RpcCodec {
  /**
   * Generate a random 16-byte correlation ID
   * Per fitz-go rpc.go: crypto/rand.Read(correlationID[:])
   */
  static generateCorrelationId(): Uint8Array {
    const correlationId = new Uint8Array(16);
    const cryptoProvider = globalThis.crypto;
    if (!cryptoProvider?.getRandomValues) {
      throw new ProtocolError(
        "Cryptographic randomness is required for RPC correlation IDs",
        undefined,
        { operation: "RPC_GENERATE_CORRELATION_ID" },
      );
    }

    cryptoProvider.getRandomValues(correlationId);
    return correlationId;
  }

  /**
   * Encode RPC_REQUEST (302)
   * Payload: [bytes correlation_id][string route][string reply_route][bytes body]
   * Where bytes/string = [u32 BE len][data]
   */
  static encodeRequest(
    correlationId: Uint8Array,
    route: string,
    replyRoute: string,
    body: Uint8Array,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeU32BE(correlationId.length);
    writer.writeBytes(correlationId);
    writer.writeRoute(route);
    writer.writeRoute(replyRoute);
    writer.writeU32BE(body.length);
    writer.writeBytes(body);
    return writer.getBuffer();
  }

  /**
   * Decode RPC_REQUEST response (standard [u8 status][payload])
   * Just validates OK status; no additional payload expected
   */
  static decodeRequestResponse(payload: Uint8Array): { status: number } {
    if (payload.length === 0) {
      throw new ProtocolError("Empty REQUEST response", undefined, {
        operation: "RPC_REQUEST",
      });
    }
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode RPC_RESPONSE (303)
   * Payload: [bytes correlation_id][u64 sequence][bytes body][u8 stream_end]
   */
  static encodeResponse(
    correlationId: Uint8Array,
    sequence: bigint,
    body: Uint8Array,
    streamEnd: boolean,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeU32BE(correlationId.length);
    writer.writeBytes(correlationId);
    writer.writeU64BE(sequence);
    writer.writeU32BE(body.length);
    writer.writeBytes(body);
    writer.writeU8(streamEnd ? 1 : 0);
    return writer.getBuffer();
  }

  /**
   * Decode RPC_RESPONSE (303)
   * Payload: [bytes correlation_id][u64 sequence][bytes body][u8 stream_end]
   * Returns: { correlationId, sequence, body, streamEnd }
   */
  static decodeResponse(payload: Uint8Array): {
    correlationId: Uint8Array;
    sequence: bigint;
    body: Uint8Array;
    streamEnd: boolean;
  } {
    const reader = new BufferReader(payload);

    const corrLen = reader.readU32BE();
    if (corrLen !== 16) {
      throw new ProtocolError(`Invalid correlation ID length: ${corrLen}, expected 16`, undefined, {
        correlationLength: corrLen,
        expectedLength: 16,
      });
    }
    const correlationId = reader.readBytes(corrLen);

    const sequence = reader.readU64BE();

    const bodyLen = reader.readU32BE();
    const body = reader.readBytes(bodyLen);

    let streamEnd = false;
    if (!reader.isEOF()) {
      streamEnd = reader.readU8() === 1;
    }

    return { correlationId, sequence, body, streamEnd };
  }

  /**
   * Encode RPC_SUBSCRIBE_WORKER (304)
   * Payload: [string worker_route]
   */
  static encodeSubscribeWorker(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode SUBSCRIBE_WORKER response (standard [u8 status])
   */
  static decodeSubscribeWorkerResponse(payload: Uint8Array): SubscribeResponse {
    if (payload.length === 0) {
      throw new ProtocolError("Empty SUBSCRIBE_WORKER response", undefined, {
        operation: "RPC_SUBSCRIBE_WORKER",
      });
    }
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode RPC_UNSUBSCRIBE_WORKER (305)
   * Payload: [string worker_route]
   */
  static encodeUnsubscribeWorker(route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode UNSUBSCRIBE_WORKER response (standard [u8 status])
   */
  static decodeUnsubscribeWorkerResponse(payload: Uint8Array): UnsubscribeResponse {
    if (payload.length === 0) {
      throw new ProtocolError("Empty UNSUBSCRIBE_WORKER response", undefined, {
        operation: "RPC_UNSUBSCRIBE_WORKER",
      });
    }
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Decode incoming RPC_REQUEST (302) for worker mode
   * Payload: [u32 corrLen=16][16 bytes correlation_id][string route][string reply_route][bytes body]
   */
  static decodeInboundRequest(payload: Uint8Array): InboundRequest {
    const reader = new BufferReader(payload);

    const corrLen = reader.readU32BE();
    if (corrLen !== 16) {
      throw new ProtocolError(`Invalid correlation ID length: ${corrLen}, expected 16`, undefined, {
        correlationLength: corrLen,
        expectedLength: 16,
      });
    }
    const correlationId = reader.readBytes(corrLen);

    const route = reader.readRoute();
    const replyRoute = reader.readRoute();

    const bodyLen = reader.readU32BE();
    const body = reader.readBytes(bodyLen);

    return { correlationId, route, replyRoute, body };
  }
}
