/**
 * Lease domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/lease/protocol.go
 */

import {
  BufferReader,
  getRouteEncoding,
  writeU64BEAt,
  writeU64BENumberAt,
} from "../../core/buffer";
import { ProtocolError } from "../../core/errors";
import { AcquireResponse, QueryResponse, SubscribeResponse, UnsubscribeResponse } from "./types";

export const LeaseCodec = {
  /**
   * Encode ACQUIRE request
   * Payload: [string route][string client_id (empty)][u64 ttl_seconds]
   */
  encodeAcquire(route: string, ttlSecs: number): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const emptyClientIdBytes = getRouteEncoding("");
    const buffer = new Uint8Array(routeBytes.length + emptyClientIdBytes.length + 8);
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    buffer.set(emptyClientIdBytes, offset);
    offset += emptyClientIdBytes.length;
    writeU64BENumberAt(buffer, offset, ttlSecs);
    return buffer;
  },

  /**
   * Decode ACQUIRE response
   * Standard response: [u8 status=0][u8 response_type][u64 fencing_token]
   * response_type: 0=Acquired, 1=AlreadyHeld (idempotent)
   */
  decodeAcquireResponse(payload: Uint8Array): AcquireResponse {
    if (payload.length < 10) {
      throw new ProtocolError(
        `ACQUIRE response too short: got ${payload.length} bytes, expected >= 10`,
        undefined,
        { operation: "LEASE_ACQUIRE", payloadLength: payload.length },
      );
    }

    const reader = new BufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      throw new ProtocolError(`ACQUIRE failed with status ${status}`, status, {
        operation: "LEASE_ACQUIRE",
        status,
      });
    }
    reader.readU8(); // responseType: 0=Acquired, 1=AlreadyHeld
    const fencingToken = reader.readU64BE();

    // response_type: 0=Acquired, 1=AlreadyHeld
    // For now, treat both as success
    return { token: fencingToken };
  },

  /**
   * Encode EXTEND request
   * Payload: [string route][string client_id (empty)][u64 fencing_token][u64 ttl_seconds]
   */
  encodeExtend(route: string, token: bigint, ttlSecs: number): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const emptyClientIdBytes = getRouteEncoding("");
    const buffer = new Uint8Array(routeBytes.length + emptyClientIdBytes.length + 16);
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    buffer.set(emptyClientIdBytes, offset);
    offset += emptyClientIdBytes.length;
    offset = writeU64BEAt(buffer, offset, token);
    writeU64BENumberAt(buffer, offset, ttlSecs);
    return buffer;
  },

  encodeRenew(route: string, token: bigint, ttlSecs: number): Uint8Array {
    return this.encodeExtend(route, token, ttlSecs);
  },

  /**
   * Encode RELEASE request
   * Payload: [string route][string client_id (empty)][u64 fencing_token]
   */
  encodeRelease(route: string, token: bigint): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const emptyClientIdBytes = getRouteEncoding("");
    const buffer = new Uint8Array(routeBytes.length + emptyClientIdBytes.length + 8);
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    buffer.set(emptyClientIdBytes, offset);
    offset += emptyClientIdBytes.length;
    writeU64BEAt(buffer, offset, token);
    return buffer;
  },

  /**
   * Encode QUERY request
   * Payload: [string route]
   */
  encodeQuery(route: string): Uint8Array {
    return getRouteEncoding(route).slice();
  },

  /**
   * Decode QUERY response
   * Free: [u8 has_holder=0][u32 pending_waiters]
   * Held: [u8 has_holder=1][string owner_id][u64 ttl_remaining_secs][u32 pending_waiters]
   */
  decodeQueryResponse(payload: Uint8Array): QueryResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      return { status };
    }
    const hasHolder = reader.readU8();

    if (hasHolder === 0) {
      // Free
      reader.readU32BE(); // pendingWaiters
      return { status, isHeld: false };
    }

    // Held
    const owner = reader.readRoute();
    const ttlRemainingSecs = reader.readU64BE();
    reader.readU32BE(); // pendingWaiters

    // Note: token not returned in QUERY response
    return {
      status,
      isHeld: true,
      owner,
      ttlRemainingSecs,
      expiresAt: BigInt(Math.floor(Date.now() / 1000)) + ttlRemainingSecs,
    };
  },

  /**
   * Encode SUBSCRIBE request
   * Payload: [string pattern]
   */
  encodeSubscribe(pattern: string): Uint8Array {
    return getRouteEncoding(pattern).slice();
  },

  /**
   * Decode SUBSCRIBE response
   * Standard response: [u8 status=0][u64 subscription_id]
   */
  decodeSubscribeResponse(payload: Uint8Array): SubscribeResponse {
    if (payload.length < 9) {
      throw new ProtocolError(
        `SUBSCRIBE response too short: got ${payload.length} bytes, expected >= 9`,
        undefined,
        { operation: "LEASE_SUBSCRIBE", payloadLength: payload.length },
      );
    }

    const reader = new BufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      return { status };
    }
    const subId = reader.readU64BE();

    return { status, subId };
  },

  /**
   * Encode UNSUBSCRIBE request
   * Payload: [string pattern]
   */
  encodeUnsubscribe(pattern: string): Uint8Array {
    return getRouteEncoding(pattern).slice();
  },

  /**
   * Decode UNSUBSCRIBE response
   * Standard response: [u8 status=0]
   */
  decodeUnsubscribeResponse(payload: Uint8Array): UnsubscribeResponse {
    if (payload.length === 0) {
      return { status: 0 };
    }

    const reader = new BufferReader(payload);
    return { status: reader.readU8() };
  },

  /**
   * Decode NOTIFY (409) message
   * Payload: [u64 subscription_id][string route][bytes payload]
   */
  decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
  } {
    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();

    return { subId, route };
  },
};
