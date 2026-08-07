/**
 * Lease domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/lease/protocol.go
 */

import {
  createBufferReader,
  getRouteEncoding,
  writeU64BEAt,
  writeU64BENumberAt,
  writeU32BEAt,
} from "../../core/buffer";
import { LeaseError, ProtocolError } from "../../core/errors";
import { AcquireResponse, QueryResponse } from "./types";

export const LeaseCodec = {
  /**
   * Encode ACQUIRE request
   * Payload: [string route][string client_id (empty)][u64 ttl_seconds][u32 wait_seconds]
   */
  encodeAcquire(route: string, ttlSecs: number, waitSeconds = 0): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const emptyClientIdBytes = getRouteEncoding("");
    const buffer = new Uint8Array(routeBytes.length + emptyClientIdBytes.length + 12);
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    buffer.set(emptyClientIdBytes, offset);
    offset += emptyClientIdBytes.length;
    offset = writeU64BENumberAt(buffer, offset, ttlSecs);
    writeU32BEAt(buffer, offset, waitSeconds);
    return buffer;
  },

  /**
   * Decode ACQUIRE response
   * Standard response: [u8 status=0][u8 response_type][u64 fencing_token]
   * response_type: 0=Acquired, 1=AlreadyHeld (idempotent)
   */
  decodeAcquireResponse(payload: Uint8Array): AcquireResponse {
    if (payload.length < 1) {
      throw new ProtocolError(
        `ACQUIRE response too short: got ${payload.length} bytes`,
        undefined,
        { operation: "LEASE_ACQUIRE", payloadLength: payload.length },
      );
    }

    const reader = createBufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      const message = reader.remainingBytes() >= 4 ? reader.readString() : `status ${status}`;
      if (!reader.isEOF())
        throw new ProtocolError("ACQUIRE error response has trailing data", status);
      throw new LeaseError(`ACQUIRE failed: ${message}`, "ACQUIRE_FAILED", status);
    }
    if (reader.remainingBytes() < 9)
      throw new ProtocolError("ACQUIRE success response is truncated", undefined, {
        operation: "LEASE_ACQUIRE",
      });
    const responseType = reader.readU8();
    if (responseType !== 0 && responseType !== 1 && responseType !== 2 && responseType !== 3) {
      throw new ProtocolError(`Unknown ACQUIRE response type ${responseType}`, undefined, {
        operation: "LEASE_ACQUIRE",
        responseType,
      });
    }
    const fencingToken = reader.readU64BE();
    if (!reader.isEOF())
      throw new ProtocolError("ACQUIRE success response has trailing data", undefined, {
        operation: "LEASE_ACQUIRE",
      });

    // response_type: 0=Acquired, 1=AlreadyHeld
    // For now, treat both as success
    return { token: fencingToken, responseType };
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
    const reader = createBufferReader(payload);
    const status = reader.readU8();
    if (status !== 0) {
      const errorMessage = reader.readString();
      if (!reader.isEOF()) {
        throw new ProtocolError("QUERY error response has trailing data", status);
      }
      return { status, errorMessage };
    }
    const hasHolder = reader.readU8();

    if (hasHolder === 0) {
      // Free
      const pendingWaiters = reader.readU32BE();
      if (!reader.isEOF()) throw new ProtocolError("QUERY response has trailing data");
      return { status, isHeld: false, pendingWaiters };
    }

    // Held
    const owner = reader.readRoute();
    const ttlRemainingSecs = reader.readU64BE();
    const pendingWaiters = reader.readU32BE();
    if (!reader.isEOF()) throw new ProtocolError("QUERY response has trailing data");

    // Note: token not returned in QUERY response
    return {
      status,
      isHeld: true,
      owner,
      ttlRemainingSecs,
      pendingWaiters,
      expiresAt: BigInt(Math.floor(Date.now() / 1000)) + ttlRemainingSecs,
    };
  },

  /**
   * Encode SUBSCRIBE request
   * Payload: [string route]
   */
  encodeSubscribe(route: string): Uint8Array {
    return getRouteEncoding(route).slice();
  },

  /**
   * Encode UNSUBSCRIBE request
   * Payload: [string route]
   */
  encodeUnsubscribe(route: string): Uint8Array {
    return getRouteEncoding(route).slice();
  },

  /**
   * Decode NOTIFY (409) message
   * Payload: [u64 subscription_id][string route][bytes payload]
   */
  decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
  } {
    const reader = createBufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();
    const notificationPayload = reader.readBytes(reader.readU32BE());
    if (notificationPayload.length !== 0 || !reader.isEOF()) {
      throw new ProtocolError("LEASE_NOTIFY payload must be empty", undefined, {
        operation: "LEASE_NOTIFY",
      });
    }

    return { subId, route };
  },
};
