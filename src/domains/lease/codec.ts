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
import { parseStandardResponse } from "../../protocol/response";
import {
  AcquireResponse,
  LeaseListCursor,
  LeaseListItem,
  LeaseListPage,
  QueryResponse,
} from "./types";

const U32_MAX = 0xffff_ffff;

function assertU32(value: number, label: string): void {
  if (!Number.isInteger(value) || value < 0 || value > U32_MAX) {
    throw new LeaseError(
      `${label} must be a safe non-negative integer representable in an unsigned 32-bit range (0..=${U32_MAX}), got ${value}`,
      "INVALID_LIST_ARGUMENT",
    );
  }
}

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
    if (payload.length === 0) {
      throw new ProtocolError("ACQUIRE response too short: got 0 bytes", undefined, {
        operation: "LEASE_ACQUIRE",
        payloadLength: 0,
      });
    }
    const parsed = parseStandardResponse(payload);
    if (!parsed.success) {
      throw new LeaseError(
        `ACQUIRE failed: ${parsed.error ?? "unknown error"}`,
        "ACQUIRE_FAILED",
        parsed.errorCode,
      );
    }
    const reader = createBufferReader(parsed.data);
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
    const parsed = parseStandardResponse(payload);
    if (!parsed.success) {
      return { status: 1, errorMessage: parsed.error, errorCode: parsed.errorCode };
    }
    const status = 0;
    const reader = createBufferReader(parsed.data);
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
   * Encode LIST request (msg_type 410)
   * Payload: [u32 pattern_len][bytes pattern][u8 has_cursor]
   *   [u64 snapshot_id][u32 offset] if has_cursor==1
   *   [u32 limit] (0 = server default page size; server clamps to its max)
   */
  encodeList(pattern: string, options?: { cursor?: LeaseListCursor; limit?: number }): Uint8Array {
    const patternBytes = getRouteEncoding(pattern);
    const cursor = options?.cursor;
    const limit = options?.limit ?? 0;
    assertU32(limit, "limit");
    if (cursor) assertU32(cursor.offset, "cursor.offset");
    const cursorLength = cursor ? 12 : 0;
    const buffer = new Uint8Array(patternBytes.length + 1 + cursorLength + 4);
    let offset = 0;

    buffer.set(patternBytes, offset);
    offset += patternBytes.length;
    buffer[offset++] = cursor ? 1 : 0;
    if (cursor) {
      offset = writeU64BEAt(buffer, offset, cursor.snapshotId);
      offset = writeU32BEAt(buffer, offset, cursor.offset);
    }
    writeU32BEAt(buffer, offset, limit);
    return buffer;
  },

  /**
   * Decode LIST response (status=0 payload, after the standard status byte)
   * [u32 item_count]
   * repeated item_count times:
   *   [u32 route_len][bytes route]
   *   [u32 owner_id_len][bytes owner_id]
   *   [u64 holder_incarnation]
   *   [u32 acquired_at_len][bytes acquired_at]
   *   [u64 expires_in_secs]
   *   [u32 renewals]
   * [u8 has_next]
   *   [u64 snapshot_id][u32 offset] if has_next==1
   */
  decodeListResponse(payload: Uint8Array): LeaseListPage {
    const parsed = parseStandardResponse(payload);
    if (!parsed.success) {
      throw new LeaseError(
        `LIST failed: ${parsed.error ?? "unknown error"}`,
        "LIST_FAILED",
        parsed.errorCode,
      );
    }

    const reader = createBufferReader(parsed.data);
    const itemCount = reader.readU32BE();
    const items: LeaseListItem[] = [];
    for (let index = 0; index < itemCount; index++) {
      const route = reader.readRoute();
      const ownerId = reader.readString();
      const holderIncarnation = reader.readU64BE();
      const acquiredAt = reader.readString();
      const expiresInSecs = reader.readU64BE();
      const renewals = reader.readU32BE();
      items.push({ route, ownerId, holderIncarnation, acquiredAt, expiresInSecs, renewals });
    }

    const hasNext = reader.readU8();
    let nextCursor: LeaseListCursor | undefined;
    if (hasNext === 1) {
      const snapshotId = reader.readU64BE();
      const offset = reader.readU32BE();
      nextCursor = { snapshotId, offset };
    } else if (hasNext !== 0) {
      throw new ProtocolError(`Unknown LIST has_next flag ${hasNext}`, undefined, {
        operation: "LEASE_LIST",
      });
    }

    if (!reader.isEOF()) {
      throw new ProtocolError("LIST success response has trailing data", undefined, {
        operation: "LEASE_LIST",
      });
    }

    return { items, nextCursor };
  },

  decodeSuccessResponse(payload: Uint8Array, operation: string): Uint8Array {
    const parsed = parseStandardResponse(payload);
    if (!parsed.success) {
      throw new LeaseError(
        `${operation} failed: ${parsed.error ?? "unknown error"}`,
        `${operation}_FAILED`,
        parsed.errorCode,
      );
    }
    return parsed.data;
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
