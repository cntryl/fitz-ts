/**
 * Lease domain types
 * Per fitz-go/internal/domains/lease/lease.go
 */

import type { Connection } from "../../client/connection";

/**
 * Change notification when a lease is released or expires
 */
export interface ChangeNotification {
  route: string;
}

/**
 * Handler for lease change notifications
 */
export type ChangeHandler = (notif: ChangeNotification) => Promise<void>;

/**
 * Active lease change subscription
 */
export type LeaseSubscription = ReturnType<typeof createLeaseSubscription>;

export function createLeaseSubscription(
  subId: bigint,
  pattern: string,
  unsubscribeFn: (subId: bigint) => Promise<void>,
) {
  const unsubscribe = async (): Promise<void> => {
    await unsubscribeFn(subId);
  };

  return {
    subId,
    pattern,
    unsubscribe,
  };
}

/**
 * Lease handle representing an acquired lease
 * Provides renew() and release() methods
 */
export type Lease = ReturnType<typeof createLease>;

export function createLease(
  token: bigint,
  expiresAt: bigint,
  route: string,
  connection: Connection,
) {
  let currentToken = token;
  let currentExpiry = expiresAt;

  const extend = async (ttlSecs: number, signal?: AbortSignal): Promise<bigint> => {
    const requestPayload = LeaseCodec.encodeExtend(route, currentToken, ttlSecs);
    const response = await connection.request(MSG_LEASE_RENEW, requestPayload, signal);
    const data = assertSuccess(response, "EXTEND");

    if (data && data.length >= 8) {
      const reader = new BufferReader(data);
      currentToken = reader.readU64BE();
    }

    currentExpiry = BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
    return currentExpiry;
  };

  const release = async (signal?: AbortSignal): Promise<void> => {
    const payload = LeaseCodec.encodeRelease(route, currentToken);
    const response = await connection.request(MSG_LEASE_RELEASE, payload, signal);
    assertSuccess(response, "RELEASE");
  };

  const getExpiry = (): bigint => currentExpiry;

  const testOnlyInvalidToken = (): bigint => currentToken + 1n;

  const testOnlyExtendWithToken = async (
    tokenToUse: bigint,
    ttlSecs: number,
    signal?: AbortSignal,
  ): Promise<bigint> => {
    const requestPayload = LeaseCodec.encodeExtend(route, tokenToUse, ttlSecs);
    const response = await connection.request(MSG_LEASE_RENEW, requestPayload, signal);
    const data = assertSuccess(response, "EXTEND");

    if (data && data.length >= 8) {
      const reader = new BufferReader(data);
      currentToken = reader.readU64BE();
    }

    currentExpiry = BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
    return currentExpiry;
  };

  const testOnlyReleaseWithToken = async (tokenToUse: bigint, signal?: AbortSignal): Promise<void> => {
    const payload = LeaseCodec.encodeRelease(route, tokenToUse);
    const response = await connection.request(MSG_LEASE_RELEASE, payload, signal);
    assertSuccess(response, "RELEASE");
  };

  return {
    extend,
    release,
    getExpiry,
    testOnlyInvalidToken,
    testOnlyExtendWithToken,
    testOnlyReleaseWithToken,
  };
}

/**
 * Response to ACQUIRE request
 */
export interface AcquireResponse {
  token: bigint;
  expiresAt?: bigint;
}

/**
 * Lease information from QUERY request
 */
export interface LeaseInfo {
  isHeld: boolean;
  owner?: string;
  token?: bigint;
  ttlRemainingSecs?: bigint;
  expiresAt?: bigint;
}

/**
 * Response to QUERY request
 */
export interface QueryResponse {
  status: number;
  isHeld?: boolean;
  owner?: string;
  token?: bigint;
  ttlRemainingSecs?: bigint;
  expiresAt?: bigint;
}

/**
 * Response to SUBSCRIBE request
 */
export interface SubscribeResponse {
  status: number;
  subId?: bigint;
}

/**
 * Response to UNSUBSCRIBE request
 */
export interface UnsubscribeResponse {
  status: number;
}

/**
 * Lease status codes
 */
export enum LeaseStatus {
  Ok = 0,
  LeaseHeld = 1,
  NotFound = 2,
  InvalidToken = 3,
}

// Import needed types for Lease class methods
import { BufferReader } from "../../core/buffer";
import { assertSuccess } from "../../protocol/response";
import { MSG_LEASE_RENEW, MSG_LEASE_RELEASE } from "../../frame/types";
import { LeaseCodec } from "./codec";
