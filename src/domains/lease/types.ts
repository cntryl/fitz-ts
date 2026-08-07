/**
 * Lease domain types
 * Per fitz-go/internal/domains/lease/lease.go
 */

import type { DisconnectListenerPort, RequestPort } from "../base";
import { FitzError, LeaseError } from "../../core/errors";

/**
 * Change notification when a lease is released or expires
 */
export interface ChangeNotification {
  route: string;
}

/**
 * Handler for lease change notifications
 */
export type ChangeHandler = (notif: ChangeNotification) => void | Promise<void>;

/**
 * Active lease change subscription
 */
export type LeaseSubscription = ReturnType<typeof createLeaseSubscription>;

export function createLeaseSubscription(
  getSubId: () => bigint,
  route: string,
  unsubscribeFn: () => Promise<void>,
) {
  const unsubscribe = async (): Promise<void> => {
    await unsubscribeFn();
  };

  return {
    get subId(): bigint {
      return getSubId();
    },
    route,
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
  connection: RequestPort & DisconnectListenerPort,
) {
  let currentToken = token;
  let currentExpiry = expiresAt;
  let closed = false;
  let operation = Promise.resolve();
  let unsubscribeDisconnect: () => void = () => undefined;
  unsubscribeDisconnect = connection.onDisconnect(() => {
    closed = true;
    unsubscribeDisconnect();
  });

  const ensureOpen = (): void => {
    if (closed) {
      throw new LeaseError("Lease handle is no longer valid after disconnect", "CLOSED");
    }
  };

  const serialize = <T>(fn: () => Promise<T>): Promise<T> => {
    const result = operation.then(fn, fn);
    operation = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  };

  const extend = (ttlSecs: number, signal?: AbortSignal): Promise<bigint> =>
    serialize(async () => {
      ensureOpen();
      try {
        const requestPayload = LeaseCodec.encodeExtend(route, currentToken, ttlSecs);
        const response = await connection.request(MSG_LEASE_RENEW, requestPayload, signal);
        const data = assertPlainSuccess(response, "EXTEND");
        if (!data || data.length < 8) {
          throw new LeaseError("EXTEND response missing fencing token", "EXTEND_INVALID_RESPONSE");
        }
        const reader = createBufferReader(data);
        currentToken = reader.readU64BE();
        currentExpiry = BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
        return currentExpiry;
      } catch (error) {
        closed = true;
        unsubscribeDisconnect();
        throw error;
      }
    });

  const release = (signal?: AbortSignal): Promise<void> =>
    serialize(async () => {
      ensureOpen();
      closed = true;
      unsubscribeDisconnect();
      const payload = LeaseCodec.encodeRelease(route, currentToken);
      const response = await connection.request(MSG_LEASE_RELEASE, payload, signal);
      assertPlainSuccess(response, "RELEASE");
    });

  const getExpiry = (): bigint => currentExpiry;

  return {
    extend,
    release,
    getExpiry,
  };
}

/**
 * Response to ACQUIRE request
 */
export interface AcquireResponse {
  token: bigint;
  responseType: 0 | 1 | 2 | 3;
  expiresAt?: bigint;
}

export interface LeaseAcquireOptions {
  waitSeconds?: number;
  signal?: AbortSignal;
}

/**
 * Lease information from QUERY request
 */
export interface LeaseInfo {
  isHeld: boolean;
  owner?: string;
  token?: bigint;
  ttlRemainingSecs?: bigint;
  pendingWaiters: number;
  expiresAt?: bigint;
}

/**
 * Response to QUERY request
 */
export interface QueryResponse {
  status: number;
  errorMessage?: string;
  isHeld?: boolean;
  owner?: string;
  token?: bigint;
  ttlRemainingSecs?: bigint;
  pendingWaiters?: number;
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

export interface WithLeaseOptions {
  waitForAvailability?: boolean;
  waitSeconds?: number;
  signal?: AbortSignal;
}

/**
 * Represents a combination of failures across a withLease() invocation's
 * lifecycle (lease loss, callback failure, release failure) rather than a
 * single domain-status code, so it's a standalone FitzError subclass with
 * its own code prefix rather than a LeaseError subclass.
 */
export class LeaseLifecycleError extends FitzError {
  readonly causes: readonly unknown[];

  constructor(message: string, causes: readonly unknown[]) {
    super(message, "LEASE_LIFECYCLE_MULTIPLE_FAILURES", undefined, { causes });
    this.name = "LeaseLifecycleError";
    this.causes = causes;
    Object.setPrototypeOf(this, LeaseLifecycleError.prototype);
  }
}

// Import needed types for Lease class methods
import { createBufferReader } from "../../core/buffer";
import { assertPlainSuccess } from "../../protocol/response";
import { MSG_LEASE_RENEW, MSG_LEASE_RELEASE } from "../../frame/types";
import { LeaseCodec } from "./codec";
