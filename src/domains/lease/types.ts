/**
 * Lease domain types
 * Per fitz-go/internal/domains/lease/lease.go
 */

import type { DisconnectListenerPort, RequestPort } from "../base";
import { createSubscriptionHandle } from "../internal/subscription-handle";
import { FitzError, LeaseError } from "../../core/errors";

/**
 * Change notification when a lease is released or expires
 */
export interface ChangeNotification {
  /** Concrete lease route that became available after release or expiry. */
  route: string;
}

/**
 * Handles a lease release/expiry wake signal. Re-query or attempt acquisition
 * to learn authoritative state; a notification does not grant ownership.
 */
export type ChangeHandler = (notif: ChangeNotification) => void | Promise<void>;

/**
 * Active lease change subscription
 */
export interface LeaseSubscription extends AsyncDisposable {
  /** Resolves after unsubscribe; rejects with `AsyncHandlerOverflowError` on local overflow. */
  readonly completion: Promise<void>;
  /** Stops this local listener and releases shared wire state after the final listener leaves. */
  unsubscribe(): Promise<void>;
}

export function createLeaseSubscription(
  unsubscribeFn: () => Promise<void>,
  signal?: AbortSignal,
): LeaseSubscription {
  return createSubscriptionHandle<LeaseSubscription>(unsubscribeFn, signal);
}

/**
 * Manual handle for one acquired lease. It is connection-bound: release it
 * explicitly, and reacquire rather than reusing it after a disconnect.
 */
export interface Lease {
  /**
   * Renews ownership for `ttlSeconds` and returns the new Unix expiry in
   * seconds. A failed extension permanently invalidates this handle.
   */
  extend(options: {
    /** New positive lease lifetime in seconds. */
    ttlSeconds: number;
    /** Cancels waiting; a post-send cancellation can leave renewal outcome ambiguous. */
    signal?: AbortSignal;
  }): Promise<bigint>;
  /** Releases ownership and permanently closes this handle, even if the request fails. */
  release(options?: {
    /** Cancels waiting; the local handle still closes and broker outcome may be ambiguous. */
    signal?: AbortSignal;
  }): Promise<void>;
  /** Returns the locally known Unix expiry timestamp in seconds. */
  getExpiry(): bigint;
}

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

  const extend = (options: { ttlSeconds: number; signal?: AbortSignal }): Promise<bigint> =>
    serialize(async () => {
      ensureOpen();
      try {
        const requestPayload = LeaseCodec.encodeExtend(route, currentToken, options.ttlSeconds);
        const response = await connection.request(MSG_LEASE_RENEW, requestPayload, options.signal);
        const data = LeaseCodec.decodeSuccessResponse(response, "EXTEND");
        if (!data || data.length < 8) {
          throw new LeaseError("EXTEND response missing fencing token", "EXTEND_INVALID_RESPONSE");
        }
        const reader = createBufferReader(data);
        currentToken = reader.readU64BE();
        currentExpiry = BigInt(Math.floor(Date.now() / 1000)) + BigInt(options.ttlSeconds);
        return currentExpiry;
      } catch (error) {
        closed = true;
        unsubscribeDisconnect();
        throw error;
      }
    });

  const release = (options: { signal?: AbortSignal } = {}): Promise<void> =>
    serialize(async () => {
      ensureOpen();
      closed = true;
      unsubscribeDisconnect();
      const payload = LeaseCodec.encodeRelease(route, currentToken);
      const response = await connection.request(MSG_LEASE_RELEASE, payload, options.signal);
      LeaseCodec.decodeSuccessResponse(response, "RELEASE");
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

/** Timing and cancellation options for manual lease acquisition. */
export interface LeaseAcquireOptions {
  /** Positive integer lifetime in seconds whose milliseconds fit a signed 32-bit timer. */
  ttlSeconds: number;
  /** Maximum seconds to wait for ownership. Defaults to 0; must fit unsigned 32-bit. */
  waitSeconds?: number;
  /** Cancels local/broker acquisition waiting; it does not release an already returned lease. */
  signal?: AbortSignal;
}

/**
 * Lease information from QUERY request
 */
export interface LeaseInfo {
  /** Whether the route currently has an owner. */
  isHeld: boolean;
  /** Broker-reported owner identity, when disclosure is permitted. */
  owner?: string;
  /** Current fencing token, when held and visible to this caller. */
  token?: bigint;
  /** Remaining lease lifetime in seconds. */
  ttlRemainingSecs?: bigint;
  /** Number of queued acquisition waiters. */
  pendingWaiters: number;
  /** Broker-reported Unix expiry in seconds. */
  expiresAt?: bigint;
}

/**
 * Response to QUERY request
 */
export interface QueryResponse {
  status: number;
  errorMessage?: string;
  errorCode?: number;
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
  /** Operation succeeded. */
  Ok = 0,
  /** Another owner currently holds the lease. */
  LeaseHeld = 1,
  /** Lease route has no active owner. */
  NotFound = 2,
  /** Supplied renewal or release token is stale or invalid. */
  InvalidToken = 3,
}

/** Managed-lease timing and cancellation options. */
export interface WithLeaseOptions {
  /**
   * Requested lease lifetime in seconds; managed renewal occurs near one third
   * of the TTL. Must be a positive safe integer whose milliseconds fit signed 32-bit timers.
   */
  ttlSeconds: number;
  /** Maximum seconds to wait for ownership. Defaults to 0; must fit an unsigned 32-bit integer. */
  waitSeconds?: number;
  /** Cancels acquisition and later aborts the admitted callback on lease loss or caller cancellation. */
  signal?: AbortSignal;
}

/**
 * Immutable authority granted when a managed lease callback is admitted.
 *
 * The fencing token is an admission epoch, not the lease handle's live
 * renewal credential. It remains stable for the callback invocation even
 * when renewal rotates the credential used for later broker operations.
 */
export interface LeaseAuthority {
  /**
   * Stable admission fencing token. Include it in protected downstream
   * writes; do not use it to renew or release the underlying lease.
   */
  readonly fencingToken: bigint;
}

/**
 * Represents a combination of failures across a withLease() invocation's
 * lifecycle (lease loss, callback failure, release failure) rather than a
 * single domain-status code, so it's a standalone FitzError subclass with
 * its own code prefix rather than a LeaseError subclass.
 */
export class LeaseLifecycleError extends FitzError {
  /** Ordered failures observed while running, renewing, and releasing the managed lease. */
  readonly causes: readonly unknown[];

  /** Creates an aggregate managed-lease lifecycle failure. */
  constructor(message: string, causes: readonly unknown[]) {
    super(message, "LEASE_LIFECYCLE_MULTIPLE_FAILURES", undefined, { causes });
    this.name = "LeaseLifecycleError";
    this.causes = causes;
    Object.setPrototypeOf(this, LeaseLifecycleError.prototype);
  }
}

// Import needed types for Lease class methods
import { createBufferReader } from "../../core/buffer";
import { MSG_LEASE_RENEW, MSG_LEASE_RELEASE } from "../../frame/types";
import { LeaseCodec } from "./codec";
