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
export interface LeaseSubscription extends AsyncDisposable {
  unsubscribe(): Promise<void>;
}

export function createLeaseSubscription(unsubscribeFn: () => Promise<void>): LeaseSubscription {
  let active = true;
  let pending: Promise<void> | undefined;
  const unsubscribe = async (): Promise<void> => {
    if (!active) return pending;
    active = false;
    pending = unsubscribeFn().catch((error: unknown) => {
      active = true;
      throw error;
    });
    try {
      await pending;
    } finally {
      pending = undefined;
    }
  };

  return {
    unsubscribe,
    async [Symbol.asyncDispose](): Promise<void> {
      try {
        await unsubscribe();
      } catch {
        // Disposal is explicitly best effort.
      }
    },
  };
}

/**
 * Lease handle representing an acquired lease
 * Provides renew() and release() methods
 */
export interface Lease {
  extend(options: { ttlSeconds: number; signal?: AbortSignal }): Promise<bigint>;
  release(options?: { signal?: AbortSignal }): Promise<void>;
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

export interface LeaseAcquireOptions {
  ttlSeconds: number;
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
  Ok = 0,
  LeaseHeld = 1,
  NotFound = 2,
  InvalidToken = 3,
}

export interface WithLeaseOptions {
  ttlSeconds: number;
  waitSeconds?: number;
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
  readonly fencingToken: bigint;
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
import { MSG_LEASE_RENEW, MSG_LEASE_RELEASE } from "../../frame/types";
import { LeaseCodec } from "./codec";
