/**
 * KV domain types.
 */

import { createSubscriptionHandle } from "../internal/subscription-handle";

/** Transaction access mode. Use `ReadOnly` when no mutation methods will be called. */
export type TxMode = "ReadOnly" | "ReadWrite";
/** Commit durability: `Buffered` acknowledges before durable flush; `Sync` waits for durability. */
export type DurabilityMode = "Buffered" | "Sync";

/** Options for starting a KV transaction. */
export interface KvBeginOptions {
  /** Access mode. Defaults to `ReadWrite`. */
  mode?: TxMode;
  /** Required acknowledgement/durability level for commit. */
  durability: DurabilityMode;
  /** Cancels transaction creation; it does not cancel an already-created transaction. */
  signal?: AbortSignal;
}

/** Options for scanning keys inside a transaction. */
export interface KvScanOptions {
  /** Inclusive binary lower bound. Omit to start at the first key. */
  startKey?: Uint8Array;
  /** Exclusive binary upper bound; it must compare greater than `startKey`. */
  endKey?: Uint8Array;
  /** Maximum entries returned in this page. */
  limit?: number;
  /** Reverses key order and range traversal when `true`. */
  reverse?: boolean;
  /** Cancels this scan request without closing the transaction. */
  signal?: AbortSignal;
}

/**
 * Result of a KV lookup. Check `type` before reading `value`; absence is a
 * normal result rather than an exception.
 */
export type KvGetResult =
  | {
      /** Discriminant indicating that the key exists. */
      type: "found";
      /** Value read from the transaction's consistent view. */
      value: Uint8Array;
    }
  | {
      /** Discriminant indicating normal key absence. */
      type: "not-found";
    };

/** One page of KV scan results in binary key order (or reverse order when requested). */
export interface KvScanPage {
  /** Key/value pairs returned by the broker. Treat buffers as caller-owned read-only data. */
  entries: readonly { key: Uint8Array; value: Uint8Array }[];
  /** Whether another page exists after this one. */
  hasMore: boolean;
}

/** Notification that one or more mutations committed beneath a subscribed selector. */
export interface KvNotification {
  /** Concrete KV route that changed. */
  route: string;
  /** Number of mutations represented by the notification. */
  mutationCount: bigint;
}

/** KV notification callback. It may run concurrently subject to `asyncHandlers` limits. */
export type KvHandler = (notification: KvNotification) => void | Promise<void>;

/** Active KV subscription. Dispose or unsubscribe it to release broker and local resources. */
export interface KvSubscription extends AsyncDisposable {
  /** Resolves after unsubscribe; rejects with `AsyncHandlerOverflowError` on local overflow. */
  readonly completion: Promise<void>;
  /** Stops this local handler; the shared wire subscription closes after its final handler leaves. */
  unsubscribe(): Promise<void>;
}

export function createKvSubscription(
  unsubscribeFn: () => Promise<void>,
  signal?: AbortSignal,
): KvSubscription {
  return createSubscriptionHandle<KvSubscription>(unsubscribeFn, signal);
}

export interface KvBeginResponse {
  status: number;
  txId?: bigint;
}

export interface KvStatusResponse {
  status: number;
  errorMessage?: string;
}

export interface KvGetResponse {
  status: number;
  found: boolean;
  value?: Uint8Array;
  errorMessage?: string;
}

export interface KvScanResponse {
  status: number;
  entries: Array<{ key: Uint8Array; value: Uint8Array }>;
  hasMore: boolean;
  errorMessage?: string;
}

export enum KvStatus {
  Ok = 0,
  TransactionAborted = 1,
  LeaseExpired = 2,
  ConflictingWrite = 3,
  KeyNotFound = 4,
  OperationNotAllowed = 5,
}

export const KvStatusNames: Record<number, string> = {
  [KvStatus.TransactionAborted]: "TransactionAborted",
  [KvStatus.LeaseExpired]: "LeaseExpired",
  [KvStatus.ConflictingWrite]: "ConflictingWrite",
  [KvStatus.KeyNotFound]: "KeyNotFound",
  [KvStatus.OperationNotAllowed]: "OperationNotAllowed",
};
