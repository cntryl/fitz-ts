/**
 * KV domain types.
 */

import { createSubscriptionHandle } from "../internal/subscription-handle";

export type TxMode = "ReadOnly" | "ReadWrite";
export type DurabilityMode = "Buffered" | "Sync";

export interface KvBeginOptions {
  mode?: TxMode;
  durability: DurabilityMode;
  signal?: AbortSignal;
}

export interface KvScanOptions {
  startKey?: Uint8Array;
  endKey?: Uint8Array;
  limit?: number;
  reverse?: boolean;
  signal?: AbortSignal;
}

export type KvGetResult = { type: "found"; value: Uint8Array } | { type: "not-found" };

export interface KvScanPage {
  entries: readonly { key: Uint8Array; value: Uint8Array }[];
  hasMore: boolean;
}

export interface KvNotification {
  route: string;
  mutationCount: bigint;
}

export type KvHandler = (notification: KvNotification) => void | Promise<void>;

export interface KvSubscription extends AsyncDisposable {
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
