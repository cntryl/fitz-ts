/**
 * KV domain types.
 */

export type TxMode = "ReadOnly" | "ReadWrite";
export type DurabilityMode = "Buffered" | "Sync";

export interface KvBeginOptions {
  mode?: TxMode;
  durability: DurabilityMode;
}

export interface KvScanOptions {
  startKey?: Uint8Array;
  endKey?: Uint8Array;
  limit?: number;
  reverse?: boolean;
}

export type KvGetResult = { type: "found"; value: Uint8Array } | { type: "not-found" };

export interface KvScanPage {
  entries: Array<{ key: Uint8Array; value: Uint8Array }>;
  hasMore: boolean;
}

export interface KvNotification {
  route: string;
  mutationCount: bigint;
}

export type KvHandler = (notification: KvNotification) => void | Promise<void>;

export type KvSubscription = ReturnType<typeof createKvSubscription>;

export function createKvSubscription(
  getSubId: () => bigint,
  pattern: string,
  unsubscribeFn: () => Promise<void>,
) {
  return {
    get subId(): bigint {
      return getSubId();
    },
    pattern,
    unsubscribe: unsubscribeFn,
  };
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
