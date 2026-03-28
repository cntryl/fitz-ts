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

export type KvGetResult =
  | { type: "found"; value: Uint8Array }
  | { type: "not-found" };

export interface KvBeginResponse {
  status: number;
  txId?: bigint;
}

export interface KvStatusResponse {
  status: number;
}

export interface KvGetResponse {
  status: number;
  found: boolean;
  value?: Uint8Array;
}

export interface KvScanResponse {
  status: number;
  keys: Uint8Array[];
  nextCursor?: Uint8Array;
}

export enum KvStatus {
  Ok = 0,
  TransactionAborted = 1,
  LeaseExpired = 2,
  ConflictingWrite = 3,
  KeyNotFound = 4,
  OperationNotAllowed = 5,
}
