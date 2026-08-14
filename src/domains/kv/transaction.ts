/**
 * KV transaction wrapper.
 */

import "../../core/async-dispose";

import { createDomainClient } from "../base";
import type { DisconnectListenerPort, RequestPort, RetryExecutionPort } from "../base";
import { KvCodec } from "./codec";
import {
  KvGetResult,
  KvScanOptions,
  KvScanPage,
  KvStatus,
  KvStatusNames,
  KvStatusResponse,
} from "./types";
import {
  MSG_KV_PUT,
  MSG_KV_INSERT,
  MSG_KV_GET,
  MSG_KV_DELETE,
  MSG_KV_DELETE_RANGE,
  MSG_KV_COMMIT,
  MSG_KV_ROLLBACK,
  MSG_KV_SCAN,
} from "../../frame/types";
import { KvError } from "../../core/errors";
import { formatStatusName } from "../internal/status";

/**
 * Single-connection KV transaction. It becomes unusable after commit,
 * rollback, disposal, or disconnect; never retain it across reconnection.
 */
export interface KvTransaction extends AsyncDisposable {
  /** Upserts `key` to `value`. Requires a `ReadWrite` transaction. */
  put(options: {
    /** Binary key. */
    key: Uint8Array;
    /** Replacement value. */
    value: Uint8Array;
    /** Cancels waiting; post-send mutation outcome can be ambiguous. */
    signal?: AbortSignal;
  }): Promise<void>;
  /** Inserts a value only when `key` is absent; conflicting writes reject. */
  insert(options: {
    /** Binary key that must not already exist. */
    key: Uint8Array;
    /** Value to insert. */
    value: Uint8Array;
    /** Cancels waiting; post-send mutation outcome can be ambiguous. */
    signal?: AbortSignal;
  }): Promise<void>;
  /** Reads `key`, returning a discriminated not-found result instead of throwing when it is absent. */
  get(options: {
    /** Binary key to read. */
    key: Uint8Array;
    /** Cancels this read request. */
    signal?: AbortSignal;
  }): Promise<KvGetResult>;
  /** Deletes `key`. Requires a `ReadWrite` transaction. */
  delete(options: {
    /** Binary key to delete. */
    key: Uint8Array;
    /** Cancels waiting; post-send mutation outcome can be ambiguous. */
    signal?: AbortSignal;
  }): Promise<void>;
  /** Deletes keys in the half-open binary range `[startKey, endKey)`. */
  deleteRange(options: {
    /** Inclusive binary lower bound. */
    startKey: Uint8Array;
    /** Exclusive binary upper bound; must compare greater than `startKey`. */
    endKey: Uint8Array;
    /** Cancels waiting; post-send mutation outcome can be ambiguous. */
    signal?: AbortSignal;
  }): Promise<void>;
  /** Reads one page of keys and values from this transaction's consistent view. */
  scan(options?: KvScanOptions): Promise<KvScanPage>;
  /** Finalizes mutations using the durability selected by {@link KvBeginOptions}. */
  commit(options?: {
    /** Cancels waiting; post-send commit outcome can be ambiguous. */
    signal?: AbortSignal;
  }): Promise<void>;
  /** Discards uncommitted mutations. Safe cleanup should prefer disposal when either outcome is acceptable. */
  rollback(options?: {
    /** Cancels waiting; the local transaction still becomes unusable. */
    signal?: AbortSignal;
  }): Promise<void>;
  /** Returns whether the local transaction handle can still issue operations. */
  isOpen(): boolean;
}

type KvTransactionConnectionPort = RequestPort & DisconnectListenerPort & RetryExecutionPort;

function compareKeys(left: Uint8Array, right: Uint8Array): number {
  const sharedLength = Math.min(left.length, right.length);
  for (let index = 0; index < sharedLength; index += 1) {
    if (left[index] !== right[index]) {
      return left[index]! - right[index]!;
    }
  }
  return left.length - right.length;
}

function assertValidRange(startKey: Uint8Array, endKey: Uint8Array): void {
  if (compareKeys(startKey, endKey) >= 0) {
    throw new KvError("Range start key must be less than end key", "INVALID_RANGE");
  }
}

export function createKvTransaction(
  connection: KvTransactionConnectionPort,
  route: string,
  txId: bigint,
) {
  let closed = false;
  let unsubscribeDisconnect: () => void = () => undefined;
  unsubscribeDisconnect = connection.onDisconnect(() => {
    closed = true;
    unsubscribeDisconnect();
  });

  const ensureOpen = (): void => {
    if (closed) {
      throw new KvError("Transaction already closed", "TX_CLOSED");
    }
  };

  const checkStatus = (response: KvStatusResponse, operation: string): void => {
    if (response.status === KvStatus.Ok) {
      return;
    }
    const reason = response.errorMessage ?? formatStatusName(response.status, KvStatusNames);
    throw new KvError(`${operation} failed: ${reason}`, operation, response.status);
  };

  const { runWithRetry } = createDomainClient(connection);

  const put = async (options: {
    key: Uint8Array;
    value: Uint8Array;
    signal?: AbortSignal;
  }): Promise<void> => {
    ensureOpen();
    const payload = KvCodec.encodePut(txId, route, options.key, options.value);
    const response = await connection.request(MSG_KV_PUT, payload, options.signal);
    checkStatus(KvCodec.decodeStatusResponse(response), "PUT");
  };

  const insert = async (options: {
    key: Uint8Array;
    value: Uint8Array;
    signal?: AbortSignal;
  }): Promise<void> => {
    ensureOpen();
    const payload = KvCodec.encodeInsert(txId, route, options.key, options.value);
    const response = await connection.request(MSG_KV_INSERT, payload, options.signal);
    checkStatus(KvCodec.decodeStatusResponse(response), "INSERT");
  };

  const get = async (options: { key: Uint8Array; signal?: AbortSignal }): Promise<KvGetResult> => {
    ensureOpen();
    return runWithRetry(
      {
        domain: "kv",
        operation: "get",
        retryClass: "replayable_read",
        signal: options.signal,
      },
      async () => {
        const payload = KvCodec.encodeGet(txId, route, options.key);
        const response = await connection.request(MSG_KV_GET, payload, options.signal);
        const decoded = KvCodec.decodeGetResponse(response);
        checkStatus(decoded, "GET");
        if (!decoded.found || !decoded.value) {
          return { type: "not-found" };
        }
        return { type: "found", value: decoded.value };
      },
    );
  };

  const deleteItem = async (options: { key: Uint8Array; signal?: AbortSignal }): Promise<void> => {
    ensureOpen();
    const payload = KvCodec.encodeDelete(txId, route, options.key);
    const response = await connection.request(MSG_KV_DELETE, payload, options.signal);
    checkStatus(KvCodec.decodeStatusResponse(response), "DELETE");
  };

  const deleteRange = async (options: {
    startKey: Uint8Array;
    endKey: Uint8Array;
    signal?: AbortSignal;
  }): Promise<void> => {
    ensureOpen();
    assertValidRange(options.startKey, options.endKey);
    const payload = KvCodec.encodeDeleteRange(txId, route, options.startKey, options.endKey);
    const response = await connection.request(MSG_KV_DELETE_RANGE, payload, options.signal);
    checkStatus(KvCodec.decodeStatusResponse(response), "DELETE_RANGE");
  };

  const scan = async (options: KvScanOptions = {}): Promise<KvScanPage> => {
    ensureOpen();
    if (options.startKey !== undefined && options.endKey !== undefined) {
      assertValidRange(options.startKey, options.endKey);
    }
    return runWithRetry(
      {
        domain: "kv",
        operation: "scan",
        retryClass: "replayable_read",
        signal: options.signal,
      },
      async () => {
        const payload = KvCodec.encodeScan(txId, route, options);
        const response = await connection.request(MSG_KV_SCAN, payload, options.signal);
        const decoded = KvCodec.decodeScanResponse(response);
        checkStatus(decoded, "SCAN");
        return { entries: decoded.entries, hasMore: decoded.hasMore };
      },
    );
  };

  const commit = async (options: { signal?: AbortSignal } = {}): Promise<void> => {
    ensureOpen();
    closed = true;
    unsubscribeDisconnect();
    const payload = KvCodec.encodeCommit(txId, route);
    const response = await connection.request(MSG_KV_COMMIT, payload, options.signal);
    checkStatus(KvCodec.decodeStatusResponse(response), "COMMIT");
  };

  const rollback = async (options: { signal?: AbortSignal } = {}): Promise<void> => {
    if (closed) {
      return;
    }

    closed = true;
    unsubscribeDisconnect();
    const payload = KvCodec.encodeRollback(txId, route);
    const response = await connection.request(MSG_KV_ROLLBACK, payload, options.signal);
    checkStatus(KvCodec.decodeStatusResponse(response), "ROLLBACK");
  };

  const isOpen = (): boolean => !closed;

  const asyncDispose = async (): Promise<void> => {
    try {
      await rollback();
    } catch {
      // Disposal is explicitly best effort.
    }
  };

  return {
    put,
    insert,
    get,
    delete: deleteItem,
    deleteRange,
    scan,
    commit,
    rollback,
    isOpen,
    [Symbol.asyncDispose]: asyncDispose,
  };
}
