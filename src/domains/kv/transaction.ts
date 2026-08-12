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

export interface KvTransaction extends AsyncDisposable {
  put(options: { key: Uint8Array; value: Uint8Array; signal?: AbortSignal }): Promise<void>;
  insert(options: { key: Uint8Array; value: Uint8Array; signal?: AbortSignal }): Promise<void>;
  get(options: { key: Uint8Array; signal?: AbortSignal }): Promise<KvGetResult>;
  delete(options: { key: Uint8Array; signal?: AbortSignal }): Promise<void>;
  deleteRange(options: {
    startKey: Uint8Array;
    endKey: Uint8Array;
    signal?: AbortSignal;
  }): Promise<void>;
  scan(options?: KvScanOptions): Promise<KvScanPage>;
  commit(options?: { signal?: AbortSignal }): Promise<void>;
  rollback(options?: { signal?: AbortSignal }): Promise<void>;
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
