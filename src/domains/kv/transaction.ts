/**
 * KV transaction wrapper.
 */

import type { DisconnectListenerPort, RequestPort, RetryExecutionPort } from "../base";
import type { RetryOperation } from "../../client/resilience";
import { KvCodec } from "./codec";
import { KvGetResult, KvScanOptions, KvStatus } from "./types";
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
import { createSliceIterator, createAsyncIterableIterator } from "../../core/iterator";

export type KvTransaction = ReturnType<typeof createKvTransaction>;

type KvTransactionConnectionPort = RequestPort & DisconnectListenerPort & RetryExecutionPort;

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

  const checkStatus = (status: number, operation: string): void => {
    if (status === KvStatus.Ok) {
      return;
    }

    const statusNames: Record<number, string> = {
      [KvStatus.TransactionAborted]: "TransactionAborted",
      [KvStatus.LeaseExpired]: "LeaseExpired",
      [KvStatus.ConflictingWrite]: "ConflictingWrite",
      [KvStatus.KeyNotFound]: "KeyNotFound",
      [KvStatus.OperationNotAllowed]: "OperationNotAllowed",
    };

    throw new KvError(
      `${operation} failed: ${statusNames[status] ?? `Unknown(${status})`}`,
      operation,
      status,
    );
  };

  const runWithRetry = async <T>(operation: RetryOperation, task: () => Promise<T>): Promise<T> => {
    if (typeof connection.executeWithRetry === "function") {
      return connection.executeWithRetry(operation, task);
    }

    return task();
  };

  const put = async (key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> => {
    ensureOpen();
    const payload = KvCodec.encodePut(txId, route, key, value);
    const response = await connection.request(MSG_KV_PUT, payload, signal);
    checkStatus(KvCodec.decodeStatusResponse(response).status, "PUT");
  };

  const insert = async (
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> => {
    ensureOpen();
    const payload = KvCodec.encodeInsert(txId, route, key, value);
    const response = await connection.request(MSG_KV_INSERT, payload, signal);
    checkStatus(KvCodec.decodeStatusResponse(response).status, "INSERT");
  };

  const get = async (key: Uint8Array, signal?: AbortSignal): Promise<KvGetResult> => {
    ensureOpen();
    return runWithRetry(
      {
        domain: "kv",
        operation: "get",
        retryClass: "replayable_read",
        signal,
      },
      async () => {
        const payload = KvCodec.encodeGet(txId, route, key);
        const response = await connection.request(MSG_KV_GET, payload, signal);
        const decoded = KvCodec.decodeGetResponse(response);
        checkStatus(decoded.status, "GET");
        if (!decoded.found || !decoded.value) {
          return { type: "not-found" };
        }
        return { type: "found", value: decoded.value };
      },
    );
  };

  const deleteItem = async (key: Uint8Array, signal?: AbortSignal): Promise<void> => {
    ensureOpen();
    const payload = KvCodec.encodeDelete(txId, route, key);
    const response = await connection.request(MSG_KV_DELETE, payload, signal);
    checkStatus(KvCodec.decodeStatusResponse(response).status, "DELETE");
  };

  const deleteRange = async (
    startKey: Uint8Array,
    endKey: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> => {
    ensureOpen();
    const payload = KvCodec.encodeDeleteRange(txId, route, startKey, endKey);
    const response = await connection.request(MSG_KV_DELETE_RANGE, payload, signal);
    checkStatus(KvCodec.decodeStatusResponse(response).status, "DELETE_RANGE");
  };

  const scan = async (
    options: KvScanOptions = {},
    signal?: AbortSignal,
  ): Promise<AsyncIterable<Uint8Array>> => {
    ensureOpen();
    return runWithRetry(
      {
        domain: "kv",
        operation: "scan",
        retryClass: "replayable_read",
        signal,
      },
      async () => {
        const payload = KvCodec.encodeScan(txId, route, options);
        const response = await connection.request(MSG_KV_SCAN, payload, signal);
        const decoded = KvCodec.decodeScanResponse(response);
        checkStatus(decoded.status, "SCAN");
        return createAsyncIterableIterator(createSliceIterator(decoded.keys));
      },
    );
  };

  const commit = async (signal?: AbortSignal): Promise<void> => {
    ensureOpen();
    closed = true;
    unsubscribeDisconnect();
    const payload = KvCodec.encodeCommit(txId, route);
    const response = await connection.request(MSG_KV_COMMIT, payload, signal);
    checkStatus(KvCodec.decodeStatusResponse(response).status, "COMMIT");
  };

  const rollback = async (signal?: AbortSignal): Promise<void> => {
    if (closed) {
      return;
    }

    closed = true;
    unsubscribeDisconnect();
    const payload = KvCodec.encodeRollback(txId, route);
    try {
      const response = await connection.request(MSG_KV_ROLLBACK, payload, signal);
      checkStatus(KvCodec.decodeStatusResponse(response).status, "ROLLBACK");
    } catch {
      // Best-effort cleanup.
    }
  };

  const getTxId = (): bigint => txId;

  const isOpen = (): boolean => !closed;

  return {
    put,
    insert,
    get,
    delete: deleteItem,
    deleteRange,
    scan,
    commit,
    rollback,
    getTxId,
    isOpen,
  };
}
