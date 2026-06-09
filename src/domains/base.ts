/**
 * Shared utilities for domain clients
 */

import { Connection } from "../client/connection";
import type { RetryOperation } from "../client/resilience";

type ResilientConnection = Connection & {
  executeWithRetry?: <T>(operation: RetryOperation, task: () => Promise<T>) => Promise<T>;
  requestDuringReconnectRestore?: (
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ) => Promise<Uint8Array>;
};

export function createDomainClient(connection: Connection) {
  const requestFrame = async (
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> => connection.request(messageType, payload, signal);

  const requestReconnectFrame = async (
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> => {
    const resilientConnection = connection as ResilientConnection;
    if (typeof resilientConnection.requestDuringReconnectRestore === "function") {
      return await resilientConnection.requestDuringReconnectRestore(messageType, payload, signal);
    }

    return await connection.request(messageType, payload, signal);
  };

  const sendFrame = async (messageType: number, payload: Uint8Array): Promise<void> =>
    connection.send(messageType, payload);

  const runWithRetry = async <T>(operation: RetryOperation, task: () => Promise<T>): Promise<T> => {
    const resilientConnection = connection as ResilientConnection;
    if (typeof resilientConnection.executeWithRetry === "function") {
      return resilientConnection.executeWithRetry(operation, task);
    }

    return task();
  };

  return {
    connection,
    requestFrame,
    requestReconnectFrame,
    sendFrame,
    runWithRetry,
  };
}
