/**
 * Shared utilities for domain clients
 */

import type { RetryOperation } from "../client/resilience";
import type {
  NotificationHandler,
  PushFrameClassifier,
  PushFrameClassifierRegistration,
} from "../client/multiplexer";
import type { ConnectionState } from "../core/types";

export type ReconnectListener = () => void | Promise<void>;
export type DisconnectListener = () => void;
export type AsyncHandlerTask = () => void | Promise<void>;

export interface RequestPort {
  request(messageType: number, payload: Uint8Array, signal?: AbortSignal): Promise<Uint8Array>;
}

export interface ReconnectRestoreRequestPort {
  requestDuringReconnectRestore(
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array>;
}

export interface SendPort {
  send(messageType: number, payload: Uint8Array, signal?: AbortSignal): Promise<void>;
}

export interface FireAndForgetPort {
  sendFireAndForget(messageType: number, payload: Uint8Array, signal?: AbortSignal): Promise<void>;
}

export interface OptionalResponsePort {
  expectOptionalResponse?(messageType: number): () => void;
  getMultiplexer?(): {
    expectOptionalResponse(messageType: number): () => void;
  };
}

export interface NotificationPort {
  registerNotificationHandler(messageType: number, handler: NotificationHandler): void;
  unregisterNotificationHandler?(messageType: number): void;
}

export interface PushClassifierPort {
  registerPushFrameClassifier?(
    messageType: number,
    classifier: PushFrameClassifier,
  ): PushFrameClassifierRegistration;
}

export interface ReconnectListenerPort {
  onReconnect(listener: ReconnectListener): () => void;
}

export interface DisconnectListenerPort {
  onDisconnect(listener: DisconnectListener): () => void;
}

export interface AsyncDispatchPort {
  dispatchAsyncHandler(task: AsyncHandlerTask): void;
  tryDispatchAsyncHandler?(task: AsyncHandlerTask): boolean;
}

export interface RetryExecutionPort {
  executeWithRetry?: <T>(operation: RetryOperation, task: () => Promise<T>) => Promise<T>;
}

export interface StateReadPort {
  getState(): ConnectionState;
}

export function createDomainClient(connection: RequestPort & Partial<ReconnectRestoreRequestPort>) {
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
    if (typeof connection.requestDuringReconnectRestore === "function") {
      return await connection.requestDuringReconnectRestore(messageType, payload, signal);
    }

    return await connection.request(messageType, payload, signal);
  };

  const runWithRetry = async <T>(operation: RetryOperation, task: () => Promise<T>): Promise<T> => {
    const retryConnection = connection as RequestPort & RetryExecutionPort;
    if (typeof retryConnection.executeWithRetry === "function") {
      return retryConnection.executeWithRetry(operation, task);
    }

    return task();
  };

  const expectOptionalResponse = (messageType: number): (() => void) => {
    const optionalConnection = connection as RequestPort & OptionalResponsePort;
    if (typeof optionalConnection.expectOptionalResponse === "function") {
      return optionalConnection.expectOptionalResponse(messageType);
    }

    const multiplexer = optionalConnection.getMultiplexer?.();
    if (multiplexer) {
      return multiplexer.expectOptionalResponse(messageType);
    }

    return () => undefined;
  };

  return {
    connection,
    requestFrame,
    requestReconnectFrame,
    runWithRetry,
    expectOptionalResponse,
  };
}
