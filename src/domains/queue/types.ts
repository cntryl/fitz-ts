/**
 * Queue domain types
 * Per fitz-go/internal/domains/queue/queue.go
 */

import { Connection } from "../../client/connection";
import { QueueCodec } from "./codec";
import { QueueError } from "../../core/errors";
import { MSG_QUEUE_EXTEND, MSG_QUEUE_COMPLETE } from "../../frame/types";

/**
 * Queue item represents a reserved queue message.
 * It carries the route and token required for `extend()` and `complete()`.
 */
export type QueueItem = ReturnType<typeof createQueueItem>;

export function createQueueItem(
  id: bigint,
  token: bigint,
  body: Uint8Array,
  route: string,
  connection: Connection,
) {
  let closed = false;
  let unsubscribeDisconnect: () => void = () => undefined;
  unsubscribeDisconnect = connection.onDisconnect(() => {
    closed = true;
    unsubscribeDisconnect();
  });

  const ensureOpen = (): void => {
    if (closed) {
      throw new QueueError("Queue item is no longer valid after disconnect", "ITEM_CLOSED");
    }
  };

  const extend = async (leaseSecs: number, signal?: AbortSignal): Promise<void> => {
    ensureOpen();
    const payload = QueueCodec.encodeExtend(route, id, token, leaseSecs);
    const response = await connection.request(MSG_QUEUE_EXTEND, payload, signal);
    const decoded = QueueCodec.decodeExtendResponse(response);

    if (decoded.status !== QueueStatus.Ok) {
      const errorCode = decoded.errorCode ?? decoded.status;
      const statusName = QueueStatus[errorCode] || `Unknown(${errorCode})`;
      const reason = decoded.errorMessage ?? statusName;
      throw new QueueError(`EXTEND failed: ${reason}`, statusName, errorCode);
    }
  };

  const complete = async (signal?: AbortSignal): Promise<void> => {
    ensureOpen();
    const requestPayload = QueueCodec.encodeComplete(route, id, token);
    const response = await connection.request(MSG_QUEUE_COMPLETE, requestPayload, signal);
    const decoded = QueueCodec.decodeCompleteResponse(response);

    if (decoded.status !== QueueStatus.Ok) {
      const errorCode = decoded.errorCode ?? decoded.status;
      const statusName = QueueStatus[errorCode] || `Unknown(${errorCode})`;
      const reason = decoded.errorMessage ?? statusName;
      throw new QueueError(`COMPLETE failed: ${reason}`, statusName, errorCode);
    }

    closed = true;
    unsubscribeDisconnect();
  };

  const testOnlyInvalidToken = (): bigint => id + 1n;

  const testOnlyCompleteWithToken = async (
    tokenToUse: bigint,
    signal?: AbortSignal,
  ): Promise<void> => {
    ensureOpen();
    const requestPayload = QueueCodec.encodeComplete(route, id, tokenToUse);
    const response = await connection.request(MSG_QUEUE_COMPLETE, requestPayload, signal);
    const decoded = QueueCodec.decodeCompleteResponse(response);

    if (decoded.status !== QueueStatus.Ok) {
      const errorCode = decoded.errorCode ?? decoded.status;
      const statusName = QueueStatus[errorCode] || `Unknown(${errorCode})`;
      const reason = decoded.errorMessage ?? statusName;
      throw new QueueError(`COMPLETE failed: ${reason}`, statusName, errorCode);
    }

    closed = true;
    unsubscribeDisconnect();
  };

  return {
    body,
    extend,
    complete,
    testOnlyInvalidToken,
    testOnlyCompleteWithToken,
  };
}

/**
 * Availability notification from a queue.
 */
export interface AvailabilityNotification {
  route: string;
}

/**
 * Handler for availability notifications.
 */
export type AvailabilityHandler = (notification: AvailabilityNotification) => void | Promise<void>;

/**
 * Queue availability subscription.
 */
export type QueueSubscription = ReturnType<typeof createQueueSubscription>;

export function createQueueSubscription(
  subId: bigint,
  pattern: string,
  unsubscribeFn: (subId: bigint) => Promise<void>,
) {
  const unsubscribe = async (): Promise<void> => {
    await unsubscribeFn(subId);
  };

  return {
    subId,
    pattern,
    unsubscribe,
  };
}

/**
 * Queue operation status codes
 */
export enum QueueStatus {
  Ok = 0,
  QueueNotFound = 1,
  MessageNotFound = 2,
  InvalidToken = 3,
  QueueFull = 4,
  InvalidDelay = 5,
}

/**
 * Options for enqueue operations.
 */
export interface EnqueueOptions {
  priority?: number;
  delayMs?: number;
  ttlMs?: number;
}

/**
 * Internal codec response types.
 */
export interface QueueEnqueueResponse {
  status: number;
  messageId?: bigint;
  errorCode?: number;
  errorMessage?: string;
}

export interface QueueReserveResponse {
  status: number;
  items?: Array<{
    id: bigint;
    token: bigint;
    body: Uint8Array;
  }>;
  cursor?: Uint8Array;
  errorCode?: number;
  errorMessage?: string;
}

export interface QueueExtendResponse {
  status: number;
  errorCode?: number;
  errorMessage?: string;
}

export interface QueueCompleteResponse {
  status: number;
  errorCode?: number;
  errorMessage?: string;
}

export interface QueueSubscribeResponse {
  status: number;
  subId?: bigint;
  errorCode?: number;
  errorMessage?: string;
}

export interface QueueUnsubscribeResponse {
  status: number;
  errorCode?: number;
  errorMessage?: string;
}
