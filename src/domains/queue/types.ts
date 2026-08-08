/**
 * Queue domain types
 * Per fitz-go/internal/domains/queue/queue.go
 */

import type { DisconnectListenerPort, RequestPort } from "../base";
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
  connection: RequestPort & DisconnectListenerPort,
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
      // EXTEND never carries a real numeric domain error code on the wire
      // (decodeExtendResponse's errorCode is always undefined) — falling
      // back to `decoded.status` here would collide with the small
      // domain-status enum (status 1 === QueueStatus.QueueNotFound) and
      // mislabel every failure as "QueueNotFound" regardless of the real
      // cause (e.g. an expired lease). Use a generic, honest code instead.
      const reason = decoded.errorMessage ?? "EXTEND_FAILED";
      throw new QueueError(`EXTEND failed: ${reason}`, "EXTEND_FAILED", decoded.errorCode);
    }
  };

  const complete = async (signal?: AbortSignal): Promise<void> => {
    ensureOpen();
    const requestPayload = QueueCodec.encodeComplete(route, id, token);
    const response = await connection.request(MSG_QUEUE_COMPLETE, requestPayload, signal);
    const decoded = QueueCodec.decodeCompleteResponse(response);

    if (decoded.status !== QueueStatus.Ok) {
      // Same reasoning as extend() above: COMPLETE's plain response never
      // carries a real domain error code either.
      const reason = decoded.errorMessage ?? "COMPLETE_FAILED";
      throw new QueueError(`COMPLETE failed: ${reason}`, "COMPLETE_FAILED", decoded.errorCode);
    }

    closed = true;
    unsubscribeDisconnect();
  };

  return {
    route,
    body,
    extend,
    complete,
  };
}

/**
 * Availability notification from a queue.
 */
export interface AvailabilityNotification {
  route: string;
  readyMessages: bigint;
  delayedMessages: bigint;
  inflightMessages: bigint;
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
  getSubId: () => bigint,
  pattern: string,
  unsubscribeFn: () => Promise<void>,
) {
  const unsubscribe = async (): Promise<void> => {
    await unsubscribeFn();
  };

  return {
    get subId(): bigint {
      return getSubId();
    },
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
  /**
   * @deprecated Not yet wire-supported — the ENQUEUE frame has no byte
   * range for priority. Setting this throws synchronously rather than
   * silently being ignored.
   */
  priority?: number;
  delayMs?: number;
  /**
   * @deprecated Not yet wire-supported — the ENQUEUE frame has no byte
   * range for a TTL. Setting this throws synchronously rather than
   * silently being ignored.
   */
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
    route: string;
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
