/**
 * Queue domain types
 * Per fitz-go/internal/domains/queue/queue.go
 */

import type { DisconnectListenerPort, RequestPort } from "../base";
import { createSubscriptionHandle } from "../internal/subscription-handle";
import { QueueCodec } from "./codec";
import { QueueError } from "../../core/errors";
import { MSG_QUEUE_EXTEND, MSG_QUEUE_COMPLETE } from "../../frame/types";

/**
 * Queue item represents a reserved queue message.
 * It carries the route and token required for `extend()` and `complete()`.
 */
export interface QueueItem {
  /** Concrete queue route from which this message was reserved. */
  readonly route: string;
  /** Message payload. Treat this buffer as immutable while processing the reservation. */
  readonly body: Uint8Array;
  /** Extends reservation visibility by `leaseSeconds`; rejects after completion or disconnect. */
  extend(options: {
    /** Additional visibility lease in seconds. */
    leaseSeconds: number;
    /** Cancels waiting; renewal outcome may be ambiguous after send. */
    signal?: AbortSignal;
  }): Promise<void>;
  /** Acknowledges successful processing and permanently consumes the message. */
  complete(options?: {
    /** Cancels waiting; completion outcome may be ambiguous after send. */
    signal?: AbortSignal;
  }): Promise<void>;
}

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

  const extend = async (options: { leaseSeconds: number; signal?: AbortSignal }): Promise<void> => {
    ensureOpen();
    const payload = QueueCodec.encodeExtend(route, id, token, options.leaseSeconds);
    const response = await connection.request(MSG_QUEUE_EXTEND, payload, options.signal);
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

  const complete = async (options: { signal?: AbortSignal } = {}): Promise<void> => {
    ensureOpen();
    const requestPayload = QueueCodec.encodeComplete(route, id, token);
    const response = await connection.request(MSG_QUEUE_COMPLETE, requestPayload, options.signal);
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
  /** Concrete queue route whose counts changed. */
  route: string;
  /** Messages immediately eligible for reservation. */
  readyMessages: bigint;
  /** Messages waiting for their enqueue delay to expire. */
  delayedMessages: bigint;
  /** Messages currently held by uncompleted reservations. */
  inflightMessages: bigint;
}

/**
 * Handles queue availability changes. Notifications are wake signals, not
 * reservations: call {@link QueueClient.reserve} to claim work. The shared
 * async-handler dispatcher controls concurrency and reports callback failures.
 */
export type AvailabilityHandler = (notification: AvailabilityNotification) => void | Promise<void>;

/**
 * Queue availability subscription.
 */
export interface QueueSubscription extends AsyncDisposable {
  /** Stops this handler; shared wire state remains until the last local handler leaves. */
  unsubscribe(): Promise<void>;
}

export function createQueueSubscription(
  unsubscribeFn: () => Promise<void>,
  signal?: AbortSignal,
): QueueSubscription {
  return createSubscriptionHandle<QueueSubscription>(unsubscribeFn, signal);
}

/**
 * Queue operation status codes
 */
export enum QueueStatus {
  /** Operation succeeded. */
  Ok = 0,
  /** Queue route does not exist. */
  QueueNotFound = 1,
  /** Reserved message no longer exists. */
  MessageNotFound = 2,
  /** Reservation token is stale or invalid. */
  InvalidToken = 3,
  /** Queue capacity is exhausted. */
  QueueFull = 4,
  /** Requested delay is outside the supported range. */
  InvalidDelay = 5,
}

/**
 * Options for enqueue operations.
 */
export interface EnqueueOptions {
  /** Message body copied into the enqueue request. */
  body: Uint8Array;
  /** Delay in seconds before reservation eligibility. Omit for immediate availability. */
  delaySeconds?: number;
  /** Cancels waiting for the enqueue response; cancellation may leave the outcome ambiguous. */
  signal?: AbortSignal;
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
