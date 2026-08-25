/**
 * Notice domain types (Pub/Sub)
 * Per fitz-go/internal/domains/notice/notice.go
 */

import { createSubscriptionHandle } from "../internal/subscription-handle";

/**
 * Received notification message
 */
export interface NoticeMsg {
  /** Concrete notice route that was published. */
  route: string;
  /** Publisher-supplied payload. */
  body: Uint8Array;
}

/**
 * Handles an ephemeral notice. Delivery is not durable and callback completion
 * is not acknowledged to the publisher. The shared async-handler dispatcher
 * controls concurrency and reports callback failures.
 */
export type NoticeHandler = (msg: NoticeMsg) => Promise<void> | void;

/**
 * Active notice subscription
 */
export interface NoticeSubscription extends AsyncDisposable {
  /** Resolves after unsubscribe; rejects with `AsyncHandlerOverflowError` on local overflow. */
  readonly completion: Promise<void>;
  /** Removes this local consumer; shared wire state closes after the last consumer leaves. */
  unsubscribe(): Promise<void>;
}

export function createNoticeSubscription(
  unsubscribeFn: () => Promise<void>,
  signal?: AbortSignal,
): NoticeSubscription {
  return createSubscriptionHandle<NoticeSubscription>(unsubscribeFn, signal);
}

/**
 * Response to SUBSCRIBE request
 */
export interface SubscribeResponse {
  status: number;
  subId?: bigint;
}

/**
 * Response to UNSUBSCRIBE request
 */
export interface UnsubscribeResponse {
  status: number;
}

/**
 * Notice status codes
 */
export enum NoticeStatus {
  /** Operation succeeded. */
  Ok = 0,
}
