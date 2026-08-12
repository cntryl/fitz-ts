/**
 * Notice domain types (Pub/Sub)
 * Per fitz-go/internal/domains/notice/notice.go
 */

import { createSubscriptionHandle } from "../internal/subscription-handle";

/**
 * Received notification message
 */
export interface NoticeMsg {
  route: string;
  body: Uint8Array;
}

/**
 * Handler for incoming notifications
 */
export type NoticeHandler = (msg: NoticeMsg) => Promise<void> | void;

/**
 * Active notice subscription
 */
export interface NoticeSubscription extends AsyncDisposable {
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
  Ok = 0,
}
