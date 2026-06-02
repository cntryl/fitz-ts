/**
 * Notice domain types (Pub/Sub)
 * Per fitz-go/internal/domains/notice/notice.go
 */

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
export type NoticeSubscription = ReturnType<typeof createNoticeSubscription>;

export function createNoticeSubscription(
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
