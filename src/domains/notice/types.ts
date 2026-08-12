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
export interface NoticeSubscription extends AsyncDisposable {
  unsubscribe(): Promise<void>;
}

export function createNoticeSubscription(unsubscribeFn: () => Promise<void>): NoticeSubscription {
  let active = true;
  let pending: Promise<void> | undefined;
  const unsubscribe = async (): Promise<void> => {
    if (!active) return pending;
    active = false;
    pending = unsubscribeFn().catch((error: unknown) => {
      active = true;
      throw error;
    });
    try {
      await pending;
    } finally {
      pending = undefined;
    }
  };

  return {
    unsubscribe,
    async [Symbol.asyncDispose](): Promise<void> {
      try {
        await unsubscribe();
      } catch {
        // Disposal is explicitly best effort.
      }
    },
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
