/**
 * Shared buffer for notifications that arrive before their subscription's
 * bookkeeping (subId -> pattern/route -> handlers) is fully registered.
 *
 * notice/client.ts, queue/client.ts, stream/client.ts, and
 * schedule/client.ts each independently reimplemented this exact
 * queue/flush/dispatch-or-queue triad, only the notification type name
 * differing. KV and Lease are deliberately NOT built on this — they drop
 * unknown-subId notifications outright rather than buffering them, a real
 * design difference this helper preserves rather than papering over.
 */
export function createPendingNotificationBuffer<
  TNotification,
  TSubscription extends {
    handlers: Map<number, (notification: TNotification) => void | Promise<void>>;
  },
>(
  resolveSubscription: (subId: bigint) => TSubscription | undefined,
  dispatchToHandler: (
    handler: (notification: TNotification) => void | Promise<void>,
    notification: TNotification,
  ) => void,
) {
  const pendingBySubId = new Map<bigint, TNotification[]>();

  const dispatchNotification = (subscription: TSubscription, notification: TNotification): void => {
    for (const handler of subscription.handlers.values()) {
      dispatchToHandler(handler, notification);
    }
  };

  /** Buffers a notification for a subId with no (yet) known subscription. */
  const queue = (subId: bigint, notification: TNotification): void => {
    const pending = pendingBySubId.get(subId);
    if (pending) {
      pending.push(notification);
      return;
    }
    pendingBySubId.set(subId, [notification]);
  };

  /** Dispatches (and clears) any notifications buffered for `subId`. */
  const flush = (subId: bigint): void => {
    const pending = pendingBySubId.get(subId);
    if (!pending || pending.length === 0) {
      return;
    }

    const subscription = resolveSubscription(subId);
    if (!subscription) {
      return;
    }

    pendingBySubId.delete(subId);
    for (const notification of pending) {
      dispatchNotification(subscription, notification);
    }
  };

  /** Dispatches immediately if the subscription is already known, else buffers it. */
  const dispatchOrQueue = (subId: bigint, notification: TNotification): void => {
    const subscription = resolveSubscription(subId);
    if (!subscription) {
      queue(subId, notification);
      return;
    }
    dispatchNotification(subscription, notification);
  };

  const remove = (subId: bigint): void => {
    pendingBySubId.delete(subId);
  };

  return { queue, flush, dispatchOrQueue, remove };
}
