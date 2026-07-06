/**
 * Queue domain client.
 */

import { createDomainClient } from "../base";
import type {
  AsyncDispatchPort,
  DisconnectListenerPort,
  NotificationPort,
  ReconnectListenerPort,
  ReconnectRestoreRequestPort,
  RequestPort,
  RetryExecutionPort,
} from "../base";
import { attachResilienceMeta } from "../../client/resilience";
import { QueueError } from "../../core/errors";
import { createWakeGate } from "../../core/wake-gate";
import {
  MSG_QUEUE_ENQUEUE,
  MSG_QUEUE_NOTIFY,
  MSG_QUEUE_RESERVE,
  MSG_QUEUE_SUBSCRIBE,
  MSG_QUEUE_UNSUBSCRIBE,
} from "../../frame/types";
import { isRouteShape, isSelectorRouteShape } from "../_routes";
import { restoreMapEntriesAtomically } from "../internal/restore";
import { formatStatusName } from "../internal/status";
import { QueueCodec } from "./codec";
import {
  AvailabilityHandler,
  AvailabilityNotification,
  EnqueueOptions,
  QueueItem,
  QueueStatus,
  QueueSubscription,
  createQueueItem,
  createQueueSubscription,
} from "./types";

type QueueSubscriptionState = {
  subId: bigint;
  handlers: Map<number, AvailabilityHandler>;
};

type QueueConnectionPort = RequestPort &
  ReconnectListenerPort &
  DisconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  RetryExecutionPort &
  Partial<ReconnectRestoreRequestPort>;

export type QueueClient = ReturnType<typeof createQueueClient>;

export function createQueueClient(connection: QueueConnectionPort) {
  const { requestFrame, requestReconnectFrame, runWithRetry } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, QueueSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  const pendingNotificationsBySubId = new Map<bigint, AvailabilityNotification[]>();
  let notificationHandlerRegistered = false;
  let nextHandlerId = 1;

  connection.onReconnect(async () => {
    if (subscriptionsByPattern.size === 0) {
      return;
    }

    await restoreMapEntriesAtomically(subscriptionsByPattern, async (pattern, state) => {
      const subId = await subscribeWire(pattern, requestReconnectFrame);
      return {
        subId,
        handlers: new Map(state.handlers),
      };
    });

    patternsBySubId.clear();
    for (const [pattern, state] of subscriptionsByPattern) {
      patternsBySubId.set(state.subId, pattern);
      flushPendingNotifications(state.subId);
    }
  });

  const enqueue = async (
    route: string,
    body: Uint8Array,
    options?: EnqueueOptions,
  ): Promise<bigint> => {
    assertQueueRoute(route);
    return runWithRetry(
      {
        domain: "queue",
        operation: "enqueue",
        retryClass: "confirmed_negative_retry",
      },
      async () => {
        const payload = QueueCodec.encodeEnqueue(route, body, options);
        const response = await requestFrame(MSG_QUEUE_ENQUEUE, payload);
        const decoded = QueueCodec.decodeEnqueueResponse(response);
        checkStatus(decoded, "ENQUEUE");

        if (decoded.messageId === undefined) {
          throw new QueueError("ENQUEUE response missing messageId", "MISSING_MESSAGE_ID");
        }

        return decoded.messageId;
      },
    );
  };

  const reserve = async (
    route: string,
    leaseSeconds: number,
    batchSize: number = 1,
    waitSeconds: number = 0,
  ): Promise<QueueItem[]> => {
    assertQueueReserveRoute(route);
    if (waitSeconds <= 0) {
      return reserveOnce(route, leaseSeconds, batchSize);
    }

    let items = await reserveOnce(route, leaseSeconds, batchSize);
    if (items.length > 0) {
      return items;
    }

    const deadline = Date.now() + waitSeconds * 1000;
    const wakeGate = createWakeGate();
    const subscription = await subscribe(route, () => {
      wakeGate.wake();
    });

    try {
      while (true) {
        const observed = wakeGate.version;
        items = await reserveOnce(route, leaseSeconds, batchSize);
        if (items.length > 0) {
          return items;
        }

        const remainingMs = deadline - Date.now();
        if (remainingMs <= 0) {
          return items;
        }

        const waitPromise = wakeGate.waitAfter(observed);
        let timeoutId: ReturnType<typeof setTimeout> | null = null;
        const timeoutPromise = new Promise<"timeout">((resolve) => {
          timeoutId = setTimeout(() => {
            resolve("timeout");
          }, remainingMs);
        });

        const result = await Promise.race([
          waitPromise.then(() => {
            if (timeoutId) {
              clearTimeout(timeoutId);
            }
            return "wake" as const;
          }),
          timeoutPromise,
        ]);

        if (result === "timeout") {
          return items;
        }
      }
    } finally {
      await subscription.unsubscribe().catch(() => undefined);
    }
  };

  const reserveWhenAvailable = async function* (
    route: string,
    options: {
      leaseSeconds: number;
      batchSize?: number;
      signal?: AbortSignal;
    },
  ): AsyncIterable<QueueItem[]> {
    assertQueueReserveRoute(route);

    const wakeGate = createWakeGate();
    const subscription = await subscribe(route, () => {
      wakeGate.wake();
    });
    const unsubscribeReconnectWake = connection.onReconnect(() => {
      wakeGate.wake();
    });

    try {
      while (true) {
        const observed = wakeGate.version;
        const items = await reserveOnce(
          route,
          options.leaseSeconds,
          options.batchSize ?? 1,
          options.signal,
        );

        if (items.length > 0) {
          yield items;
          continue;
        }

        await wakeGate.waitAfter(observed, { signal: options.signal });
      }
    } finally {
      unsubscribeReconnectWake();
      await subscription.unsubscribe().catch(() => undefined);
    }
  };

  const reserveOnce = async (
    route: string,
    leaseSeconds: number,
    batchSize: number,
    signal?: AbortSignal,
  ): Promise<QueueItem[]> => {
    const payload = QueueCodec.encodeReserve(route, leaseSeconds, batchSize);
    const response = await requestFrame(MSG_QUEUE_RESERVE, payload, signal);
    const decoded = QueueCodec.decodeReserveResponse(response);
    checkStatus(decoded, "RESERVE");

    return (decoded.items ?? []).map((item) =>
      createQueueItem(item.id, item.token, item.body, route, connection),
    );
  };

  const subscribe = async (
    pattern: string,
    handler: AvailabilityHandler,
  ): Promise<QueueSubscription> => {
    assertQueueSubscriptionPattern(pattern);
    initNotificationHandler();
    const existing = subscriptionsByPattern.get(pattern);
    if (existing) {
      return addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await subscribeWire(pattern);
    return addLocalSubscription(pattern, subId, handler);
  };

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const payload = QueueCodec.encodeSubscribe(wireWatchPattern(pattern));
    const response = await request(MSG_QUEUE_SUBSCRIBE, payload);
    const decoded = QueueCodec.decodeSubscribeResponse(response);
    checkStatus(decoded, "SUBSCRIBE");

    if (decoded.subId === undefined) {
      throw new QueueError("SUBSCRIBE response missing subId", "MISSING_SUB_ID");
    }

    return decoded.subId;
  };

  const addLocalSubscription = (
    pattern: string,
    subId: bigint,
    handler: AvailabilityHandler,
  ): QueueSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      subscriptionsByPattern.set(pattern, subscription);
      patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    flushPendingNotifications(subId);

    return createQueueSubscription(subId, pattern, async () => {
      await unsubscribe(pattern, handlerId);
    });
  };

  const unsubscribe = async (pattern: string, handlerId: number): Promise<void> => {
    const subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      return;
    }

    subscription.handlers.delete(handlerId);
    if (subscription.handlers.size > 0) {
      return;
    }

    subscriptionsByPattern.delete(pattern);
    patternsBySubId.delete(subscription.subId);
    pendingNotificationsBySubId.delete(subscription.subId);
    const payload = QueueCodec.encodeUnsubscribe(wireWatchPattern(pattern));
    const response = await requestFrame(MSG_QUEUE_UNSUBSCRIBE, payload);
    const decoded = QueueCodec.decodeUnsubscribeResponse(response);
    checkStatus(decoded, "UNSUBSCRIBE");
  };

  const initNotificationHandler = (): void => {
    if (notificationHandlerRegistered) {
      return;
    }

    notificationHandlerRegistered = true;
    connection.registerNotificationHandler(MSG_QUEUE_NOTIFY, (payload) => {
      try {
        const { subId, route } = QueueCodec.decodeNotification(payload);
        const notification: AvailabilityNotification = { route: publicQueueRoute(route) };
        const pattern = patternsBySubId.get(subId);
        if (!pattern) {
          queuePendingNotification(subId, notification);
          return;
        }

        const subscription = subscriptionsByPattern.get(pattern);
        if (!subscription) {
          queuePendingNotification(subId, notification);
          return;
        }

        dispatchNotification(subscription, notification);
      } catch {
        // Best-effort notification dispatch.
      }
    });
  };

  const queuePendingNotification = (
    subId: bigint,
    notification: AvailabilityNotification,
  ): void => {
    const pending = pendingNotificationsBySubId.get(subId);
    if (pending) {
      pending.push(notification);
      return;
    }

    pendingNotificationsBySubId.set(subId, [notification]);
  };

  const flushPendingNotifications = (subId: bigint): void => {
    const pending = pendingNotificationsBySubId.get(subId);
    if (!pending || pending.length === 0) {
      return;
    }

    const pattern = patternsBySubId.get(subId);
    const subscription = pattern === undefined ? undefined : subscriptionsByPattern.get(pattern);
    if (!subscription) {
      return;
    }

    pendingNotificationsBySubId.delete(subId);
    for (const notification of pending) {
      dispatchNotification(subscription, notification);
    }
  };

  const dispatchNotification = (
    subscription: QueueSubscriptionState,
    notification: AvailabilityNotification,
  ): void => {
    for (const handler of subscription.handlers.values()) {
      connection.dispatchAsyncHandler(async () => {
        await handler(notification);
      });
    }
  };

  const wireWatchPattern = (pattern: string): string => {
    if (pattern.endsWith("/ready")) {
      return pattern;
    }

    return `${pattern}/ready`;
  };

  const publicQueueRoute = (route: string): string => {
    if (!route.endsWith("/ready")) {
      return route;
    }

    return route.slice(0, -"/ready".length);
  };

  const checkStatus = (
    response: { status: number; errorCode?: number; errorMessage?: string },
    operation: string,
  ): void => {
    if (response.status === QueueStatus.Ok) {
      return;
    }

    const errorCode = response.errorCode ?? response.status;
    const statusNames: Record<number, string> = {
      [QueueStatus.QueueNotFound]: "QueueNotFound",
      [QueueStatus.MessageNotFound]: "MessageNotFound",
      [QueueStatus.InvalidToken]: "InvalidToken",
      [QueueStatus.QueueFull]: "QueueFull",
      [QueueStatus.InvalidDelay]: "InvalidDelay",
    };

    const statusName = formatStatusName(errorCode, statusNames);
    const reason = response.errorMessage ?? statusName;

    throw attachResilienceMeta(
      new QueueError(`${operation} failed: ${reason}`, statusName, errorCode),
      {
        boundary: "post-send",
        failureKind: "domain",
        explicitNegative: true,
      },
    );
  };

  return {
    enqueue,
    reserve,
    reserveWhenAvailable,
    subscribe,
  };
}

export const QueueClient = createQueueClient;

export * from "./types";

function assertQueueRoute(route: string): void {
  if (!isRouteShape(route, "queue", 3)) {
    throw new QueueError(
      `Invalid queue route: ${route} (expected queue://{realm}/{area}/{resource}, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}

function assertQueueReserveRoute(route: string): void {
  if (!isSelectorRouteShape(route, "queue", 3)) {
    throw new QueueError(
      `Invalid queue route: ${route} (expected queue://{realm}/{area}/{resource} or queue://{realm}/{area}/*, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}

function assertQueueSubscriptionPattern(pattern: string): void {
  if (!isSelectorRouteShape(pattern, "queue", 3, { allowRealmWildcard: true })) {
    throw new QueueError(
      `Invalid queue pattern: ${pattern} (expected queue://{realm}/{area}/{resource}, queue://{realm}/{area}/*, or queue://{realm}/**)`,
      "INVALID_ROUTE",
    );
  }
}
