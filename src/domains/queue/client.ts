/**
 * Queue domain client.
 */

import { createDomainClient } from "../base";
import { attachResilienceMeta } from "../../client/resilience";
import type { Connection } from "../../client/connection";
import { QueueError } from "../../core/errors";
import {
  MSG_QUEUE_ENQUEUE,
  MSG_QUEUE_NOTIFY,
  MSG_QUEUE_RESERVE,
  MSG_QUEUE_SUBSCRIBE,
  MSG_QUEUE_UNSUBSCRIBE,
} from "../../frame/types";
import { isRouteShape, isSelectorRouteShape } from "../_routes";
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

export type QueueClient = ReturnType<typeof createQueueClient>;

export function createQueueClient(connection: Connection) {
  const { requestFrame, runWithRetry } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, QueueSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  let notificationHandlerRegistered = false;
  let nextHandlerId = 1;

  connection.onReconnect(async () => {
    if (subscriptionsByPattern.size === 0) {
      return;
    }

    const subscriptions = Array.from(subscriptionsByPattern.entries(), ([pattern, state]) => ({
      pattern,
      handlers: Array.from(state.handlers.entries()),
    }));
    subscriptionsByPattern.clear();
    patternsBySubId.clear();

    for (const subscription of subscriptions) {
      const subId = await subscribeWire(subscription.pattern);
      subscriptionsByPattern.set(subscription.pattern, {
        subId,
        handlers: new Map(subscription.handlers),
      });
      patternsBySubId.set(subId, subscription.pattern);
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
    let pendingNotifications = 0;
    let waiter: (() => void) | undefined;
    const subscription = await subscribe(route, async () => {
      pendingNotifications += 1;
      if (!waiter) {
        return;
      }

      const resolve = waiter;
      waiter = undefined;
      pendingNotifications = 0;
      resolve();
    });

    try {
      while (true) {
        items = await reserveOnce(route, leaseSeconds, batchSize);
        if (items.length > 0) {
          return items;
        }

        const remainingMs = deadline - Date.now();
        if (remainingMs <= 0) {
          return items;
        }

        await new Promise<void>((resolve) => {
          if (pendingNotifications > 0) {
            pendingNotifications = 0;
            resolve();
            return;
          }

          const release = () => {
            clearTimeout(timeoutId);
            if (waiter === release) {
              waiter = undefined;
            }
            resolve();
          };
          const timeoutId = setTimeout(release, remainingMs);
          waiter = release;
        });
      }
    } finally {
      await subscription.unsubscribe();
    }
  };

  const reserveOnce = async (
    route: string,
    leaseSeconds: number,
    batchSize: number,
  ): Promise<QueueItem[]> => {
    const payload = QueueCodec.encodeReserve(route, leaseSeconds, batchSize);
    const response = await requestFrame(MSG_QUEUE_RESERVE, payload);
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

  const subscribeWire = async (pattern: string): Promise<bigint> => {
    const payload = QueueCodec.encodeSubscribe(wireWatchPattern(pattern));
    const response = await requestFrame(MSG_QUEUE_SUBSCRIBE, payload);
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
        const pattern = patternsBySubId.get(subId);
        if (!pattern) {
          return;
        }

        const subscription = subscriptionsByPattern.get(pattern);
        if (!subscription) {
          return;
        }

        const notification: AvailabilityNotification = { route: publicQueueRoute(route) };
        for (const handler of subscription.handlers.values()) {
          connection.dispatchAsyncHandler(async () => {
            await handler(notification);
          });
        }
      } catch {
        // Best-effort notification dispatch.
      }
    });
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

    const statusName = statusNames[errorCode] ?? `Unknown(${errorCode})`;
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
    subscribe,
  };
}

type QueueClientConstructor = {
  new (connection: Connection): QueueClient;
  (connection: Connection): QueueClient;
};

export const QueueClient: QueueClientConstructor = function (connection: Connection) {
  return createQueueClient(connection);
} as unknown as QueueClientConstructor;

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
