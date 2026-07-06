/**
 * Schedule domain client.
 */

import { createDomainClient } from "../base";
import type {
  AsyncDispatchPort,
  NotificationPort,
  ReconnectListenerPort,
  ReconnectRestoreRequestPort,
  RequestPort,
} from "../base";
import {
  MSG_SCHEDULE_CANCEL,
  MSG_SCHEDULE_CREATE,
  MSG_SCHEDULE_LIST,
  MSG_SCHEDULE_NOTIFY,
  MSG_SCHEDULE_SUBSCRIBE,
  MSG_SCHEDULE_UNSUBSCRIBE,
} from "../../frame/types";
import { parseStandardResponse } from "../../protocol/response";
import { ScheduleCodec } from "./codec";
import { createWakeGate } from "../../core/wake-gate";
import {
  ScheduleEntry,
  ScheduleError,
  ScheduleHandler,
  ScheduleNotification,
  ScheduleSubscription,
  createScheduleSubscription,
} from "./types";
import { isRouteShape } from "../_routes";
import { restoreMapEntriesAtomically } from "../internal/restore";

type ScheduleSubscriptionState = {
  subId: bigint;
  handlers: Map<number, ScheduleHandler>;
};

type ScheduleConnectionPort = RequestPort &
  ReconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  Partial<ReconnectRestoreRequestPort>;

export type ScheduleClient = ReturnType<typeof createScheduleClient>;

export function createScheduleClient(connection: ScheduleConnectionPort) {
  const { requestFrame, requestReconnectFrame } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, ScheduleSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  const pendingNotificationsBySubId = new Map<bigint, ScheduleNotification[]>();
  let notifyHandlerInitialized = false;
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

  const create = async (
    route: string,
    cronExpr: string,
    payload: Uint8Array = new Uint8Array(),
  ): Promise<string> => {
    assertConcreteScheduleRoute(route);

    const response = await requestFrame(
      MSG_SCHEDULE_CREATE,
      ScheduleCodec.encodeCreate(route, cronExpr, payload),
    );
    const decoded = ScheduleCodec.decodeCreateResponse(assertSuccess(response, "CREATE"));
    return decoded.scheduleId ?? route;
  };

  const cancel = async (route: string): Promise<void> => {
    assertConcreteScheduleRoute(route);

    const response = await requestFrame(MSG_SCHEDULE_CANCEL, ScheduleCodec.encodeCancel(route));
    ScheduleCodec.decodeCancelResponse(assertSuccess(response, "CANCEL"));
  };

  const list = async (
    offset: bigint = 0n,
    limit: bigint = 0n,
  ): Promise<[ScheduleEntry[], bigint]> => {
    const response = await requestFrame(MSG_SCHEDULE_LIST, ScheduleCodec.encodeList(offset, limit));
    const decoded = ScheduleCodec.decodeListResponse(assertSuccess(response, "LIST"));
    return [decoded.entries, decoded.totalCount];
  };

  const waitForNotifications = async function* (
    route: string,
    options: {
      signal?: AbortSignal;
    } = {},
  ): AsyncIterable<ScheduleNotification> {
    assertConcreteScheduleRoute(route);

    const wakeGate = createWakeGate();
    const pendingNotifications: ScheduleNotification[] = [];
    const subscription = await subscribe(route, (notification) => {
      pendingNotifications.push(notification);
      wakeGate.wake();
    });

    try {
      while (true) {
        const notification = pendingNotifications.shift();
        if (notification) {
          yield notification;
          continue;
        }

        const observed = wakeGate.version;
        if (pendingNotifications.length > 0) {
          continue;
        }

        await wakeGate.waitAfter(observed, { signal: options.signal });
      }
    } finally {
      await subscription.unsubscribe().catch(() => undefined);
    }
  };

  const subscribe = async (
    pattern: string,
    handler: ScheduleHandler,
  ): Promise<ScheduleSubscription> => {
    assertConcreteScheduleRoute(pattern);

    initNotifyHandler();
    const existing = subscriptionsByPattern.get(pattern);
    if (existing) {
      return addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await subscribeWire(pattern);
    return addLocalSubscription(pattern, subId, handler);
  };

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const response = await request(MSG_SCHEDULE_SUBSCRIBE, ScheduleCodec.encodeSubscribe(pattern));
    const decoded = ScheduleCodec.decodeSubscribeResponse(assertSuccess(response, "SUBSCRIBE"));

    return decoded.subId;
  };

  const addLocalSubscription = (
    pattern: string,
    subId: bigint,
    handler: ScheduleHandler,
  ): ScheduleSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      subscriptionsByPattern.set(pattern, subscription);
      patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    flushPendingNotifications(subId);
    return createScheduleSubscription(subId, pattern, async () => {
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
    const response = await requestFrame(
      MSG_SCHEDULE_UNSUBSCRIBE,
      ScheduleCodec.encodeUnsubscribe(pattern),
    );
    ScheduleCodec.decodeUnsubscribeResponse(assertSuccess(response, "UNSUBSCRIBE"));
  };

  const initNotifyHandler = (): void => {
    if (notifyHandlerInitialized) {
      return;
    }

    notifyHandlerInitialized = true;
    connection.registerNotificationHandler(MSG_SCHEDULE_NOTIFY, (payload) => {
      try {
        const decoded = ScheduleCodec.decodeNotification(payload);
        const pattern = patternsBySubId.get(decoded.subId);
        if (!pattern) {
          queuePendingNotification(decoded.subId, {
            payload: decoded.payload,
          });
          return;
        }

        const subscription = subscriptionsByPattern.get(pattern);
        if (!subscription) {
          queuePendingNotification(decoded.subId, {
            payload: decoded.payload,
          });
          return;
        }

        const notification: ScheduleNotification = {
          payload: decoded.payload,
        };
        dispatchNotification(subscription, notification);
      } catch {
        // Best-effort notification dispatch.
      }
    });
  };

  const queuePendingNotification = (subId: bigint, notification: ScheduleNotification): void => {
    const existing = pendingNotificationsBySubId.get(subId);
    if (existing) {
      existing.push(notification);
      return;
    }

    pendingNotificationsBySubId.set(subId, [notification]);
  };

  const flushPendingNotifications = (subId: bigint): void => {
    const pending = pendingNotificationsBySubId.get(subId);
    if (!pending || pending.length === 0) {
      return;
    }

    pendingNotificationsBySubId.delete(subId);
    const subscription = Array.from(subscriptionsByPattern.values()).find(
      (entry) => entry.subId === subId,
    );
    if (!subscription) {
      return;
    }

    for (const notification of pending) {
      dispatchNotification(subscription, notification);
    }
  };

  const dispatchNotification = (
    subscription: ScheduleSubscriptionState,
    notification: ScheduleNotification,
  ): void => {
    for (const handler of subscription.handlers.values()) {
      connection.dispatchAsyncHandler(async () => {
        await handler(notification);
      });
    }
  };

  const assertSuccess = (payload: Uint8Array, operation: string): Uint8Array => {
    const result = parseStandardResponse(payload);
    if (result.success) {
      return result.data;
    }

    throw new ScheduleError(
      `${operation} failed: ${result.error ?? "Unknown error"}`,
      mapErrorCode(result.error),
    );
  };

  const mapErrorCode = (message?: string): string => {
    const normalized = message?.toLowerCase() ?? "";
    if (normalized.includes("not found")) {
      return "NOT_FOUND";
    }
    if (normalized.includes("invalid route")) {
      return "INVALID_ROUTE";
    }
    if (normalized.includes("cron")) {
      return "INVALID_CRON";
    }
    return "REQUEST_FAILED";
  };

  return {
    create,
    cancel,
    list,
    subscribe,
    waitForNotifications,
  };
}

type ScheduleClientConstructor = {
  new (connection: ScheduleConnectionPort): ScheduleClient;
  (connection: ScheduleConnectionPort): ScheduleClient;
};

export const ScheduleClient: ScheduleClientConstructor = function (
  connection: ScheduleConnectionPort,
) {
  return createScheduleClient(connection);
} as unknown as ScheduleClientConstructor;

export * from "./types";

function assertConcreteScheduleRoute(route: string): void {
  if (!isRouteShape(route, "schedule", 4)) {
    throw new ScheduleError(
      `Invalid schedule route: ${route} (expected schedule://{realm}/{area}/{resource}/{operation}, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}
