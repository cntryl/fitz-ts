/**
 * Notice domain client.
 */

import { createDomainClient } from "../base";
import type {
  AsyncDispatchPort,
  FireAndForgetPort,
  NotificationPort,
  OptionalResponsePort,
  ReconnectListenerPort,
  ReconnectRestoreRequestPort,
  RequestPort,
} from "../base";
import { NoticeError } from "../../core/errors";
import {
  MSG_NOTICE_NOTIFY,
  MSG_NOTICE_PUBLISH,
  MSG_NOTICE_SUBSCRIBE,
  MSG_NOTICE_UNSUBSCRIBE,
} from "../../frame/types";
import { isRouteShape, isSelectorRouteShape } from "../_routes";
import { restoreMapEntriesAtomically } from "../internal/restore";
import { NoticeCodec } from "./codec";
import { createNoticeSubscription, NoticeHandler, NoticeMsg, NoticeSubscription } from "./types";

type NoticeSubscriptionState = {
  subId: bigint;
  handlers: Map<number, NoticeHandler>;
};

type NoticeConnectionPort = RequestPort &
  ReconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  FireAndForgetPort &
  OptionalResponsePort &
  Partial<ReconnectRestoreRequestPort>;

export type NoticeClient = ReturnType<typeof createNoticeClient>;

export function createNoticeClient(connection: NoticeConnectionPort) {
  const { requestFrame, requestReconnectFrame, expectOptionalResponse } =
    createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, NoticeSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  const pendingNotificationsBySubId = new Map<bigint, NoticeMsg[]>();
  let initialized = false;
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

  const publish = async (route: string, body: Uint8Array): Promise<void> => {
    assertNoticeRoute(route);
    const payload = NoticeCodec.encodePublish(route, body);
    const cancelOptionalResponse = expectOptionalResponse(MSG_NOTICE_PUBLISH);
    try {
      await connection.sendFireAndForget(MSG_NOTICE_PUBLISH, payload);
    } catch (error) {
      cancelOptionalResponse();
      throw error;
    }
  };

  const subscribe = async (
    pattern: string,
    handler: NoticeHandler,
  ): Promise<NoticeSubscription> => {
    assertNoticePattern(pattern);
    initNotifyHandler();
    const existing = subscriptionsByPattern.get(pattern);
    if (existing) {
      return addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await subscribeWire(pattern);
    return addLocalSubscription(pattern, subId, handler);
  };

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const payload = NoticeCodec.encodeSubscribe(pattern);
    const response = await request(MSG_NOTICE_SUBSCRIBE, payload);
    const decoded = NoticeCodec.decodeSubscribeResponse(response);

    if (decoded.subId === undefined) {
      throw new NoticeError("SUBSCRIBE response missing subId", "MISSING_SUB_ID");
    }

    return decoded.subId;
  };

  const addLocalSubscription = (
    pattern: string,
    subId: bigint,
    handler: NoticeHandler,
  ): NoticeSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      subscriptionsByPattern.set(pattern, subscription);
      patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    flushPendingNotifications(subId);
    return createNoticeSubscription(subId, pattern, async (_subId: bigint) => {
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
    const payload = NoticeCodec.encodeUnsubscribe(subscription.subId);
    await requestFrame(MSG_NOTICE_UNSUBSCRIBE, payload);
  };

  const initNotifyHandler = (): void => {
    if (initialized) {
      return;
    }

    initialized = true;
    connection.registerNotificationHandler(MSG_NOTICE_NOTIFY, (payload) => {
      try {
        const { subId, route, body } = NoticeCodec.decodeNotification(payload);
        const pattern = patternsBySubId.get(subId);
        if (!pattern) {
          queuePendingNotification(subId, { route, body });
          return;
        }

        const subscription = subscriptionsByPattern.get(pattern);
        if (!subscription) {
          queuePendingNotification(subId, { route, body });
          return;
        }

        dispatchNotification(subscription, { route, body });
      } catch {
        // Best-effort notification dispatch.
      }
    });
  };

  const queuePendingNotification = (subId: bigint, notification: NoticeMsg): void => {
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
    subscription: NoticeSubscriptionState,
    notification: NoticeMsg,
  ): void => {
    for (const handler of subscription.handlers.values()) {
      connection.dispatchAsyncHandler(async () => {
        await handler(notification);
      });
    }
  };

  return {
    publish,
    subscribe,
  };
}

type NoticeClientConstructor = {
  new (connection: NoticeConnectionPort): NoticeClient;
  (connection: NoticeConnectionPort): NoticeClient;
};

export const NoticeClient: NoticeClientConstructor = function (connection: NoticeConnectionPort) {
  return createNoticeClient(connection);
} as unknown as NoticeClientConstructor;

export * from "./types";

function assertNoticeRoute(route: string): void {
  if (!isRouteShape(route, "notice", 3)) {
    throw new NoticeError(
      `Invalid notice route: ${route} (expected notice://{realm}/{area}/{resource}, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}

function assertNoticePattern(pattern: string): void {
  if (!isSelectorRouteShape(pattern, "notice", 3, { allowRealmWildcard: true })) {
    throw new NoticeError(
      `Invalid notice pattern: ${pattern} (expected notice://{realm}/{area}/{resource}, notice://{realm}/{area}/*, or notice://{realm}/**)`,
      "INVALID_ROUTE",
    );
  }
}
