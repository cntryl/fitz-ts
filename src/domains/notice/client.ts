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
  let initialized = false;
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
      const subId = await subscribeWire(subscription.pattern, requestReconnectFrame);
      subscriptionsByPattern.set(subscription.pattern, {
        subId,
        handlers: new Map(subscription.handlers),
      });
      patternsBySubId.set(subId, subscription.pattern);
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
          return;
        }

        const subscription = subscriptionsByPattern.get(pattern);
        if (!subscription) {
          return;
        }

        const msg: NoticeMsg = { route, body };
        for (const handler of subscription.handlers.values()) {
          connection.dispatchAsyncHandler(async () => {
            await handler(msg);
          });
        }
      } catch {
        // Best-effort notification dispatch.
      }
    });
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
