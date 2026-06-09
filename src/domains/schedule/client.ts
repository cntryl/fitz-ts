/**
 * Schedule domain client.
 */

import { createDomainClient } from "../base";
import type { Connection } from "../../client/connection";
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
import {
  ScheduleEntry,
  ScheduleError,
  ScheduleHandler,
  ScheduleNotification,
  ScheduleSubscription,
  createScheduleSubscription,
} from "./types";
import { isRouteShape } from "../_routes";

type ScheduleSubscriptionState = {
  subId: bigint;
  handlers: Map<number, ScheduleHandler>;
};

export type ScheduleClient = ReturnType<typeof createScheduleClient>;

export function createScheduleClient(connection: Connection) {
  const { requestFrame, requestReconnectFrame } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, ScheduleSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  let notifyHandlerInitialized = false;
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
          return;
        }

        const subscription = subscriptionsByPattern.get(pattern);
        if (!subscription) {
          return;
        }

        const notification: ScheduleNotification = {
          payload: decoded.payload,
        };
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
  };
}

type ScheduleClientConstructor = {
  new (connection: Connection): ScheduleClient;
  (connection: Connection): ScheduleClient;
};

export const ScheduleClient: ScheduleClientConstructor = function (connection: Connection) {
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
