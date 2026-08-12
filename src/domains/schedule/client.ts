/**
 * Schedule domain client.
 */

import { createDomainClient } from "../base";
import type {
  AsyncDispatchPort,
  BackgroundErrorPort,
  DisconnectListenerPort,
  NotificationPort,
  ReconnectListenerPort,
  ReconnectRestoreRequestPort,
  RequestPort,
} from "../base";
import {
  MSG_SCHEDULE_CANCEL,
  MSG_SCHEDULE_CREATE,
  MSG_SCHEDULE_LIST_PAGE,
  MSG_SCHEDULE_NOTIFY,
  MSG_SCHEDULE_SUBSCRIBE,
  MSG_SCHEDULE_UNSUBSCRIBE,
} from "../../frame/types";
import { parsePlainResponse, parseStandardResponse } from "../../protocol/response";
import { ScheduleCodec } from "./codec";
import {
  ScheduleEntry,
  ScheduleDeliveryMode,
  ScheduleHandler,
  ScheduleNotification,
  ScheduleSubscription,
  ScheduleListPage,
  ScheduleStatusNames,
  createScheduleSubscription,
} from "./types";
import { ScheduleError } from "../../core/errors";
import { formatStatusName } from "../internal/status";
import { isRegistrationPatternShape, isRouteShape } from "../_routes";
import { restoreMapEntriesAtomically } from "../internal/restore";
import { createKeyedSingleFlight } from "../internal/keyed-single-flight";
import {
  awaitPendingUnsubscribe,
  createGenerationCounter,
  isCurrentEmptyState,
} from "../internal/subscription-handle";
import { createPendingNotificationBuffer } from "../internal/pending-notifications";
import {
  createSubscriptionIterator,
  type SubscriptionIteratorOptions,
} from "../internal/subscription-iterator";

type ScheduleSubscriptionState = {
  subId: bigint;
  handlers: Map<number, ScheduleHandler>;
  generation: number;
  // Set while a wire UNSUBSCRIBE for this pattern is awaiting its broker
  // round-trip. subscribe()'s "reuse the existing state" path must wait it
  // out rather than reuse it blindly — see awaitPendingUnsubscribe().
  pendingUnsubscribe?: Promise<void>;
};

type ScheduleConnectionPort = RequestPort &
  Partial<BackgroundErrorPort> &
  ReconnectListenerPort &
  DisconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  Partial<ReconnectRestoreRequestPort>;

export interface ScheduleClient {
  create(
    route: string,
    options: {
      cron: string;
      deliveryMode: ScheduleDeliveryMode;
      payload?: Uint8Array;
      signal?: AbortSignal;
    },
  ): Promise<void>;
  cancel(route: string, options?: { signal?: AbortSignal }): Promise<void>;
  entries(
    selector: string,
    options?: { pageSize?: bigint; signal?: AbortSignal },
  ): AsyncIterableIterator<readonly ScheduleEntry[]>;
  subscribe(
    pattern: string,
    handler: ScheduleHandler,
    options?: { signal?: AbortSignal },
  ): Promise<ScheduleSubscription>;
  notifications(
    pattern: string,
    options?: SubscriptionIteratorOptions,
  ): AsyncIterable<ScheduleNotification>;
}

export function createScheduleClient(connection: ScheduleConnectionPort): ScheduleClient {
  const registerSingleFlight = createKeyedSingleFlight<string, bigint>();
  const { requestFrame, requestReconnectFrame } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, ScheduleSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  const subIdGeneration = createGenerationCounter();
  const pendingNotifications = createPendingNotificationBuffer<
    ScheduleNotification,
    ScheduleSubscriptionState
  >(
    (subId) => {
      const pattern = patternsBySubId.get(subId);
      return pattern === undefined ? undefined : subscriptionsByPattern.get(pattern);
    },
    (handler, notification) => {
      connection.dispatchAsyncHandler(async () => {
        await handler(notification);
      });
    },
  );
  let notifyHandlerInitialized = false;
  let nextHandlerId = 1;

  connection.onReconnect(async () => {
    if (subscriptionsByPattern.size === 0) {
      return;
    }

    await restoreMapEntriesAtomically(
      subscriptionsByPattern,
      async (pattern, state) => {
        const subId = await subscribeWire(pattern, requestReconnectFrame);
        // Carry the generation forward: this is the same logical
        // subscription surviving reconnect, not a new one.
        return { subId, handlers: new Map(state.handlers), generation: state.generation };
      },
      async (pattern) => {
        assertPlainSuccess(
          await requestReconnectFrame(
            MSG_SCHEDULE_UNSUBSCRIBE,
            ScheduleCodec.encodeUnsubscribe(pattern),
          ),
          "UNSUBSCRIBE",
        );
      },
    );

    patternsBySubId.clear();
    for (const [pattern, state] of subscriptionsByPattern) {
      patternsBySubId.set(state.subId, pattern);
      pendingNotifications.flush(state.subId);
    }
  });

  const create = async (
    route: string,
    options: {
      cron: string;
      deliveryMode: ScheduleDeliveryMode;
      payload?: Uint8Array;
      signal?: AbortSignal;
    },
  ): Promise<void> => {
    assertConcreteScheduleRoute(route);

    const response = await requestFrame(
      MSG_SCHEDULE_CREATE,
      ScheduleCodec.encodeCreate(
        route,
        options.cron,
        options.deliveryMode,
        options.payload ?? new Uint8Array(),
      ),
      options.signal,
    );
    ScheduleCodec.decodeCreateResponse(assertPlainSuccess(response, "CREATE"));
  };

  const cancel = async (route: string, options: { signal?: AbortSignal } = {}): Promise<void> => {
    assertConcreteScheduleRoute(route);

    const response = await requestFrame(
      MSG_SCHEDULE_CANCEL,
      ScheduleCodec.encodeCancel(route),
      options.signal,
    );
    ScheduleCodec.decodeCancelResponse(assertPlainSuccess(response, "CANCEL"));
  };

  const listPage = async (options: {
    offset?: bigint;
    limit?: bigint;
    signal?: AbortSignal;
  }): Promise<ScheduleListPage> => {
    const response = await requestFrame(
      MSG_SCHEDULE_LIST_PAGE,
      ScheduleCodec.encodeListPage(options.offset, options.limit),
      options.signal,
    );
    return ScheduleCodec.decodeListPage(assertSuccess(response, "LIST_PAGE"));
  };

  const entries = async function* (
    selector: string,
    options: { pageSize?: bigint; signal?: AbortSignal } = {},
  ): AsyncIterableIterator<readonly ScheduleEntry[]> {
    if (!isScheduleSelector(selector))
      throw new ScheduleError("invalid schedule selector", "INVALID_ROUTE");
    let offset = 0n;
    let complete = false;
    while (!complete) {
      const page = await listPage({ offset, limit: options.pageSize, signal: options.signal });
      yield Object.freeze(
        page.entries.filter((entry) => routeMatchesSchedule(entry.route, selector)),
      );
      offset += BigInt(page.entries.length);
      complete = offset >= page.totalCount || page.entries.length === 0;
    }
  };

  const subscribe = async (
    pattern: string,
    handler: ScheduleHandler,
    options?: { signal?: AbortSignal },
  ): Promise<ScheduleSubscription> => {
    assertSchedulePattern(pattern);

    initNotifyHandler();

    while (true) {
      const existing = subscriptionsByPattern.get(pattern);
      if (existing) {
        if (existing.pendingUnsubscribe) {
          // An UNSUBSCRIBE for this pattern is in flight — reusing this
          // state now would register the handler locally without ever
          // sending a fresh wire SUBSCRIBE. Wait it out, then re-decide
          // against whatever state (or lack of one) remains.
          await awaitPendingUnsubscribe(existing);
          continue;
        }
        return addLocalSubscription(pattern, existing.subId, handler, options?.signal);
      }

      const subId = await registerSingleFlight(pattern, () => subscribeWire(pattern));
      return addLocalSubscription(pattern, subId, handler, options?.signal);
    }
  };

  const notifications = (
    pattern: string,
    iteratorOptions?: SubscriptionIteratorOptions,
  ): AsyncIterable<ScheduleNotification> =>
    createSubscriptionIterator((handler) => subscribe(pattern, handler), iteratorOptions);

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const response = await request(MSG_SCHEDULE_SUBSCRIBE, ScheduleCodec.encodeSubscribe(pattern));
    const decoded = ScheduleCodec.decodeSubscribeResponse(
      assertPlainSuccess(response, "SUBSCRIBE"),
    );

    return decoded.subId;
  };

  const addLocalSubscription = (
    pattern: string,
    subId: bigint,
    handler: ScheduleHandler,
    signal?: AbortSignal,
  ): ScheduleSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map(), generation: subIdGeneration.next() };
      subscriptionsByPattern.set(pattern, subscription);
      patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    pendingNotifications.flush(subId);
    return createScheduleSubscription(async () => unsubscribe(pattern, handlerId), signal);
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

    const wireUnsubscribe = (async (): Promise<void> => {
      const response = await requestFrame(
        MSG_SCHEDULE_UNSUBSCRIBE,
        ScheduleCodec.encodeUnsubscribe(pattern),
      );
      ScheduleCodec.decodeUnsubscribeResponse(assertPlainSuccess(response, "UNSUBSCRIBE"));
    })();
    subscription.pendingUnsubscribe = wireUnsubscribe;
    try {
      await wireUnsubscribe;
    } finally {
      if (subscription.pendingUnsubscribe === wireUnsubscribe) {
        subscription.pendingUnsubscribe = undefined;
      }
    }
    // A concurrent subscribe() may have reused this same (not-yet-deleted)
    // state object while the round-trip above was in flight, repopulating
    // `handlers` — only clear the pattern-level bookkeeping if it's still
    // genuinely empty.
    if (isCurrentEmptyState(subscriptionsByPattern, pattern, subscription)) {
      subscriptionsByPattern.delete(pattern);
      patternsBySubId.delete(subscription.subId);
      pendingNotifications.remove(subscription.subId);
    }
  };

  const initNotifyHandler = (): void => {
    if (notifyHandlerInitialized) {
      return;
    }

    notifyHandlerInitialized = true;
    connection.registerNotificationHandler(MSG_SCHEDULE_NOTIFY, (payload) => {
      try {
        const decoded = ScheduleCodec.decodeNotification(payload);
        pendingNotifications.dispatchOrQueue(decoded.subId, {
          route: decoded.route,
          payload: decoded.payload,
        });
      } catch (error) {
        connection.reportBackgroundError?.("fitz.schedule.notification_malformed", error, {
          messageType: MSG_SCHEDULE_NOTIFY,
        });
      }
    });
  };

  const assertSuccess = (payload: Uint8Array, operation: string): Uint8Array => {
    const result = parseStandardResponse(payload);
    if (result.success) {
      return result.data;
    }

    // Prefer the real numeric domain code the broker already sends over
    // guessing the failure kind from free-text message substrings — the
    // latter silently reclassifies as "REQUEST_FAILED" the moment the
    // broker's wording changes, and misses codes the message text doesn't
    // happen to mention.
    const code =
      result.errorCode !== undefined
        ? formatStatusName(result.errorCode, ScheduleStatusNames)
        : mapErrorCode(result.error);

    throw new ScheduleError(
      `${operation} failed: ${result.error ?? "Unknown error"}`,
      code,
      result.errorCode,
    );
  };

  const assertPlainSuccess = (payload: Uint8Array, operation: string): Uint8Array => {
    const result = parsePlainResponse(payload);
    if (!result.success) {
      throw new ScheduleError(
        `${operation} failed: ${result.error ?? "unknown error"}`,
        `${operation}_FAILED`,
      );
    }
    return result.data;
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
    entries,
    subscribe,
    notifications,
  };
}

export * from "./types";

function assertConcreteScheduleRoute(route: string): void {
  if (!isRouteShape(route, "schedule", 4)) {
    throw new ScheduleError(
      `Invalid schedule route: ${route} (expected schedule://{realm}/{area}/{resource}/{operation}, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}

function routeMatchesSchedule(route: string, selector: string): boolean {
  const routeParts = route.split("://")[1]?.split("/") ?? [];
  const selectorParts = selector.split("://")[1]?.split("/") ?? [];
  if (selectorParts.length === 2 && selectorParts[1] === "**") {
    return routeParts.length === 4 && routeParts[0] === selectorParts[0];
  }
  if (selectorParts.length === 3) {
    return (
      routeParts.length === 4 &&
      selectorParts.every((part, i) => part === "*" || part === routeParts[i])
    );
  }
  return (
    routeParts.length === selectorParts.length &&
    selectorParts.every((part, i) => part === "*" || part === routeParts[i])
  );
}

function isScheduleSelector(selector: string): boolean {
  if (!selector.startsWith("schedule://")) return false;
  const parts = selector.slice("schedule://".length).split("/");
  if (
    parts.length === 2 &&
    parts[1] === "**" &&
    parts[0] !== undefined &&
    parts[0] !== "" &&
    !parts[0].includes("*")
  )
    return true;
  if (parts.length === 3 && parts.every((part) => part.length > 0)) {
    const [realm, area, resource] = parts as [string, string, string];
    return (
      !realm.includes("*") &&
      (area === "*" || !area.includes("*")) &&
      (resource === "*" || !resource.includes("*"))
    );
  }
  if (parts.length !== 4 || parts.some((part) => part.length === 0)) return false;
  const [realm, area, resource, operation] = parts as [string, string, string, string];
  const literal = (part: string) => !part.includes("*");
  const wild = (part: string) => part === "*";
  return (
    (literal(realm) && literal(area) && literal(resource) && literal(operation)) ||
    (literal(realm) && literal(area) && literal(resource) && wild(operation)) ||
    (literal(realm) && wild(area) && wild(resource) && wild(operation))
  );
}

function assertSchedulePattern(pattern: string): void {
  if (!isRegistrationPatternShape(pattern, "schedule", 4)) {
    throw new ScheduleError(
      `Invalid schedule subscription pattern: ${pattern} (expected a whole-segment pattern capable of matching four segments)`,
      "INVALID_ROUTE",
    );
  }
}
