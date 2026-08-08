/**
 * Schedule domain client.
 */

import { createDomainClient } from "../base";
import type {
  AsyncDispatchPort,
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
import { createWakeGate } from "../../core/wake-gate";
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
  createLiveSubIdGetter,
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
  ReconnectListenerPort &
  DisconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  Partial<ReconnectRestoreRequestPort>;

export interface ScheduleClient {
  create(
    route: string,
    cronExpr: string,
    deliveryMode: ScheduleDeliveryMode,
    payload?: Uint8Array,
  ): Promise<string>;
  cancel(route: string): Promise<void>;
  listPage(offset?: bigint, limit?: bigint): Promise<ScheduleListPage>;
  listBySelector(selector: string): Promise<ScheduleEntry[]>;
  waitForNotifications(
    route: string,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<ScheduleNotification>;
  subscribe(pattern: string, handler: ScheduleHandler): Promise<ScheduleSubscription>;
  subscribeIterator(
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
    cronExpr: string,
    deliveryMode: ScheduleDeliveryMode,
    payload: Uint8Array = new Uint8Array(),
  ): Promise<string> => {
    assertConcreteScheduleRoute(route);

    const response = await requestFrame(
      MSG_SCHEDULE_CREATE,
      ScheduleCodec.encodeCreate(route, cronExpr, deliveryMode, payload),
    );
    const decoded = ScheduleCodec.decodeCreateResponse(assertPlainSuccess(response, "CREATE"));
    return decoded.scheduleId ?? route;
  };

  const cancel = async (route: string): Promise<void> => {
    assertConcreteScheduleRoute(route);

    const response = await requestFrame(MSG_SCHEDULE_CANCEL, ScheduleCodec.encodeCancel(route));
    ScheduleCodec.decodeCancelResponse(assertPlainSuccess(response, "CANCEL"));
  };

  const listPage = async (offset?: bigint, limit?: bigint): Promise<ScheduleListPage> => {
    const response = await requestFrame(
      MSG_SCHEDULE_LIST_PAGE,
      ScheduleCodec.encodeListPage(offset, limit),
    );
    return ScheduleCodec.decodeListPage(assertSuccess(response, "LIST_PAGE"));
  };

  const listBySelector = async (selector: string): Promise<ScheduleEntry[]> => {
    if (!isScheduleSelector(selector))
      throw new ScheduleError("invalid schedule selector", "INVALID_ROUTE");
    const entries: ScheduleEntry[] = [];
    let offset = 0n;
    let complete = false;
    while (!complete) {
      const page = await listPage(offset);
      entries.push(...page.entries.filter((entry) => routeMatchesSchedule(entry.route, selector)));
      offset += BigInt(page.entries.length);
      complete = offset >= page.totalCount || page.entries.length === 0;
    }
    return entries;
  };

  const waitForNotifications = async function* (
    route: string,
    options: {
      signal?: AbortSignal;
    } = {},
  ): AsyncIterable<ScheduleNotification> {
    assertConcreteScheduleRoute(route);

    const wakeGate = createWakeGate();
    const queuedNotifications: ScheduleNotification[] = [];
    const subscription = await subscribe(route, (notification) => {
      queuedNotifications.push(notification);
      wakeGate.wake();
    });
    const unsubscribeReconnectWake = connection.onReconnect(() => {
      wakeGate.wake();
    });
    // Without this, a disconnect while idle-parked in wakeGate.waitAfter()
    // below has no wake source at all — a `for await...break` or
    // client.close() can hang forever, leaking the wire subscription in the
    // finally block that never runs.
    //
    // Deliberately just a wake, not a sticky "stop" flag: a sticky flag
    // fires for every disconnect, including the transient ones a live
    // reconnect resolves moments later, and would make this throw before
    // the restored subscription (via the onReconnect wake above) ever gets
    // a chance to keep the iteration going — regressing reconnect survival
    // for exactly the case this generator is meant to support. This matches
    // the equivalent notification-waiting generators in the KV, Lease,
    // Notice, and Stream domains, none of which treat disconnect as
    // terminal either — callers that need prompt teardown on a permanent
    // disconnect should pass `options.signal` and abort it themselves.
    const unsubscribeDisconnectWake = connection.onDisconnect(() => {
      wakeGate.wake();
    });

    try {
      while (true) {
        const notification = queuedNotifications.shift();
        if (notification) {
          yield notification;
          continue;
        }

        const observed = wakeGate.version;
        if (queuedNotifications.length > 0) {
          continue;
        }

        await wakeGate.waitAfter(observed, { signal: options.signal });
      }
    } finally {
      unsubscribeReconnectWake();
      unsubscribeDisconnectWake();
      await subscription.unsubscribe().catch(() => undefined);
    }
  };

  const subscribe = async (
    pattern: string,
    handler: ScheduleHandler,
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
        return addLocalSubscription(pattern, existing.subId, handler);
      }

      const subId = await registerSingleFlight(pattern, () => subscribeWire(pattern));
      return addLocalSubscription(pattern, subId, handler);
    }
  };

  const subscribeIterator = (
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
    return createScheduleSubscription(
      createLiveSubIdGetter(subscriptionsByPattern, pattern, subId, subscription.generation),
      pattern,
      async () => {
        await unsubscribe(pattern, handlerId);
      },
    );
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
    listPage,
    listBySelector,
    subscribe,
    subscribeIterator,
    waitForNotifications,
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
