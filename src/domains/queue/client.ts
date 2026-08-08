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
import { isRegistrationPatternShape, isRouteShape } from "../_routes";
import { restoreMapEntriesAtomically } from "../internal/restore";
import { createKeyedSingleFlight } from "../internal/keyed-single-flight";
import { formatStatusName } from "../internal/status";
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
  generation: number;
  // Set while a wire UNSUBSCRIBE for this pattern is awaiting its broker
  // round-trip. subscribe()'s "reuse the existing state" path must wait it
  // out rather than reuse it blindly — see awaitPendingUnsubscribe().
  pendingUnsubscribe?: Promise<void>;
};

type QueueConnectionPort = RequestPort &
  ReconnectListenerPort &
  DisconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  RetryExecutionPort &
  Partial<ReconnectRestoreRequestPort>;

export interface QueueClient {
  enqueue(route: string, body: Uint8Array, options?: EnqueueOptions): Promise<bigint>;
  /**
   * Reserves up to `batchSize` messages.
   *
   * Note: unlike {@link QueueClient.reserveWhenAvailable}, this takes
   * positional parameters rather than an options object — the two are
   * conceptually the same operation in different shapes for historical
   * reasons; take care not to transpose `batchSize` and `waitSeconds`.
   */
  reserve(
    route: string,
    leaseSeconds: number,
    batchSize?: number,
    waitSeconds?: number,
    signal?: AbortSignal,
  ): Promise<QueueItem[]>;
  reserveWhenAvailable(
    route: string,
    options: { leaseSeconds: number; batchSize?: number; signal?: AbortSignal },
  ): AsyncIterable<QueueItem[]>;
  subscribe(pattern: string, handler: AvailabilityHandler): Promise<QueueSubscription>;
  subscribeIterator(
    pattern: string,
    options?: SubscriptionIteratorOptions,
  ): AsyncIterable<AvailabilityNotification>;
}

export function createQueueClient(connection: QueueConnectionPort): QueueClient {
  const registerSingleFlight = createKeyedSingleFlight<string, bigint>();
  const { requestFrame, requestReconnectFrame, runWithRetry } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, QueueSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  const subIdGeneration = createGenerationCounter();
  const pendingNotifications = createPendingNotificationBuffer<
    AvailabilityNotification,
    QueueSubscriptionState
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
  let notificationHandlerRegistered = false;
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
        QueueCodec.decodeUnsubscribeResponse(
          await requestReconnectFrame(MSG_QUEUE_UNSUBSCRIBE, QueueCodec.encodeUnsubscribe(pattern)),
        );
      },
    );

    patternsBySubId.clear();
    for (const [pattern, state] of subscriptionsByPattern) {
      patternsBySubId.set(state.subId, pattern);
      pendingNotifications.flush(state.subId);
    }
  });

  const enqueue = async (
    route: string,
    body: Uint8Array,
    options?: EnqueueOptions,
  ): Promise<bigint> => {
    assertQueueRoute(route);
    if (options?.priority !== undefined || options?.ttlMs !== undefined) {
      // encodeEnqueue has no wire-format byte range for either field — they
      // were previously accepted and silently dropped. Fail loudly instead
      // of guessing at wire bytes until the protocol actually supports
      // them.
      throw new QueueError(
        "EnqueueOptions.priority and .ttlMs are not yet supported by the wire protocol",
        "UNSUPPORTED_OPTION",
      );
    }
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
    signal?: AbortSignal,
  ): Promise<QueueItem[]> => {
    assertQueueReserveRoute(route);
    if (!Number.isInteger(batchSize) || batchSize < 0 || batchSize > 1024) {
      throw new QueueError("RESERVE batch size must be between 0 and 1024", "INVALID_BATCH_SIZE");
    }
    return reserveOnce(route, leaseSeconds, batchSize, signal, waitSeconds);
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
    // Without this, a disconnect while idle-parked in wakeGate.waitAfter()
    // below has no wake source at all: a `for await...break` or
    // client.close() can hang forever (a generator's implicit .return()
    // doesn't run its `finally` until the awaited promise itself settles),
    // leaking the wire subscription in the finally block that never runs.
    //
    // Deliberately just a wake, not a sticky "stop" flag: this fires for
    // every disconnect, including the transient ones a live reconnect
    // resolves moments later, and the restored subscription is meant to let
    // the loop keep going across those. Retrying reserveOnce() after waking
    // lets the connection layer itself decide the outcome — it already
    // waits out an in-progress reconnect, or fails fast once the connection
    // is genuinely closed (reconnect disabled/exhausted, or client.close()),
    // which is exactly the failure this generator should propagate.
    const unsubscribeDisconnectWake = connection.onDisconnect(() => {
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
      unsubscribeDisconnectWake();
      await subscription.unsubscribe().catch(() => undefined);
    }
  };

  const reserveOnce = async (
    route: string,
    leaseSeconds: number,
    batchSize: number,
    signal?: AbortSignal,
    waitSeconds?: number,
  ): Promise<QueueItem[]> => {
    const payload = QueueCodec.encodeReserve(route, leaseSeconds, batchSize, waitSeconds);
    const response = await requestFrame(MSG_QUEUE_RESERVE, payload, signal);
    const decoded = QueueCodec.decodeReserveResponse(response, route);
    checkStatus(decoded, "RESERVE");

    return (decoded.items ?? []).map((item) =>
      createQueueItem(item.id, item.token, item.body, item.route, connection),
    );
  };

  const subscribe = async (
    pattern: string,
    handler: AvailabilityHandler,
  ): Promise<QueueSubscription> => {
    assertQueueSubscriptionPattern(pattern);
    initNotificationHandler();

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

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const payload = QueueCodec.encodeSubscribe(pattern);
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
      subscription = { subId, handlers: new Map(), generation: subIdGeneration.next() };
      subscriptionsByPattern.set(pattern, subscription);
      patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    pendingNotifications.flush(subId);

    return createQueueSubscription(
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
      const payload = QueueCodec.encodeUnsubscribe(pattern);
      const response = await requestFrame(MSG_QUEUE_UNSUBSCRIBE, payload);
      const decoded = QueueCodec.decodeUnsubscribeResponse(response);
      checkStatus(decoded, "UNSUBSCRIBE");
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

  const subscribeIterator = (
    pattern: string,
    iteratorOptions?: SubscriptionIteratorOptions,
  ): AsyncIterable<AvailabilityNotification> =>
    createSubscriptionIterator((handler) => subscribe(pattern, handler), iteratorOptions);

  const initNotificationHandler = (): void => {
    if (notificationHandlerRegistered) {
      return;
    }

    notificationHandlerRegistered = true;
    connection.registerNotificationHandler(MSG_QUEUE_NOTIFY, (payload) => {
      try {
        const { subId, route, readyMessages, delayedMessages, inflightMessages } =
          QueueCodec.decodeNotification(payload);
        const notification: AvailabilityNotification = {
          route,
          readyMessages,
          delayedMessages,
          inflightMessages,
        };
        pendingNotifications.dispatchOrQueue(subId, notification);
      } catch {
        // Best-effort notification dispatch.
      }
    });
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
      4001: "InvalidToken",
      4002: "LeaseExpired",
      4003: "MessageNotFound",
      4004: "QueueNotFound",
      4005: "QueueFull",
      4009: "Unauthorized",
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
    subscribeIterator,
  };
}

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
  if (!isRegistrationPatternShape(route, "queue", 3)) {
    throw new QueueError(
      `Invalid queue selector: ${route} (expected a whole-segment pattern capable of matching three segments)`,
      "INVALID_ROUTE",
    );
  }
}

function assertQueueSubscriptionPattern(pattern: string): void {
  if (!isRegistrationPatternShape(pattern, "queue", 3)) {
    throw new QueueError(
      `Invalid queue pattern: ${pattern} (expected a whole-segment pattern capable of matching three segments)`,
      "INVALID_ROUTE",
    );
  }
}
