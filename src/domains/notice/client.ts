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
import { createBufferReader } from "../../core/buffer";
import { parseStandardResponse } from "../../protocol/response";
import { NoticeCodec } from "./codec";
import { createNoticeSubscription, NoticeHandler, NoticeMsg, NoticeSubscription } from "./types";
import {
  createSubscriptionIterator,
  type SubscriptionIteratorOptions,
} from "../internal/subscription-iterator";

type NoticeSubscriptionState = {
  subId: bigint;
  handlers: Map<number, NoticeHandler>;
  generation: number;
  // Set while a wire UNSUBSCRIBE for this pattern is awaiting its broker
  // round-trip. subscribe()'s "reuse the existing state" path must wait it
  // out rather than reuse it blindly — see awaitPendingUnsubscribe().
  pendingUnsubscribe?: Promise<void>;
};

type NoticeConnectionPort = RequestPort &
  ReconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  FireAndForgetPort &
  OptionalResponsePort &
  Partial<ReconnectRestoreRequestPort>;

export interface NoticeClient {
  publish(route: string, body: Uint8Array): Promise<void>;
  subscribe(pattern: string, handler: NoticeHandler): Promise<NoticeSubscription>;
  subscribeIterator(
    pattern: string,
    options?: SubscriptionIteratorOptions,
  ): AsyncIterable<NoticeMsg>;
}

export function createNoticeClient(connection: NoticeConnectionPort): NoticeClient {
  const { requestFrame, requestReconnectFrame, expectOptionalResponse } =
    createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, NoticeSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  const subIdGeneration = createGenerationCounter();
  const pendingNotifications = createPendingNotificationBuffer<NoticeMsg, NoticeSubscriptionState>(
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
  let initialized = false;
  let nextHandlerId = 1;
  const registerSingleFlight = createKeyedSingleFlight<string, bigint>();

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
      async (_pattern, state) => {
        parseStandardResponse(
          await requestReconnectFrame(
            MSG_NOTICE_UNSUBSCRIBE,
            NoticeCodec.encodeUnsubscribe(state.subId),
          ),
        );
      },
    );

    patternsBySubId.clear();
    for (const [pattern, state] of subscriptionsByPattern) {
      patternsBySubId.set(state.subId, pattern);
      pendingNotifications.flush(state.subId);
    }
  });

  const publish = async (route: string, body: Uint8Array): Promise<void> => {
    assertNoticeRoute(route);
    const payload = NoticeCodec.encodePublish(route, body);
    const cancelOptionalResponse = expectOptionalResponse(MSG_NOTICE_PUBLISH);
    try {
      await connection.sendFireAndForget(MSG_NOTICE_PUBLISH, payload);
    } finally {
      // Must run on the success path too — this fire-and-forget PUBLISH
      // never gets an actual response, so leaving the registration in place
      // only on success would leak one optional-response slot per
      // successful publish() call for the lifetime of the connection.
      cancelOptionalResponse();
    }
  };

  const subscribe = async (
    pattern: string,
    handler: NoticeHandler,
  ): Promise<NoticeSubscription> => {
    assertNoticePattern(pattern);
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
  ): AsyncIterable<NoticeMsg> =>
    createSubscriptionIterator((handler) => subscribe(pattern, handler), iteratorOptions);

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const payload = NoticeCodec.encodeSubscribe(pattern);
    const response = await request(MSG_NOTICE_SUBSCRIBE, payload);
    const parsed = parseStandardResponse(response);
    if (!parsed.success) {
      throw new NoticeError(
        `SUBSCRIBE failed: ${parsed.error ?? "unknown error"}`,
        "SUBSCRIBE_FAILED",
        parsed.errorCode,
      );
    }
    const reader = createBufferReader(parsed.data);
    if (reader.readU8() !== 1 || reader.remainingBytes() !== 8) {
      throw new NoticeError("SUBSCRIBE response missing subId", "MISSING_SUB_ID");
    }
    return reader.readU64BE();
  };

  const addLocalSubscription = (
    pattern: string,
    subId: bigint,
    handler: NoticeHandler,
  ): NoticeSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map(), generation: subIdGeneration.next() };
      subscriptionsByPattern.set(pattern, subscription);
      patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    pendingNotifications.flush(subId);
    return createNoticeSubscription(
      createLiveSubIdGetter(subscriptionsByPattern, pattern, subId, subscription.generation),
      pattern,
      async () => {
        await unsubscribe(pattern, handlerId);
      },
    );
  };

  const unsubscribe = async (pattern: string, handlerId: number): Promise<void> => {
    const subscription = subscriptionsByPattern.get(pattern);
    if (!subscription || !subscription.handlers.has(handlerId)) {
      return;
    }

    if (subscription.handlers.size > 1) {
      // Other handlers remain — safe to remove this one locally without a
      // wire round-trip.
      subscription.handlers.delete(handlerId);
      return;
    }

    // This is the last handler. Don't remove it locally until the wire
    // UNSUBSCRIBE is confirmed — if it fails, the broker still expects
    // notifications to keep reaching it.
    const wireUnsubscribe = (async (): Promise<void> => {
      const payload = NoticeCodec.encodeUnsubscribe(subscription.subId);
      const parsed = parseStandardResponse(await requestFrame(MSG_NOTICE_UNSUBSCRIBE, payload));
      if (!parsed.success) {
        throw new NoticeError(
          `UNSUBSCRIBE failed: ${parsed.error ?? "unknown error"}`,
          "UNSUBSCRIBE_FAILED",
          parsed.errorCode,
        );
      }
    })();
    subscription.pendingUnsubscribe = wireUnsubscribe;
    try {
      await wireUnsubscribe;
    } finally {
      if (subscription.pendingUnsubscribe === wireUnsubscribe) {
        subscription.pendingUnsubscribe = undefined;
      }
    }

    subscription.handlers.delete(handlerId);
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
    if (initialized) {
      return;
    }

    initialized = true;
    connection.registerNotificationHandler(MSG_NOTICE_NOTIFY, (payload) => {
      try {
        const { subId, route, body } = NoticeCodec.decodeNotification(payload);
        pendingNotifications.dispatchOrQueue(subId, { route, body });
      } catch {
        // Best-effort notification dispatch.
      }
    });
  };

  return {
    publish,
    subscribe,
    subscribeIterator,
  };
}

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
  if (!isRegistrationPatternShape(pattern, "notice")) {
    throw new NoticeError(
      `Invalid notice pattern: ${pattern} (wildcards must be whole * or ** segments)`,
      "INVALID_ROUTE",
    );
  }
}
