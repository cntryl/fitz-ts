/**
 * Stream domain client for append-only log operations.
 *
 * Stream uses session-based transactional semantics:
 * 1. `begin()` starts a write session
 * 2. `append(expectedOffset, ...)` on the session adds records with OCC
 * 3. `commit()` or `rollback()` finalizes the session
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
  RetryExecutionPort,
} from "../base";
import { StreamCodec, type StreamWireReadOptions } from "./codec";
import {
  StreamSession,
  StreamRecord,
  StreamMetadata,
  StreamBeginOptions,
  StreamReadOptions,
  StreamReadPage,
  StreamReadBatch,
  StreamStatus,
  StreamStatusNames,
  StreamCommitHandler,
  StreamCommitNotification,
  StreamSubscription,
} from "./types";
import { createStreamSession } from "./session";
import { StreamError, StreamReadStalledError } from "../../core/errors";
import { createWakeGate } from "../../core/wake-gate";
import {
  MSG_STREAM_BEGIN,
  MSG_STREAM_READ,
  MSG_STREAM_LAST,
  MSG_STREAM_GET_METADATA,
  MSG_STREAM_SUBSCRIBE,
  MSG_STREAM_UNSUBSCRIBE,
  MSG_STREAM_NOTIFY,
} from "../../frame/types";
import { classifyStreamSelectorScope, isRouteShape, isStreamSelectorShape } from "../_routes";
import { restoreMapEntriesAtomically } from "../internal/restore";
import { createKeyedSingleFlight } from "../internal/keyed-single-flight";
import { formatStatusName } from "../internal/status";
import {
  awaitPendingUnsubscribe,
  createSubscriptionController,
  createGenerationCounter,
  isCurrentEmptyState,
} from "../internal/subscription-handle";
import {
  dispatchSubscriptionHandler,
  type SubscriptionHandlerRegistration,
} from "../internal/subscription-dispatch";
import { createPendingNotificationBuffer } from "../internal/pending-notifications";
import {
  createSubscriptionIterator,
  type SubscriptionIteratorOptions,
} from "../internal/subscription-iterator";
import { createBufferReader } from "../../core/buffer";

type StreamSubscriptionState = {
  subId: bigint;
  handlers: Map<number, SubscriptionHandlerRegistration<StreamCommitNotification>>;
  generation: number;
  // Set while a wire UNSUBSCRIBE for this pattern is awaiting its broker
  // round-trip. subscribe()'s "reuse the existing state" path must wait it
  // out rather than reuse it blindly — see awaitPendingUnsubscribe().
  pendingUnsubscribe?: Promise<void>;
};

type StreamConnectionPort = RequestPort &
  ReconnectListenerPort &
  DisconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  Partial<BackgroundErrorPort> &
  RetryExecutionPort &
  Partial<ReconnectRestoreRequestPort>;

/**
 * Append-only event stream facade with transactional writes and gap-aware
 * reads. Concrete routes use `stream://realm/area/resource`. Read/subscription
 * selectors accept whole-segment `*`, `stream://realm/**`, and `stream://**`;
 * the first wildcard selects the area, realm, or global ordering axis.
 */
export interface StreamClient {
  /** Begins a write session for one concrete resource route; always commit, rollback, or dispose it. */
  begin(route: string, options?: StreamBeginOptions): Promise<StreamSession>;
  /** Reads a concrete or wildcard selector without losing filtered-offset progress. */
  read(selector: string, options: StreamReadOptions): AsyncIterableIterator<StreamReadBatch>;
  /** Returns the latest record for a concrete route, or `null` when the stream is empty. */
  peek(
    route: string,
    options?: {
      /** Cancels this read-only request. */
      signal?: AbortSignal;
    },
  ): Promise<StreamRecord | null>;
  /** Reads limits, retention, offsets, and watermarks for a concrete stream route. */
  metadata(
    route: string,
    options?: {
      /** Cancels this read-only request. */
      signal?: AbortSignal;
    },
  ): Promise<StreamMetadata>;
  /** Registers a callback for commits matching `pattern`; dispose the returned handle. */
  subscribe(
    pattern: string,
    handler: StreamCommitHandler,
    options?: {
      /** Automatically unsubscribes this handler when aborted. */
      signal?: AbortSignal;
    },
  ): Promise<StreamSubscription>;
  /** Returns an async stream of commit summaries; breaking iteration unsubscribes. */
  notifications(
    pattern: string,
    options?: SubscriptionIteratorOptions,
  ): AsyncIterable<StreamCommitNotification>;
}

export function createStreamClient(connection: StreamConnectionPort): StreamClient {
  const registerSingleFlight = createKeyedSingleFlight<string, bigint>();
  const { requestFrame, requestReconnectFrame, runWithRetry } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, StreamSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  const subIdGeneration = createGenerationCounter();
  const pendingNotifications = createPendingNotificationBuffer<
    StreamCommitNotification,
    StreamSubscriptionState
  >(
    (subId) => {
      const pattern = patternsBySubId.get(subId);
      return pattern === undefined ? undefined : subscriptionsByPattern.get(pattern);
    },
    (registration, notification) => {
      dispatchSubscriptionHandler(
        connection,
        registration,
        notification,
        "stream",
        notification.route,
      );
    },
  );
  let initialized = false;
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
        assertStreamResponseSuccess(
          StreamCodec.decodeResponse(
            await requestReconnectFrame(
              MSG_STREAM_UNSUBSCRIBE,
              StreamCodec.encodeUnsubscribe(pattern),
            ),
            MSG_STREAM_UNSUBSCRIBE,
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

  const begin = async (route: string, options: StreamBeginOptions = {}): Promise<StreamSession> => {
    assertStreamRoute(route);
    const payload = StreamCodec.encodeBegin(route, options.ingestMetadata);
    const response = await requestFrame(MSG_STREAM_BEGIN, payload, options.signal);
    const decoded = StreamCodec.decodeBeginResponse(response);

    checkStatus(decoded, "BEGIN");

    if (decoded.sessionId === undefined) {
      throw new StreamError("BEGIN response missing sessionId", "MISSING_SESSION_ID");
    }

    return createStreamSession(connection, route, decoded.sessionId);
  };

  const readPage = async (
    route: string,
    startOffset: bigint,
    limit: number = 100,
    options?: StreamWireReadOptions & { signal?: AbortSignal },
  ): Promise<StreamReadPage> => {
    assertStreamPattern(route);
    return runWithRetry(
      {
        domain: "stream",
        operation: "read",
        retryClass: "replayable_read",
        signal: options?.signal,
      },
      async () => {
        const payload = StreamCodec.encodeRead(route, startOffset, limit, options);
        const response = await requestFrame(MSG_STREAM_READ, payload, options?.signal);
        const decoded = StreamCodec.decodeReadResponse(response, route);

        checkStatus(decoded, "READ");

        return {
          items: decoded.items,
          cursor: decoded.cursor ?? {
            lastResourceOffset: startOffset,
            lastAreaOffset: undefined,
            lastRealmOffset: undefined,
            lastGlobalOffset: undefined,
            cursorFingerprint: undefined,
            capturedWatermark: undefined,
            hasMore: false,
          },
        };
      },
    );
  };

  const read = async function* (
    route: string,
    options: StreamReadOptions,
  ): AsyncIterableIterator<StreamReadBatch> {
    assertStreamPattern(route);
    if (options === undefined || options.fromOffset === undefined || options.mode === undefined) {
      throw new StreamError("READ requires fromOffset and mode", "INVALID_READ_OPTIONS");
    }

    const wakeGate = createWakeGate();
    const subscription =
      options.mode === "follow"
        ? await subscribe(
            route,
            () => {
              wakeGate.wake();
            },
            { signal: options.signal },
          )
        : undefined;
    const unsubscribeReconnectWake =
      options.mode === "follow"
        ? connection.onReconnect(() => {
            wakeGate.wake();
          })
        : () => undefined;

    try {
      let offset = options.fromOffset;
      let cursorFingerprint: bigint | undefined;
      let capturedWatermark: bigint | undefined;
      let stalledPages = 0;

      while (true) {
        const observed = wakeGate.version;
        const page = await readPage(route, offset, options.batchSize ?? 100, {
          maxBytes: options.maxBytes,
          filter: options.filter,
          cursorFingerprint,
          capturedWatermark,
          signal: options.signal,
        });

        // Adopt the cursor unconditionally whenever the server returns one
        // — a page can legitimately report hasMore=true with zero *event*
        // items (e.g. maxBytes too small to fit the next record, or a page
        // of entirely filtered-out items), and gating this on
        // `page.items.length > 0` left the loop re-requesting the exact
        // same window forever in that case. Only the yield itself needs to
        // wait for actual records.
        const fromOffset = offset;
        const nextOffset = streamNextOffset(route, fromOffset, page.cursor);
        cursorFingerprint = page.cursor.cursorFingerprint;
        capturedWatermark = page.cursor.capturedWatermark;
        const records = StreamCodec.flattenStreamReadItems(page.items);

        if (page.cursor.hasMore && nextOffset <= fromOffset) {
          stalledPages += 1;
          if (stalledPages >= 2) throw new StreamReadStalledError(route, fromOffset);
        } else {
          stalledPages = 0;
        }
        offset = nextOffset;

        yield Object.freeze({
          items: Object.freeze([...page.items]),
          records: Object.freeze([...records]),
          fromOffset,
          nextOffset,
          caughtUp: !page.cursor.hasMore,
        });

        if (page.cursor.hasMore) {
          continue;
        }

        if (options.mode === "replay") return;

        await wakeGate.waitAfter(observed, { signal: options.signal });
      }
    } finally {
      unsubscribeReconnectWake();
      await subscription?.[Symbol.asyncDispose]();
    }
  };

  const peek = async (
    route: string,
    options: { signal?: AbortSignal } = {},
  ): Promise<StreamRecord | null> => {
    assertStreamRoute(route);
    return runWithRetry(
      {
        domain: "stream",
        operation: "last",
        retryClass: "replayable_read",
        signal: options.signal,
      },
      async () => {
        const payload = StreamCodec.encodeLast(route);
        const response = await requestFrame(MSG_STREAM_LAST, payload, options.signal);
        const decoded = StreamCodec.decodeLastResponse(response, route);

        checkStatus(decoded, "LAST");

        return decoded.record ?? null;
      },
    );
  };

  const metadata = async (
    route: string,
    options: { signal?: AbortSignal } = {},
  ): Promise<StreamMetadata> => {
    assertStreamRoute(route);
    return runWithRetry(
      {
        domain: "stream",
        operation: "metadata",
        retryClass: "replayable_read",
        signal: options.signal,
      },
      async () => {
        const payload = StreamCodec.encodeMetadata(route);
        const response = await requestFrame(MSG_STREAM_GET_METADATA, payload, options.signal);
        const decoded = StreamCodec.decodeMetadataResponse(response);

        checkStatus(decoded, "GET_METADATA");

        return (
          decoded.metadata ?? {
            firstOffset: 0n,
            lastOffset: 0n,
            recordCount: 0n,
          }
        );
      },
    );
  };

  const subscribe = async (
    pattern: string,
    handler: StreamCommitHandler,
    options?: { signal?: AbortSignal },
  ): Promise<StreamSubscription> => {
    assertStreamPattern(pattern);
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
  ): AsyncIterable<StreamCommitNotification> =>
    createSubscriptionIterator((handler) => subscribe(pattern, handler), iteratorOptions);

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const payload = StreamCodec.encodeSubscribe(pattern);
    const parsed = StreamCodec.decodeResponse(
      await request(MSG_STREAM_SUBSCRIBE, payload),
      MSG_STREAM_SUBSCRIBE,
    );
    assertStreamResponseSuccess(parsed, "SUBSCRIBE");
    const reader = createBufferReader(parsed.data);
    if (reader.readU8() !== 1 || reader.remainingBytes() < 8) {
      throw new StreamError("SUBSCRIBE response missing subId", "MISSING_SESSION_ID");
    }
    const subId = reader.readU64BE();
    if (!reader.isEOF()) {
      const dataLength = reader.readU32BE();
      if (dataLength !== 0 || !reader.isEOF()) {
        throw new StreamError("SUBSCRIBE response has unexpected data", "INVALID_RESPONSE");
      }
    }
    return subId;
  };

  const addLocalSubscription = (
    pattern: string,
    subId: bigint,
    handler: StreamCommitHandler,
    signal?: AbortSignal,
  ): StreamSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map(), generation: subIdGeneration.next() };
      subscriptionsByPattern.set(pattern, subscription);
      patternsBySubId.set(subId, pattern);
    }

    const controller = createSubscriptionController<StreamSubscription>(
      async () => unsubscribe(pattern, handlerId),
      signal,
    );
    subscription.handlers.set(handlerId, { handler, fail: (error) => controller.fail(error) });
    pendingNotifications.flush(subId);
    return controller.handle;
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
      const payload = StreamCodec.encodeUnsubscribe(pattern);
      assertStreamResponseSuccess(
        StreamCodec.decodeResponse(
          await requestFrame(MSG_STREAM_UNSUBSCRIBE, payload),
          MSG_STREAM_UNSUBSCRIBE,
        ),
        "UNSUBSCRIBE",
      );
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
    if (initialized) {
      return;
    }

    initialized = true;
    connection.registerNotificationHandler(MSG_STREAM_NOTIFY, (payload) => {
      try {
        const decoded = StreamCodec.decodeNotification(payload);
        const notification = toCommitNotification(decoded);
        pendingNotifications.dispatchOrQueue(decoded.subId, notification);
      } catch (error) {
        connection.reportBackgroundError?.("fitz.stream.notification_malformed", error, {
          messageType: MSG_STREAM_NOTIFY,
        });
      }
    });
  };

  const toCommitNotification = (decoded: ReturnType<typeof StreamCodec.decodeNotification>) => {
    const parsedPayload = decoded.parsedPayload;

    return {
      route: decoded.route,
      event: parsedPayload?.event,
      firstResourceOffset:
        parsedPayload?.first_resource_offset !== undefined
          ? BigInt(parsedPayload.first_resource_offset)
          : undefined,
      lastResourceOffset:
        parsedPayload?.last_resource_offset !== undefined
          ? BigInt(parsedPayload.last_resource_offset)
          : undefined,
      firstAreaOffset:
        parsedPayload?.first_area_offset !== undefined
          ? BigInt(parsedPayload.first_area_offset)
          : undefined,
      lastAreaOffset:
        parsedPayload?.last_area_offset !== undefined
          ? BigInt(parsedPayload.last_area_offset)
          : undefined,
      firstRealmOffset:
        parsedPayload?.first_realm_offset !== undefined
          ? BigInt(parsedPayload.first_realm_offset)
          : undefined,
      lastRealmOffset:
        parsedPayload?.last_realm_offset !== undefined
          ? BigInt(parsedPayload.last_realm_offset)
          : undefined,
      batchSize: parsedPayload?.batch_size,
      payload: parsedPayload,
    };
  };

  const checkStatus = (
    response: { status: number; errorCode?: number; errorMessage?: string },
    operation: string,
  ): void => {
    if (response.status === StreamStatus.Ok) {
      return;
    }
    const code = response.errorCode ?? response.status;
    const reason = response.errorMessage ?? formatStatusName(code, StreamStatusNames);
    throw new StreamError(`${operation} failed: ${reason}`, operation, code);
  };

  return {
    begin,
    read,
    peek,
    metadata,
    subscribe,
    notifications,
  };
}

function assertStreamResponseSuccess(
  response: { status: number; errorCode?: number; errorMessage?: string },
  operation: string,
): void {
  if (response.status === StreamStatus.Ok) return;
  throw new StreamError(
    `${operation} failed: ${response.errorMessage ?? "unknown error"}`,
    `${operation}_FAILED`,
    response.errorCode,
  );
}

export function streamNextOffset(
  route: string,
  currentOffset: bigint,
  cursor: StreamReadPage["cursor"],
): bigint {
  // Reuse the same canonical classifier the codec uses to decide whether a
  // page even carries lastGlobalOffset/cursorFingerprint/capturedWatermark
  // — matching it here (rather than re-guessing from string suffixes) is
  // what keeps both "stream://**" and its "stream://*/*/*" alias treated
  // as global, instead of the alias silently falling through as
  // realm-scoped.
  const scope = classifyStreamSelectorScope(route);
  if (scope === "global") {
    return cursor.lastGlobalOffset === undefined ? currentOffset : cursor.lastGlobalOffset + 1n;
  }
  // `{realm}/**` is the documented alias for `{realm}/*/*` — both must
  // resolve to the realm axis, not fall through to the resource default.
  if (scope === "realm") {
    return cursor.lastRealmOffset === undefined ? currentOffset : cursor.lastRealmOffset + 1n;
  }
  if (scope === "area") {
    return cursor.lastAreaOffset === undefined ? currentOffset : cursor.lastAreaOffset + 1n;
  }
  return cursor.lastResourceOffset + 1n;
}

export * from "./types";

function assertStreamRoute(route: string): void {
  if (!isRouteShape(route, "stream", 3)) {
    throw new StreamError(
      `Invalid stream route: ${route} (expected stream://{realm}/{area}/{resource}, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}

function assertStreamPattern(pattern: string): void {
  if (!isStreamSelectorShape(pattern)) {
    throw new StreamError(
      `Invalid stream selector: ${pattern} (expected realm/area/resource, realm/area/*, realm/*/*, or stream://**)`,
      "INVALID_ROUTE",
    );
  }
}
