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
  DisconnectListenerPort,
  NotificationPort,
  ReconnectListenerPort,
  ReconnectRestoreRequestPort,
  RequestPort,
  RetryExecutionPort,
} from "../base";
import { StreamCodec, isGlobalSelector } from "./codec";
import {
  StreamSession,
  StreamRecord,
  StreamMetadata,
  StreamReadOptions,
  StreamReadPage,
  StreamStatus,
  StreamStatusNames,
  StreamCommitHandler,
  StreamCommitNotification,
  StreamSubscription,
  createStreamSubscription,
} from "./types";
import { createStreamSession } from "./session";
import { StreamError } from "../../core/errors";
import { createSliceIterator, createAsyncIterableIterator } from "../../core/iterator";
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
import { isRouteShape, isStreamSelectorShape } from "../_routes";
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
import { createBufferReader } from "../../core/buffer";
import { parseStandardResponse } from "../../protocol/response";

type StreamSubscriptionState = {
  subId: bigint;
  handlers: Map<number, StreamCommitHandler>;
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
  RetryExecutionPort &
  Partial<ReconnectRestoreRequestPort>;

export interface StreamClient {
  begin(route: string, ingestMetadata?: Uint8Array): Promise<StreamSession>;
  readPage(
    route: string,
    startOffset: bigint,
    limit?: number,
    options?: StreamReadOptions,
  ): Promise<StreamReadPage>;
  read(
    route: string,
    startOffset: bigint,
    limit?: number,
    options?: StreamReadOptions,
  ): Promise<StreamRecord[]>;
  readWhenCommitted(
    route: string,
    options: {
      offset: bigint;
      batchSize?: number;
      signal?: AbortSignal;
      maxBytes?: bigint;
      filter?: StreamReadOptions["filter"];
    },
  ): AsyncIterable<StreamRecord[]>;
  consume(
    route: string,
    startOffset: bigint,
    limit?: number,
    options?: StreamReadOptions,
  ): Promise<AsyncIterable<StreamRecord>>;
  peek(route: string): Promise<StreamRecord | null>;
  metadata(route: string): Promise<StreamMetadata>;
  subscribe(pattern: string, handler: StreamCommitHandler): Promise<StreamSubscription>;
  subscribeIterator(
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
    (handler, notification) => {
      connection.dispatchAsyncHandler(async () => {
        await handler(notification);
      });
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
        parseStandardResponse(
          await requestReconnectFrame(
            MSG_STREAM_UNSUBSCRIBE,
            StreamCodec.encodeUnsubscribe(pattern),
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

  const begin = async (route: string, ingestMetadata?: Uint8Array): Promise<StreamSession> => {
    assertStreamRoute(route);
    const payload = StreamCodec.encodeBegin(route, ingestMetadata);
    const response = await requestFrame(MSG_STREAM_BEGIN, payload);
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
    options?: StreamReadOptions,
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

  const read = async (
    route: string,
    startOffset: bigint,
    limit: number = 100,
    options?: StreamReadOptions,
  ): Promise<StreamRecord[]> => {
    const page = await readPage(route, startOffset, limit, options);
    return StreamCodec.flattenStreamReadItems(page.items);
  };

  const readWhenCommitted = async function* (
    route: string,
    options: {
      offset: bigint;
      batchSize?: number;
      signal?: AbortSignal;
      maxBytes?: bigint;
      filter?: StreamReadOptions["filter"];
    },
  ): AsyncIterable<StreamRecord[]> {
    assertStreamPattern(route);

    const wakeGate = createWakeGate();
    const subscription = await subscribe(route, () => {
      wakeGate.wake();
    });
    const unsubscribeReconnectWake = connection.onReconnect(() => {
      wakeGate.wake();
    });

    try {
      let offset = options.offset;
      let cursorFingerprint: bigint | undefined;
      let capturedWatermark: bigint | undefined;

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
        offset = streamCursorOffset(route, page.cursor) + 1n;
        cursorFingerprint = page.cursor.cursorFingerprint;
        capturedWatermark = page.cursor.capturedWatermark;
        const records = StreamCodec.flattenStreamReadItems(page.items);
        if (records.length > 0) {
          yield records;
        }

        if (page.cursor.hasMore) {
          continue;
        }

        await wakeGate.waitAfter(observed, { signal: options.signal });
      }
    } finally {
      unsubscribeReconnectWake();
      await subscription.unsubscribe().catch(() => undefined);
    }
  };

  const consume = async (
    route: string,
    startOffset: bigint,
    limit?: number,
    options?: StreamReadOptions,
  ): Promise<AsyncIterable<StreamRecord>> => {
    const page = await readPage(route, startOffset, limit ?? 100, options);
    // Mirrors KvTransaction.scan()'s guard for the identical shape (a
    // single page wrapped as a batch async iterable): "consume" implies
    // full traversal, so silently truncating to one page with no signal
    // that more data exists is a footgun, not a documented limitation.
    // Only fires when the caller left `limit` unspecified — an explicit
    // limit is an intentional bound, not truncation.
    if (page.cursor.hasMore && limit === undefined) {
      throw new StreamError(
        "CONSUME truncated an unbounded response unexpectedly",
        "CONSUME_TRUNCATED",
      );
    }

    const records = StreamCodec.flattenStreamReadItems(page.items);
    return createAsyncIterableIterator(createSliceIterator(records));
  };

  const peek = async (route: string): Promise<StreamRecord | null> => {
    assertStreamRoute(route);
    return runWithRetry(
      {
        domain: "stream",
        operation: "last",
        retryClass: "replayable_read",
      },
      async () => {
        const payload = StreamCodec.encodeLast(route);
        const response = await requestFrame(MSG_STREAM_LAST, payload);
        const decoded = StreamCodec.decodeLastResponse(response, route);

        checkStatus(decoded, "LAST");

        return decoded.record ?? null;
      },
    );
  };

  const metadata = async (route: string): Promise<StreamMetadata> => {
    assertStreamRoute(route);
    return runWithRetry(
      {
        domain: "stream",
        operation: "metadata",
        retryClass: "replayable_read",
      },
      async () => {
        const payload = StreamCodec.encodeMetadata(route);
        const response = await requestFrame(MSG_STREAM_GET_METADATA, payload);
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
        return addLocalSubscription(pattern, existing.subId, handler);
      }

      const subId = await registerSingleFlight(pattern, () => subscribeWire(pattern));
      return addLocalSubscription(pattern, subId, handler);
    }
  };

  const subscribeIterator = (
    pattern: string,
    iteratorOptions?: SubscriptionIteratorOptions,
  ): AsyncIterable<StreamCommitNotification> =>
    createSubscriptionIterator((handler) => subscribe(pattern, handler), iteratorOptions);

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const payload = StreamCodec.encodeSubscribe(pattern);
    const parsed = parseStandardResponse(await request(MSG_STREAM_SUBSCRIBE, payload));
    if (!parsed.success) {
      throw new StreamError(
        `SUBSCRIBE failed: ${parsed.error ?? "unknown error"}`,
        "SUBSCRIBE_FAILED",
        parsed.errorCode,
      );
    }
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
  ): StreamSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map(), generation: subIdGeneration.next() };
      subscriptionsByPattern.set(pattern, subscription);
      patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    pendingNotifications.flush(subId);
    return createStreamSubscription(
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
      const payload = StreamCodec.encodeUnsubscribe(pattern);
      const parsed = parseStandardResponse(await requestFrame(MSG_STREAM_UNSUBSCRIBE, payload));
      if (!parsed.success) {
        throw new StreamError(
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
      } catch {
        // Best-effort notification dispatch.
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
    readPage,
    read,
    readWhenCommitted,
    consume,
    peek,
    metadata,
    subscribe,
    subscribeIterator,
  };
}

function streamCursorOffset(route: string, cursor: StreamReadPage["cursor"]): bigint {
  // Reuse the same canonical classifier the codec uses to decide whether a
  // page even carries lastGlobalOffset/cursorFingerprint/capturedWatermark
  // — matching it here (rather than re-guessing from string suffixes) is
  // what keeps both "stream://**" and its "stream://*/*/*" alias treated
  // as global, instead of the alias silently falling through as
  // realm-scoped.
  if (isGlobalSelector(route)) {
    return cursor.lastGlobalOffset ?? cursor.lastResourceOffset;
  }
  // `{realm}/**` is the documented alias for `{realm}/*/*` — both must
  // resolve to the realm axis, not fall through to the resource default.
  if (route.endsWith("/**") || route.endsWith("/*/*")) {
    return cursor.lastRealmOffset ?? cursor.lastResourceOffset;
  }
  if (route.endsWith("/*")) return cursor.lastAreaOffset ?? cursor.lastResourceOffset;
  return cursor.lastResourceOffset;
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
