/**
 * Stream domain client for append-only log operations.
 *
 * Stream uses session-based transactional semantics:
 * 1. `begin()` starts a write session
 * 2. `append(expectedOffset, ...)` on the session adds records with OCC
 * 3. `commit()` or `rollback()` finalizes the session
 */

import { createDomainClient } from "../base";
import { StreamCodec } from "./codec";
import {
  StreamSession,
  StreamRecord,
  StreamMetadata,
  StreamReadOptions,
  StreamReadPage,
  StreamStatus,
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
import { isRouteShape, isSelectorRouteShape } from "../_routes";
import type { Connection } from "../../client/connection";

type StreamSubscriptionState = {
  subId: bigint;
  handlers: Map<number, StreamCommitHandler>;
};

export type StreamClient = ReturnType<typeof createStreamClient>;

export function createStreamClient(connection: Connection) {
  const { requestFrame, requestReconnectFrame, runWithRetry } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, StreamSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  let initialized = false;
  let nextHandlerId = 1;

  connection.onReconnect(async () => {
    if (subscriptionsByPattern.size === 0) {
      return;
    }

    const snapshot = Array.from(subscriptionsByPattern.entries(), ([pattern, state]) => ({
      pattern,
      handlers: Array.from(state.handlers.entries()),
    }));
    subscriptionsByPattern.clear();
    patternsBySubId.clear();

    for (const subscription of snapshot) {
      const subId = await subscribeWire(subscription.pattern, requestReconnectFrame);
      subscriptionsByPattern.set(subscription.pattern, {
        subId,
        handlers: new Map(subscription.handlers),
      });
      patternsBySubId.set(subId, subscription.pattern);
    }
  });

  const begin = async (route: string, ingestMetadata?: Uint8Array): Promise<StreamSession> => {
    assertStreamRoute(route);
    const payload = StreamCodec.encodeBegin(route, ingestMetadata);
    const response = await requestFrame(MSG_STREAM_BEGIN, payload);
    const decoded = StreamCodec.decodeBeginResponse(response);

    checkStatus(decoded.status, "BEGIN");

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
        const decoded = StreamCodec.decodeReadResponse(response);

        checkStatus(decoded.status, "READ");

        return {
          items: decoded.items,
          cursor: decoded.cursor ?? {
            lastResourceOffset: startOffset,
            lastAreaOffset: undefined,
            lastRealmOffset: undefined,
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

    try {
      let offset = options.offset;

      while (true) {
        const observed = wakeGate.version;
        const page = await readPage(route, offset, options.batchSize ?? 100, {
          maxBytes: options.maxBytes,
          filter: options.filter,
          signal: options.signal,
        });

        if (page.items.length > 0) {
          offset = page.cursor.lastResourceOffset + 1n;
          const records = StreamCodec.flattenStreamReadItems(page.items);
          if (records.length > 0) {
            yield records;
          }
        }

        if (page.cursor.hasMore) {
          continue;
        }

        await wakeGate.waitAfter(observed, { signal: options.signal });
      }
    } finally {
      await subscription.unsubscribe().catch(() => undefined);
    }
  };

  const consume = async (
    route: string,
    startOffset: bigint,
    limit: number = 100,
    options?: StreamReadOptions,
  ): Promise<AsyncIterable<StreamRecord>> => {
    const records = await read(route, startOffset, limit, options);
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
        const decoded = StreamCodec.decodeLastResponse(response);

        checkStatus(decoded.status, "LAST");

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

        checkStatus(decoded.status, "GET_METADATA");

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
    const existing = subscriptionsByPattern.get(pattern);
    if (existing) {
      return addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await subscribeWire(pattern);
    return addLocalSubscription(pattern, subId, handler);
  };

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const payload = StreamCodec.encodeSubscribe(pattern);
    const response = await request(MSG_STREAM_SUBSCRIBE, payload);
    const decoded = StreamCodec.decodeSubscribeResponse(response);
    checkStatus(decoded.status, "SUBSCRIBE");

    if (decoded.subId === undefined) {
      throw new StreamError("SUBSCRIBE response missing subId", "MISSING_SESSION_ID");
    }

    return decoded.subId;
  };

  const addLocalSubscription = (
    pattern: string,
    subId: bigint,
    handler: StreamCommitHandler,
  ): StreamSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      subscriptionsByPattern.set(pattern, subscription);
      patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    return createStreamSubscription(subId, pattern, async () => {
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
    const payload = StreamCodec.encodeUnsubscribe(pattern);
    const response = await requestFrame(MSG_STREAM_UNSUBSCRIBE, payload);
    const decoded = StreamCodec.decodeUnsubscribeResponse(response);
    checkStatus(decoded.status, "UNSUBSCRIBE");
  };

  const initNotifyHandler = (): void => {
    if (initialized) {
      return;
    }

    initialized = true;
    connection.registerNotificationHandler(MSG_STREAM_NOTIFY, (payload) => {
      try {
        const decoded = StreamCodec.decodeNotification(payload);
        const pattern = patternsBySubId.get(decoded.subId);
        if (!pattern) {
          return;
        }

        const subscription = subscriptionsByPattern.get(pattern);
        if (!subscription) {
          return;
        }

        const parsedPayload = decoded.parsedPayload;

        const notification: StreamCommitNotification = {
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

  const checkStatus = (status: number, operation: string): void => {
    if (status === StreamStatus.Ok) {
      return;
    }

    const statusNames: Record<number, string> = {
      [StreamStatus.StreamNotFound]: "StreamNotFound",
      [StreamStatus.OffsetOutOfRange]: "OffsetOutOfRange",
      [StreamStatus.InvalidOffset]: "InvalidOffset",
      [StreamStatus.StreamFull]: "StreamFull",
      [StreamStatus.SessionNotFound]: "SessionNotFound",
      [StreamStatus.SessionClosed]: "SessionClosed",
      [StreamStatus.ExpectedOffsetMismatch]: "ExpectedOffsetMismatch",
    };

    const statusName = statusNames[status] || `Unknown(${status})`;
    throw new StreamError(`${operation} failed: ${statusName}`, statusName, status);
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
  };
}

type StreamClientConstructor = {
  new (connection: Connection): StreamClient;
  (connection: Connection): StreamClient;
};

export const StreamClient: StreamClientConstructor = function (connection: Connection) {
  return createStreamClient(connection);
} as unknown as StreamClientConstructor;

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
  if (!isSelectorRouteShape(pattern, "stream", 3, { allowRealmWildcard: true })) {
    throw new StreamError(
      `Invalid stream pattern: ${pattern} (expected stream://{realm}/{area}/{resource}, stream://{realm}/{area}/*, or stream://{realm}/**)`,
      "INVALID_ROUTE",
    );
  }
}
