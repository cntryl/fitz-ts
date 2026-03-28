/**
 * Stream domain client for append-only log operations.
 *
 * Stream uses session-based transactional semantics:
 * 1. `begin()` starts a write session with `expectedOffset` (OCC)
 * 2. `append()` on the session adds records
 * 3. `commit()` or `rollback()` finalizes the session
 */

import { DomainClient } from "../base";
import { StreamCodec } from "./codec";
import {
  StreamSession,
  StreamRecord,
  StreamMetadata,
  StreamStatus,
  StreamCommitHandler,
  StreamCommitNotification,
  StreamSubscription,
} from "./types";
import { StreamSessionImpl } from "./session";
import { StreamError } from "../../core/errors";
import { SliceIterator, AsyncIterableIterator } from "../../core/iterator";
import {
  MSG_STREAM_BEGIN,
  MSG_STREAM_READ,
  MSG_STREAM_LAST,
  MSG_STREAM_GET_METADATA,
  MSG_STREAM_SUBSCRIBE,
  MSG_STREAM_UNSUBSCRIBE,
  MSG_STREAM_NOTIFY,
} from "../../frame/types";

type StreamSubscriptionState = {
  subId: bigint;
  handlers: Map<number, StreamCommitHandler>;
};

export class StreamClient extends DomainClient {
  private subscriptionsByPattern = new Map<string, StreamSubscriptionState>();
  private patternsBySubId = new Map<bigint, string>();
  private initialized = false;
  private nextHandlerId = 1;

  constructor(connection: import("../../client/connection").Connection) {
    super(connection);
    this.connection.onReconnect(async () => {
      if (this.subscriptionsByPattern.size === 0) {
        return;
      }

      const snapshot = Array.from(
        this.subscriptionsByPattern.entries(),
        ([pattern, state]) => ({
          pattern,
          handlers: Array.from(state.handlers.entries()),
        }),
      );
      this.subscriptionsByPattern.clear();
      this.patternsBySubId.clear();

      for (const subscription of snapshot) {
        const subId = await this.subscribeWire(subscription.pattern);
        this.subscriptionsByPattern.set(subscription.pattern, {
          subId,
          handlers: new Map(subscription.handlers),
        });
        this.patternsBySubId.set(subId, subscription.pattern);
      }
    });
  }

  /**
   * Begin a write session on the stream.
   * @param route Stream route (e.g., "stream://realm/area/events")
   * @param expectedOffset Client's view of the next offset (OCC - optimistic concurrency control)
   * @returns StreamSession for append/commit/rollback
   */
  async begin(route: string, expectedOffset: bigint): Promise<StreamSession> {
    const payload = StreamCodec.encodeBegin(route, expectedOffset);
    const response = await this.requestFrame(MSG_STREAM_BEGIN, payload);
    const decoded = StreamCodec.decodeBeginResponse(response);

    this.checkStatus(decoded.status, "BEGIN");

    if (decoded.sessionId === undefined) {
      throw new StreamError(
        "BEGIN response missing sessionId",
        "MISSING_SESSION_ID",
      );
    }

    return new StreamSessionImpl(this.connection, route, decoded.sessionId);
  }

  /**
   * Read records from the stream.
   * @param route Stream route
   * @param startOffset Offset to start reading from (0 for beginning)
   * @param limit Maximum number of records to read (default: 100)
   * @returns Array of stream records
   */
  async read(
    route: string,
    startOffset: bigint,
    limit: number = 100,
  ): Promise<StreamRecord[]> {
    const payload = StreamCodec.encodeRead(route, startOffset, limit);
    const response = await this.requestFrame(MSG_STREAM_READ, payload);
    const decoded = StreamCodec.decodeReadResponse(response);

    this.checkStatus(decoded.status, "READ");

    return decoded.records;
  }

  /**
   * Consume records from the stream as an async iterator.
   * @param route Stream route
   * @param startOffset Offset to start reading from (0 for beginning)
   * @param limit Maximum number of records to read (default: 100)
   * @returns AsyncIterable of stream records
   */
  async consume(
    route: string,
    startOffset: bigint,
    limit: number = 100,
  ): Promise<AsyncIterable<StreamRecord>> {
    const records = await this.read(route, startOffset, limit);
    const iterator = new SliceIterator(records);
    return new AsyncIterableIterator(iterator);
  }

  /**
   * Get the last record in the stream.
   * @param route Stream route
   * @returns The most recent record, or null if stream is empty
   */
  async peek(route: string): Promise<StreamRecord | null> {
    const payload = StreamCodec.encodeLast(route);
    const response = await this.requestFrame(MSG_STREAM_LAST, payload);
    const decoded = StreamCodec.decodeLastResponse(response);

    this.checkStatus(decoded.status, "LAST");

    return decoded.record ?? null;
  }

  /**
   * Get stream metadata.
   * @param route Stream route
   * @returns Stream metadata (offsets and record count)
   */
  async metadata(route: string): Promise<StreamMetadata> {
    const payload = StreamCodec.encodeMetadata(route);
    const response = await this.requestFrame(MSG_STREAM_GET_METADATA, payload);
    const decoded = StreamCodec.decodeMetadataResponse(response);

    this.checkStatus(decoded.status, "GET_METADATA");

    return (
      decoded.metadata ?? {
        firstOffset: 0n,
        lastOffset: 0n,
        recordCount: 0n,
      }
    );
  }

  async subscribe(
    pattern: string,
    handler: StreamCommitHandler,
  ): Promise<StreamSubscription> {
    this.initNotifyHandler();
    const existing = this.subscriptionsByPattern.get(pattern);
    if (existing) {
      return this.addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await this.subscribeWire(pattern);
    return this.addLocalSubscription(pattern, subId, handler);
  }

  private async subscribeWire(pattern: string): Promise<bigint> {
    const payload = StreamCodec.encodeSubscribe(pattern);
    const response = await this.requestFrame(MSG_STREAM_SUBSCRIBE, payload);
    const decoded = StreamCodec.decodeSubscribeResponse(response);
    this.checkStatus(decoded.status, "SUBSCRIBE");

    if (decoded.subId === undefined) {
      throw new StreamError(
        "SUBSCRIBE response missing subId",
        "MISSING_SUB_ID",
      );
    }

    return decoded.subId;
  }

  private addLocalSubscription(
    pattern: string,
    subId: bigint,
    handler: StreamCommitHandler,
  ): StreamSubscription {
    const handlerId = this.nextHandlerId++;
    let subscription = this.subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      this.subscriptionsByPattern.set(pattern, subscription);
      this.patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    return new StreamSubscription(subId, pattern, async () => {
      await this.unsubscribe(pattern, handlerId);
    });
  }

  private async unsubscribe(pattern: string, handlerId: number): Promise<void> {
    const subscription = this.subscriptionsByPattern.get(pattern);
    if (!subscription) {
      return;
    }

    subscription.handlers.delete(handlerId);
    if (subscription.handlers.size > 0) {
      return;
    }

    this.subscriptionsByPattern.delete(pattern);
    this.patternsBySubId.delete(subscription.subId);
    const payload = StreamCodec.encodeUnsubscribe(pattern);
    const response = await this.requestFrame(MSG_STREAM_UNSUBSCRIBE, payload);
    const decoded = StreamCodec.decodeUnsubscribeResponse(response);
    this.checkStatus(decoded.status, "UNSUBSCRIBE");
  }

  private initNotifyHandler(): void {
    if (this.initialized) {
      return;
    }

    this.initialized = true;
    this.connection.registerNotificationHandler(
      MSG_STREAM_NOTIFY,
      (payload) => {
        try {
          const decoded = StreamCodec.decodeNotification(payload);
          const pattern = this.patternsBySubId.get(decoded.subId);
          if (!pattern) {
            return;
          }

          const subscription = this.subscriptionsByPattern.get(pattern);
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
            this.connection.dispatchAsyncHandler(async () => {
              await handler(notification);
            });
          }
        } catch {
          // Best-effort notification dispatch.
        }
      },
    );
  }

  /**
   * Check status and throw an error for non-OK responses.
   */
  private checkStatus(status: number, operation: string): void {
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
    throw new StreamError(
      `${operation} failed: ${statusName}`,
      statusName,
      status,
    );
  }
}
