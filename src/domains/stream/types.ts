/**
 * Stream domain type definitions
 * Stream uses session-based transactional semantics:
 * 1. Begin() returns a session with server-assigned sessionID
 * 2. Append(expectedOffset, ...) on session validates optimistic concurrency
 * 3. Commit() or Rollback() to finalize
 */

import "../../core/async-dispose";
import { createSubscriptionHandle } from "../internal/subscription-handle";

/**
 * Stream record with offset, timestamp, and payload
 */
export interface StreamRecord {
  /** Concrete resource route containing the record. */
  route: string;
  /** Resource-local record offset. */
  offset: bigint;
  /** Broker-assigned Unix timestamp in milliseconds. */
  timestamp: bigint;
  /** Application payload. */
  body: Uint8Array;
  /** Area-wide ordered offset when returned by a broader selector. */
  areaOffset?: bigint;
  /** Realm-wide ordered offset when returned by a broader selector. */
  realmOffset?: bigint;
  /** Globally ordered offset when returned by a global selector. */
  globalOffset?: bigint;
  /** Optional ingest metadata attached when the write session began. */
  metadata?: Uint8Array;
}

/**
 * Stream metadata.
 */
export interface StreamMetadata {
  /** First retained resource-local offset. */
  firstOffset: bigint;
  /** Last assigned resource-local offset. */
  lastOffset: bigint;
  /** Number of currently retained records. */
  recordCount: bigint;
  /** Broker event-count limit per append batch, when configured. */
  maxBatchEvents?: bigint;
  /** Broker byte limit per append batch, when configured. */
  maxBatchBytes?: bigint;
  /** Retention lifetime in seconds, when configured. */
  ttlSeconds?: bigint;
  /** Latest area-wide offset visible for this resource. */
  areaWatermark?: bigint;
  /** Latest realm-wide offset visible for this resource. */
  realmWatermark?: bigint;
}

/** Application-defined discriminator used by server-side read filters. */
export type StreamDiscriminator = string;

/** Reason an offset was represented without exposing its record. */
export type StreamFilteredReason = "server_filter" | "permission" | "projection";

/** One predicate applied to a record discriminator by the broker. */
export type StreamFilterClause =
  | { kind: "Equals"; value: string }
  | { kind: "NotEquals"; value: string }
  | { kind: "StartsWith"; value: string }
  | { kind: "AnyOf"; values: string[] };

/** Conjunction of discriminator clauses applied during a stream read. */
export interface StreamFilterSet {
  /** Clauses evaluated by the broker; every clause must match. */
  clauses: StreamFilterClause[];
}

/** Options for appending one record within a write session. */
export interface StreamAppendOptions {
  /** Required resource-local offset immediately preceding this append; provides OCC fencing. */
  expectedOffset: bigint;
  /** Record payload. */
  body: Uint8Array;
  /** Optional value used by server-side filters. */
  discriminator?: StreamDiscriminator;
  /** Cancels waiting for the append response; an ambiguous post-send result must be resolved before retry. */
  signal?: AbortSignal;
}

/** Options for beginning a stream write session. */
export interface StreamBeginOptions {
  /** Opaque metadata associated with every record committed by the session. */
  ingestMetadata?: Uint8Array;
  /** Cancels session creation. */
  signal?: AbortSignal;
}

/** Options for replaying or following a stream selector. */
export interface StreamReadOptions {
  /** Inclusive offset on the selector's ordering axis. */
  fromOffset: bigint;
  /** `replay` ends at the captured watermark; `follow` waits for later commits. */
  mode: "replay" | "follow";
  /** Requested maximum events per broker page. */
  batchSize?: number;
  /** Requested maximum payload bytes per broker page. */
  maxBytes?: bigint;
  /** Optional server-side discriminator filter. Filtered offsets still advance progress. */
  filter?: StreamFilterSet;
  /** Cancels the read and closes its iterator. */
  signal?: AbortSignal;
}

export interface StreamReadCursor {
  lastResourceOffset: bigint;
  lastAreaOffset?: bigint;
  lastRealmOffset?: bigint;
  lastGlobalOffset?: bigint;
  cursorFingerprint?: bigint;
  capturedWatermark?: bigint;
  hasMore: boolean;
}

/** Visible event item returned from a read. */
export interface StreamReadEvent {
  /** Discriminant for a visible record. */
  kind: "event";
  /** Concrete resource route. */
  route: string;
  /** Complete visible record. */
  record: StreamRecord;
}

/** One hidden/filtered offset that still contributes to cursor progress. */
export interface StreamReadFiltered {
  /** Discriminant for one filtered offset. */
  kind: "filtered";
  /** Concrete resource route. */
  route: string;
  /** Hidden offset on the relevant selector axis. */
  offset: bigint;
  /** Broker-provided filtering reason, when available. */
  reason?: StreamFilteredReason;
}

/** Inclusive range of hidden offsets compressed into one read item. */
export interface StreamReadFilteredRange {
  /** Discriminant for a compressed filtered range. */
  kind: "filtered_range";
  /** Concrete resource route. */
  route: string;
  /** First hidden offset, inclusive. */
  fromOffset: bigint;
  /** Last hidden offset, inclusive. */
  toOffset: bigint;
  /** Broker-provided filtering reason, when available. */
  reason?: StreamFilteredReason;
}

/** Read item preserving progress across both visible and filtered offsets. */
export type StreamReadItem = StreamReadEvent | StreamReadFiltered | StreamReadFilteredRange;

export interface StreamReadPage {
  items: readonly StreamReadItem[];
  cursor: StreamReadCursor;
}

/** Consumer-facing read batch with visible records and complete progress information. */
export interface StreamReadBatch {
  /** Ordered visible and filtered items. Use this when exact offset accounting matters. */
  readonly items: readonly StreamReadItem[];
  /** Convenience projection containing only visible records. */
  readonly records: readonly StreamRecord[];
  /** Inclusive selector offset requested for this batch. */
  readonly fromOffset: bigint;
  /** Offset to pass as `fromOffset` to continue without gaps or duplicates. */
  readonly nextOffset: bigint;
  /** Whether the iterator reached its captured watermark at this batch. */
  readonly caughtUp: boolean;
}

/** Commit durability: `Buffered` may acknowledge before flush; `Sync` waits for durability. */
export type StreamCommitMode = "Buffered" | "Sync";

/** Raw snake-case commit payload retained for compatibility with future broker fields. */
export interface StreamCommitPayload {
  /** Broker event label. */
  event?: string;
  /** First resource offset encoded as a safe JavaScript number. */
  first_resource_offset?: number;
  /** Last resource offset encoded as a safe JavaScript number. */
  last_resource_offset?: number;
  /** First area offset encoded as a safe JavaScript number. */
  first_area_offset?: number;
  /** Last area offset encoded as a safe JavaScript number. */
  last_area_offset?: number;
  /** First realm offset encoded as a safe JavaScript number. */
  first_realm_offset?: number;
  /** Last realm offset encoded as a safe JavaScript number. */
  last_realm_offset?: number;
  /** Number of committed records. */
  batch_size?: number;
}

/** Summary emitted when a committed stream batch matches a subscription. */
export interface StreamCommitNotification {
  /** Concrete resource route that committed. */
  route: string;
  /** Broker event label, when supplied. */
  event?: string;
  /** First resource-local offset in the commit. */
  firstResourceOffset?: bigint;
  /** Last resource-local offset in the commit. */
  lastResourceOffset?: bigint;
  /** First area-wide offset in the commit. */
  firstAreaOffset?: bigint;
  /** Last area-wide offset in the commit. */
  lastAreaOffset?: bigint;
  /** First realm-wide offset in the commit. */
  firstRealmOffset?: bigint;
  /** Last realm-wide offset in the commit. */
  lastRealmOffset?: bigint;
  /** Number of records committed. */
  batchSize?: number;
  /** Raw decoded broker payload retained for forward-compatible fields. */
  payload: StreamCommitPayload;
}

/** Commit-notification callback; execution follows configured async-handler limits. */
export type StreamCommitHandler = (notification: StreamCommitNotification) => void | Promise<void>;

/** Active stream commit subscription. */
export interface StreamSubscription extends AsyncDisposable {
  /** Stops this consumer and releases shared wire state after the final consumer leaves. */
  unsubscribe(): Promise<void>;
}

export function createStreamSubscription(
  unsubscribeFn: () => Promise<void>,
  signal?: AbortSignal,
): StreamSubscription {
  return createSubscriptionHandle<StreamSubscription>(unsubscribeFn, signal);
}

/**
 * Stream session for write operations.
 * Obtained from `StreamClient.begin()`.
 */
export interface StreamSession {
  /**
   * Appends one record using optimistic concurrency and returns its assigned
   * resource-local offset. Serialize operations on a session; after an
   * ambiguous post-send failure, resolve the expected offset before retrying.
   */
  append(options: StreamAppendOptions): Promise<bigint>;

  /**
   * Finalizes the session using the requested durability. A successful
   * `Buffered` commit may precede durable flush; use `Sync` when acknowledgement
   * must imply durability. The handle closes after commit.
   */
  commit(options: {
    /** Required acknowledgement/durability level. */
    mode: StreamCommitMode;
    /** Cancels waiting; a post-send cancellation can leave commit outcome ambiguous. */
    signal?: AbortSignal;
  }): Promise<void>;

  /**
   * Rolls back and discards uncommitted appends. The handle closes afterward.
   */
  rollback(options?: {
    /** Cancels waiting; the local session still becomes unusable. */
    signal?: AbortSignal;
  }): Promise<void>;

  /**
   * Returns whether the local handle can still issue operations. A disconnect
   * invalidates it even if the broker later reconnects.
   */
  isOpen(): boolean;
  /** Rolls back an open session during `await using` cleanup. */
  [Symbol.asyncDispose](): Promise<void>;
}

/**
 * Stream operation status codes
 */
export enum StreamStatus {
  /** Operation succeeded. */
  Ok = 0,
  /** Stream route does not exist. */
  StreamNotFound = 1,
  /** Offset is outside retained history. */
  OffsetOutOfRange = 2,
  /** Offset is invalid for this operation. */
  InvalidOffset = 3,
  /** Stream capacity is exhausted. */
  StreamFull = 4,
  /** Write session does not exist. */
  SessionNotFound = 5,
  /** Write session is already finalized. */
  SessionClosed = 6,
  /** Optimistic expected offset did not match. */
  ExpectedOffsetMismatch = 7,
}

export const StreamStatusNames: Record<number, string> = {
  [StreamStatus.StreamNotFound]: "StreamNotFound",
  [StreamStatus.OffsetOutOfRange]: "OffsetOutOfRange",
  [StreamStatus.InvalidOffset]: "InvalidOffset",
  [StreamStatus.StreamFull]: "StreamFull",
  [StreamStatus.SessionNotFound]: "SessionNotFound",
  [StreamStatus.SessionClosed]: "SessionClosed",
  [StreamStatus.ExpectedOffsetMismatch]: "ExpectedOffsetMismatch",
};
