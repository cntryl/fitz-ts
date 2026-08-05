/**
 * Stream domain type definitions
 * Stream uses session-based transactional semantics:
 * 1. Begin() returns a session with server-assigned sessionID
 * 2. Append(expectedOffset, ...) on session validates optimistic concurrency
 * 3. Commit() or Rollback() to finalize
 */

import "../../core/async-dispose";

/**
 * Stream record with offset, timestamp, and payload
 */
export interface StreamRecord {
  route: string;
  offset: bigint;
  timestamp: bigint;
  body: Uint8Array;
  areaOffset?: bigint;
  realmOffset?: bigint;
  globalOffset?: bigint;
  metadata?: Uint8Array;
}

/**
 * Stream metadata.
 */
export interface StreamMetadata {
  firstOffset: bigint;
  lastOffset: bigint;
  recordCount: bigint;
  maxBatchEvents?: bigint;
  maxBatchBytes?: bigint;
  ttlSeconds?: bigint;
  areaWatermark?: bigint;
  realmWatermark?: bigint;
}

export type StreamDiscriminator = string;

export type StreamFilteredReason = "server_filter" | "permission" | "projection";

export type StreamFilterClause =
  | { kind: "Equals"; value: string }
  | { kind: "NotEquals"; value: string }
  | { kind: "StartsWith"; value: string }
  | { kind: "AnyOf"; values: string[] };

export interface StreamFilterSet {
  clauses: StreamFilterClause[];
}

export interface StreamAppendOptions {
  discriminator?: StreamDiscriminator;
}

export interface StreamReadOptions {
  maxBytes?: bigint;
  filter?: StreamFilterSet;
  cursorFingerprint?: bigint;
  capturedWatermark?: bigint;
  /** @deprecated use cursorFingerprint/capturedWatermark */
  resumeRealm?: string;
  signal?: AbortSignal;
}

export interface StreamReadCursor {
  lastResourceOffset: bigint;
  lastAreaOffset?: bigint;
  lastRealmOffset?: bigint;
  lastGlobalOffset?: bigint;
  cursorFingerprint?: bigint;
  capturedWatermark?: bigint;
  /** @deprecated retained for legacy cursor consumers */
  currentRealm?: string;
  hasMore: boolean;
}

export interface StreamReadEvent {
  kind: "event";
  route: string;
  record: StreamRecord;
}

export interface StreamReadFiltered {
  kind: "filtered";
  route: string;
  offset: bigint;
  reason?: StreamFilteredReason;
}

export interface StreamReadFilteredRange {
  kind: "filtered_range";
  route: string;
  fromOffset: bigint;
  toOffset: bigint;
  reason?: StreamFilteredReason;
}

export type StreamReadItem = StreamReadEvent | StreamReadFiltered | StreamReadFilteredRange;

export interface StreamReadPage {
  items: StreamReadItem[];
  cursor: StreamReadCursor;
}

export type StreamCommitMode = "Buffered" | "Sync";

export interface StreamCommitPayload {
  event?: string;
  first_resource_offset?: number;
  last_resource_offset?: number;
  first_area_offset?: number;
  last_area_offset?: number;
  first_realm_offset?: number;
  last_realm_offset?: number;
  batch_size?: number;
}

export interface StreamCommitNotification {
  route: string;
  event?: string;
  firstResourceOffset?: bigint;
  lastResourceOffset?: bigint;
  firstAreaOffset?: bigint;
  lastAreaOffset?: bigint;
  firstRealmOffset?: bigint;
  lastRealmOffset?: bigint;
  batchSize?: number;
  payload: StreamCommitPayload;
}

export type StreamCommitHandler = (notification: StreamCommitNotification) => void | Promise<void>;

export type StreamSubscription = ReturnType<typeof createStreamSubscription>;

export function createStreamSubscription(
  subId: bigint,
  pattern: string,
  unsubscribeFn: (pattern: string) => Promise<void>,
) {
  const unsubscribe = async (): Promise<void> => {
    await unsubscribeFn(pattern);
  };

  return {
    subId,
    unsubscribe,
  };
}

/**
 * Stream session for write operations.
 * Obtained from `StreamClient.begin()`.
 */
export interface StreamSession {
  /**
   * Append a record to the stream.
   * Returns the assigned offset
   */
  append(expectedOffset: bigint, body: Uint8Array, signal?: AbortSignal): Promise<bigint>;
  append(
    expectedOffset: bigint,
    body: Uint8Array,
    options?: StreamAppendOptions,
    signal?: AbortSignal,
  ): Promise<bigint>;

  /**
   * Commit the write session and make appended records durable.
   */
  commit(mode: StreamCommitMode, signal?: AbortSignal): Promise<void>;

  /**
   * Roll back and discard uncommitted appends.
   */
  rollback(signal?: AbortSignal): Promise<void>;

  /**
   * Check if session is still open
   */
  isOpen(): boolean;
  [Symbol.asyncDispose](): Promise<void>;
}

/**
 * Stream operation status codes
 */
export enum StreamStatus {
  Ok = 0,
  StreamNotFound = 1,
  OffsetOutOfRange = 2,
  InvalidOffset = 3,
  StreamFull = 4,
  SessionNotFound = 5,
  SessionClosed = 6,
  ExpectedOffsetMismatch = 7,
}
