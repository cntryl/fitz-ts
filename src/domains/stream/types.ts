/**
 * Stream domain type definitions
 * Stream uses session-based transactional semantics:
 * 1. Begin() returns a session with server-assigned sessionID
 * 2. Append() on session (no offset tracking needed)
 * 3. Commit() or Rollback() to finalize
 */

/**
 * Stream record with offset, timestamp, and payload
 */
export interface StreamRecord {
  offset: bigint;
  timestamp: bigint;
  body: Uint8Array;
}

/**
 * Stream metadata.
 */
export interface StreamMetadata {
  firstOffset: bigint;
  lastOffset: bigint;
  recordCount: bigint;
}

export interface StreamCommitNotification {
  route: string;
  event?: string;
  firstResourceOffset?: bigint;
  lastResourceOffset?: bigint;
  batchSize?: number;
  payload: unknown;
}

export type StreamCommitHandler = (
  notification: StreamCommitNotification,
) => void | Promise<void>;

export class StreamSubscription {
  constructor(
    private readonly subId: bigint,
    private readonly pattern: string,
    private readonly unsubscribeFn: (pattern: string) => Promise<void>,
  ) {}

  async unsubscribe(): Promise<void> {
    await this.unsubscribeFn(this.pattern);
  }
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
  append(body: Uint8Array): Promise<bigint>;

  /**
   * Commit the write session and make appended records durable.
   */
  commit(): Promise<void>;

  /**
   * Roll back and discard uncommitted appends.
   */
  rollback(): Promise<void>;

  /**
   * Check if session is still open
   */
  isOpen(): boolean;
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
