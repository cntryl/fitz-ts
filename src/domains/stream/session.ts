/**
 * Stream session implementation
 * Per fitz-go/internal/domains/stream/stream.go
 */

import { Connection } from "../../client/connection";
import { StreamCodec } from "./codec";
import { StreamAppendOptions, StreamCommitMode, StreamSession, StreamStatus } from "./types";
import { StreamError } from "../../core/errors";
import { MSG_STREAM_APPEND, MSG_STREAM_COMMIT, MSG_STREAM_ROLLBACK } from "../../frame/types";

export class StreamSessionImpl implements StreamSession {
  private readonly connection: Connection;
  private readonly sessionId: bigint;
  private closed = false;
  private readonly unsubscribeDisconnect: () => void;

  constructor(connection: Connection, _route: string, sessionId: bigint) {
    this.connection = connection;
    this.sessionId = sessionId;
    this.unsubscribeDisconnect = this.connection.onDisconnect(() => {
      this.closed = true;
    });
  }

  /**
   * Append a record to the stream.
   * Returns the assigned offset
   */
  async append(expectedOffset: bigint, body: Uint8Array, signal?: AbortSignal): Promise<bigint>;
  async append(
    expectedOffset: bigint,
    body: Uint8Array,
    options?: StreamAppendOptions,
    signal?: AbortSignal,
  ): Promise<bigint>;
  async append(
    expectedOffset: bigint,
    body: Uint8Array,
    optionsOrSignal?: AbortSignal | StreamAppendOptions,
    signal?: AbortSignal,
  ): Promise<bigint> {
    this.ensureOpen();

    const { options, requestSignal } = normalizeAppendArguments(optionsOrSignal, signal);
    const payload = StreamCodec.encodeAppend(
      this.sessionId,
      expectedOffset,
      body,
      undefined,
      options?.discriminator,
    );
    const response = await this.connection.request(MSG_STREAM_APPEND, payload, requestSignal);
    const decoded = StreamCodec.decodeAppendResponse(response);

    this.checkStatus(decoded.status, "APPEND");

    return decoded.offset ?? 0n;
  }

  /**
   * Commit the write session and make appends durable.
   */
  async commit(mode: StreamCommitMode, signal?: AbortSignal): Promise<void> {
    this.ensureOpen();
    this.closed = true;
    this.unsubscribeDisconnect();

    const payload = StreamCodec.encodeCommit(this.sessionId, mode);
    const response = await this.connection.request(MSG_STREAM_COMMIT, payload, signal);
    const decoded = StreamCodec.decodeCommitResponse(response);

    this.checkStatus(decoded.status, "COMMIT");
  }

  /**
   * Roll back and discard uncommitted appends.
   */
  async rollback(signal?: AbortSignal): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.unsubscribeDisconnect();

    const payload = StreamCodec.encodeRollback(this.sessionId);
    try {
      const response = await this.connection.request(MSG_STREAM_ROLLBACK, payload, signal);
      const decoded = StreamCodec.decodeRollbackResponse(response);
      this.checkStatus(decoded.status, "ROLLBACK");
    } catch {
      // Ignore rollback errors
    }
  }

  /**
   * Check if the session is still open.
   */
  isOpen(): boolean {
    return !this.closed;
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new StreamError("Stream session already closed", "SESSION_CLOSED");
    }
  }

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
    throw new StreamError(`${operation} failed: ${statusName}`, statusName, status);
  }
}

function normalizeAppendArguments(
  optionsOrSignal?: AbortSignal | StreamAppendOptions,
  signal?: AbortSignal,
): { options?: StreamAppendOptions; requestSignal?: AbortSignal } {
  if (isAbortSignal(optionsOrSignal)) {
    return { requestSignal: optionsOrSignal };
  }

  return {
    options: optionsOrSignal,
    requestSignal: signal,
  };
}

function isAbortSignal(value: unknown): value is AbortSignal {
  return (
    typeof value === "object" &&
    value !== null &&
    "aborted" in value &&
    typeof (value as AbortSignal).addEventListener === "function"
  );
}
