/**
 * Stream session implementation
 * Per fitz-go/internal/domains/stream/stream.go
 */

import type { DisconnectListenerPort, RequestPort } from "../base";
import { StreamCodec } from "./codec";
import { StreamAppendOptions, StreamCommitMode, StreamSession, StreamStatus } from "./types";
import { StreamError } from "../../core/errors";
import { MSG_STREAM_APPEND, MSG_STREAM_COMMIT, MSG_STREAM_ROLLBACK } from "../../frame/types";
import { formatStatusName } from "../internal/status";

export function createStreamSession(
  connection: RequestPort & DisconnectListenerPort,
  _route: string,
  sessionId: bigint,
): StreamSession {
  let closed = false;
  let unsubscribeDisconnect: () => void = () => undefined;
  unsubscribeDisconnect = connection.onDisconnect(() => {
    closed = true;
    unsubscribeDisconnect();
  });

  const ensureOpen = (): void => {
    if (closed) {
      throw new StreamError("Stream session already closed", "SESSION_CLOSED");
    }
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

    const statusName = formatStatusName(status, statusNames);
    throw new StreamError(`${operation} failed: ${statusName}`, statusName, status);
  };

  const append = async (
    expectedOffset: bigint,
    body: Uint8Array,
    optionsOrSignal?: AbortSignal | StreamAppendOptions,
    signal?: AbortSignal,
  ): Promise<bigint> => {
    ensureOpen();

    const { options, requestSignal } = normalizeAppendArguments(optionsOrSignal, signal);
    const payload = StreamCodec.encodeAppend(
      sessionId,
      expectedOffset,
      body,
      undefined,
      options?.discriminator,
    );
    const response = await connection.request(MSG_STREAM_APPEND, payload, requestSignal);
    const decoded = StreamCodec.decodeAppendResponse(response);

    checkStatus(decoded.status, "APPEND");

    return decoded.offset ?? 0n;
  };

  const commit = async (mode: StreamCommitMode, signal?: AbortSignal): Promise<void> => {
    ensureOpen();

    const payload = StreamCodec.encodeCommit(sessionId, mode);
    const response = await connection.request(MSG_STREAM_COMMIT, payload, signal);
    const decoded = StreamCodec.decodeCommitResponse(response);

    checkStatus(decoded.status, "COMMIT");
    closed = true;
    unsubscribeDisconnect();
  };

  const rollback = async (signal?: AbortSignal): Promise<void> => {
    if (closed) {
      return;
    }

    closed = true;
    unsubscribeDisconnect();

    const payload = StreamCodec.encodeRollback(sessionId);
    try {
      const response = await connection.request(MSG_STREAM_ROLLBACK, payload, signal);
      const decoded = StreamCodec.decodeRollbackResponse(response);
      checkStatus(decoded.status, "ROLLBACK");
    } catch {
      // Ignore rollback errors
    }
  };

  const isOpen = (): boolean => !closed;

  return {
    append,
    commit,
    rollback,
    isOpen,
  };
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
