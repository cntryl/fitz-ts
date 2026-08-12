/**
 * Stream session implementation
 * Per fitz-go/internal/domains/stream/stream.go
 */

import type { DisconnectListenerPort, RequestPort } from "../base";
import { StreamCodec } from "./codec";
import {
  StreamAppendOptions,
  StreamCommitMode,
  StreamSession,
  StreamStatus,
  StreamStatusNames,
} from "./types";
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

  const checkStatus = (
    response: { status: number; errorCode?: number; errorMessage?: string },
    operation: string,
  ): void => {
    if (response.status === StreamStatus.Ok) {
      return;
    }

    const code = response.errorCode ?? response.status;
    const statusName = response.errorMessage ?? formatStatusName(code, StreamStatusNames);
    throw new StreamError(`${operation} failed: ${statusName}`, operation, code);
  };

  const append = async (options: StreamAppendOptions): Promise<bigint> => {
    ensureOpen();
    const payload = StreamCodec.encodeAppend(
      sessionId,
      options.expectedOffset,
      options.body,
      undefined,
      options.discriminator,
    );
    const response = await connection.request(MSG_STREAM_APPEND, payload, options.signal);
    const decoded = StreamCodec.decodeAppendResponse(response);

    checkStatus(decoded, "APPEND");

    return decoded.offset ?? 0n;
  };

  const commit = async (options: {
    mode: StreamCommitMode;
    signal?: AbortSignal;
  }): Promise<void> => {
    ensureOpen();

    const payload = StreamCodec.encodeCommit(sessionId, options.mode);
    const response = await connection.request(MSG_STREAM_COMMIT, payload, options.signal);
    const decoded = StreamCodec.decodeCommitResponse(response);

    checkStatus(decoded, "COMMIT");
    closed = true;
    unsubscribeDisconnect();
  };

  const rollback = async (options: { signal?: AbortSignal } = {}): Promise<void> => {
    if (closed) {
      return;
    }

    closed = true;
    unsubscribeDisconnect();

    const payload = StreamCodec.encodeRollback(sessionId);
    const response = await connection.request(MSG_STREAM_ROLLBACK, payload, options.signal);
    const decoded = StreamCodec.decodeRollbackResponse(response);
    checkStatus(decoded, "ROLLBACK");
  };

  const isOpen = (): boolean => !closed;

  const asyncDispose = async (): Promise<void> => {
    try {
      await rollback();
    } catch {
      // Disposal is explicitly best effort.
    }
  };

  return {
    append,
    commit,
    rollback,
    isOpen,
    [Symbol.asyncDispose]: asyncDispose,
  };
}
