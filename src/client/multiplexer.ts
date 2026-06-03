/**
 * Multiplexer for correlating in-flight requests with responses
 * Per CLIENT_SPEC.md and fitz-go/internal/core/connection/mux.go
 *
 * Key Pattern: FIFO ordering per MessageType
 * - Responses matched to requests in order received per MessageType
 * - Matches server's sequential processing model per actor/route
 * - No correlation IDs for most operations (except RPC streaming)
 */

import { Deferred, ConnectionState, FitzMeter, FitzTracer } from "../core/types";
import { ConnectionError, TimeoutError } from "../core/errors";

export interface MultiplexerObservability {
  meter?: FitzMeter;
  tracer?: FitzTracer;
}

export interface PendingRequest {
  deferred: Deferred<Uint8Array>;
  timeout: ReturnType<typeof setTimeout>;
  sentAt: Date;
}

/**
 * Handler for server-pushed frames.
 *
 * Most pushed traffic uses dedicated `*_NOTIFY` message types. RPC is the
 * protocol exception: inbound worker requests and streaming responses arrive on
 * `MSG_RPC_REQUEST` and `MSG_RPC_RESPONSE`, so those frames are only treated as
 * pushes when there is no matching FIFO request waiting on the same type.
 */
export type NotificationHandler = (payload: Uint8Array) => void;

/**
 * RPC correlation handler for streamed responses
 * @param correlationId 16-byte correlation ID
 * @param payload Response payload
 */
export type RpcCorrelationHandler = (correlationId: Uint8Array, payload: Uint8Array) => void;

export type Multiplexer = ReturnType<typeof createMultiplexer>;

export function createMultiplexer(observability: MultiplexerObservability = {}) {
  const pending: Map<number, PendingRequest[]> = new Map();
  const notificationHandlers: Map<number, NotificationHandler> = new Map();
  const optionalResponses: Map<number, number> = new Map();
  let state: ConnectionState = ConnectionState.Disconnected;

  let requestsInFlight = 0;
  let requestsTotal = 0;
  let responsesTotal = 0;
  let responsesDropped = 0;
  let responsesIgnored = 0;

  const getOrCreatePendingQueue = (messageType: number): PendingRequest[] => {
    const existing = pending.get(messageType);
    if (existing) {
      return existing;
    }

    const created: PendingRequest[] = [];
    pending.set(messageType, created);
    return created;
  };

  const setConnected = (): void => {
    state = ConnectionState.Authenticated;
  };

  const setDisconnected = (): void => {
    state = ConnectionState.Disconnected;
    optionalResponses.clear();
    cancelAll();
  };

  const registerNotificationHandler = (messageType: number, handler: NotificationHandler): void => {
    notificationHandlers.set(messageType, handler);
  };

  const unregisterNotificationHandler = (messageType: number): void => {
    notificationHandlers.delete(messageType);
  };

  const expectOptionalResponse = (messageType: number): (() => void) => {
    const nextCount = (optionalResponses.get(messageType) ?? 0) + 1;
    optionalResponses.set(messageType, nextCount);

    return () => {
      const currentCount = optionalResponses.get(messageType) ?? 0;
      if (currentCount <= 1) {
        optionalResponses.delete(messageType);
        return;
      }
      optionalResponses.set(messageType, currentCount - 1);
    };
  };

  const request = async (
    messageType: number,
    frameData: Uint8Array,
    send: (data: Uint8Array) => Promise<void>,
    timeoutMs: number,
    signal?: AbortSignal,
  ): Promise<Uint8Array> => {
    const attributes = { messageType };
    const span = observability.tracer?.startSpan("fitz.request", attributes);
    let spanEnded = false;

    if (signal?.aborted) {
      const error = abortError();
      span?.recordException(error);
      span?.end();
      observability.meter?.counter("fitz.request.failed", 1, {
        ...attributes,
        error: error.name,
      });
      throw error;
    }

    const deferred = Deferred<Uint8Array>();
    let settled = false;
    let timeout: ReturnType<typeof setTimeout>;
    let onAbort: (() => void) | undefined;

    const finalize = (): boolean => {
      if (settled) {
        return false;
      }

      settled = true;
      clearTimeout(timeout);
      if (signal && onAbort) {
        signal.removeEventListener("abort", onAbort);
      }
      return true;
    };

    const failRequest = (error: Error): void => {
      if (!finalize()) {
        return;
      }

      unregisterRequest(messageType, deferred);
      observability.meter?.counter("fitz.request.failed", 1, {
        ...attributes,
        error: error.name,
      });
      span?.recordException(error);
      if (!spanEnded) {
        span?.end();
        spanEnded = true;
      }
      void deferred.promise.catch(() => undefined);
      deferred.reject(error);
    };

    timeout = setTimeout(() => {
      observability.meter?.counter("fitz.request.timeout", 1, attributes);
      failRequest(
        new TimeoutError(`Request timeout for message type ${messageType} after ${timeoutMs}ms`, {
          messageType,
          timeoutMs,
        }),
      );
    }, timeoutMs);

    if (signal) {
      onAbort = () => {
        failRequest(abortError());
      };
      signal.addEventListener("abort", onAbort, { once: true });
    }

    const requestEntry: PendingRequest = {
      deferred,
      timeout,
      sentAt: new Date(),
    };

    getOrCreatePendingQueue(messageType).push(requestEntry);

    requestsInFlight++;
    requestsTotal++;
    observability.meter?.counter("fitz.request.started", 1, attributes);
    observability.meter?.gauge?.("fitz.requests.in_flight", requestsInFlight, attributes);

    try {
      await send(frameData);
    } catch (err) {
      const alreadySettled = !finalize();
      if (alreadySettled) {
        if (signal?.aborted) {
          throw abortError();
        }
        throw err;
      }

      unregisterRequest(messageType, deferred);
      observability.meter?.counter("fitz.request.failed", 1, {
        ...attributes,
        error: err instanceof Error ? err.name : "unknown",
      });
      if (!spanEnded) {
        span?.recordException(err);
        span?.end();
        spanEnded = true;
      }
      if (signal?.aborted) {
        throw abortError();
      }
      throw err;
    }

    if (state !== ConnectionState.Authenticated) {
      const error = new ConnectionError("Connection closed or reset", { state });
      unregisterRequest(messageType, deferred);
      finalize();
      throw error;
    }

    const payload = await deferred.promise.catch((error) => {
      throw error;
    });
    finalize();
    const durationMs = Date.now() - requestEntry.sentAt.getTime();
    span?.setAttribute("fitz.request.duration_ms", durationMs);
    observability.meter?.histogram("fitz.request.duration", durationMs, attributes);
    if (!spanEnded) {
      span?.end();
      spanEnded = true;
    }
    return payload;
  };

  const unregisterRequest = (messageType: number, deferred: Deferred<Uint8Array>): void => {
    const queue = pending.get(messageType);
    if (!queue) return;

    const index = queue.findIndex((r) => r.deferred === deferred);
    if (index >= 0) {
      queue.splice(index, 1);
      requestsInFlight--;
      observability.meter?.gauge?.("fitz.requests.in_flight", requestsInFlight, {
        messageType,
      });
      if (queue.length === 0) {
        pending.delete(messageType);
      }
    }
  };

  const dispatch = (messageType: number, payload: Uint8Array): void => {
    const queue = pending.get(messageType);
    if (queue && queue.length > 0) {
      const request = queue.shift();
      if (!request) {
        return;
      }
      if (queue.length === 0) {
        pending.delete(messageType);
      }

      clearTimeout(request.timeout);
      requestsInFlight--;
      responsesTotal++;
      observability.meter?.counter("fitz.response.received", 1, {
        messageType,
      });
      observability.meter?.gauge?.("fitz.requests.in_flight", requestsInFlight, {
        messageType,
      });

      request.deferred.resolve(payload);
      return;
    }

    const handler = notificationHandlers.get(messageType);
    if (handler) {
      try {
        handler(payload);
      } catch {
        // Best-effort notification dispatch.
      }
      return;
    }

    const optionalCount = optionalResponses.get(messageType) ?? 0;
    if (optionalCount > 0) {
      if (optionalCount === 1) {
        optionalResponses.delete(messageType);
      } else {
        optionalResponses.set(messageType, optionalCount - 1);
      }
      responsesIgnored++;
      observability.meter?.counter("fitz.response.ignored", 1, {
        messageType,
      });
      return;
    }

    if (state !== ConnectionState.Authenticated) {
      responsesIgnored++;
      observability.meter?.counter("fitz.response.ignored", 1, {
        messageType,
        state,
      });
      return;
    }

    responsesDropped++;
    observability.meter?.counter("fitz.response.dropped", 1, {
      messageType,
    });
  };

  const cancelAll = (): void => {
    for (const [, queue] of pending) {
      for (const request of queue) {
        clearTimeout(request.timeout);
        void request.deferred.promise.catch(() => undefined);
        void Promise.resolve().then(() =>
          request.deferred.reject(
            new ConnectionError("Connection closed or reset", {
              state,
            }),
          ),
        );
      }
    }
    pending.clear();
    requestsInFlight = 0;
    observability.meter?.gauge?.("fitz.requests.in_flight", 0);
  };

  const getMetrics = () => ({
    requestsInFlight,
    requestsTotal,
    responsesTotal,
    responsesDropped,
    responsesIgnored,
  });

  const getInFlightCount = (): number => requestsInFlight;

  const hasPending = (): boolean => requestsInFlight > 0;

  const getState = (): ConnectionState => state;

  return {
    setConnected,
    setDisconnected,
    registerNotificationHandler,
    unregisterNotificationHandler,
    expectOptionalResponse,
    request,
    dispatch,
    cancelAll,
    getMetrics,
    getInFlightCount,
    hasPending,
    getState,
  };
}

interface MultiplexerConstructor {
  new (observability?: MultiplexerObservability): Multiplexer;
}

export const Multiplexer: MultiplexerConstructor = function (
  observability: MultiplexerObservability = {},
) {
  return createMultiplexer(observability);
} as unknown as MultiplexerConstructor;

function abortError(): Error {
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}
