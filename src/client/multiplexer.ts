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
  sentAt: number | undefined;
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
  type PendingQueue = {
    queue: Array<PendingRequest | undefined>;
    head: number;
    tail: number;
    size: number;
  };

  const pending: Map<number, PendingQueue> = new Map();
  const notificationHandlers: Map<number, NotificationHandler> = new Map();
  const optionalResponses: Map<number, number> = new Map();
  let state: ConnectionState = ConnectionState.Disconnected;

  let requestsInFlight = 0;
  let requestsTotal = 0;
  let responsesTotal = 0;
  let responsesDropped = 0;
  let responsesIgnored = 0;

  const meter = observability.meter;
  const tracer = observability.tracer;
  const hasObservability = meter !== undefined || tracer !== undefined;

  const getOrCreatePendingQueue = (messageType: number): PendingQueue => {
    const existing = pending.get(messageType);
    if (existing) {
      return existing;
    }

    const created: PendingQueue = {
      queue: Array.from({ length: 128 }) as Array<PendingRequest | undefined>,
      head: 0,
      tail: 0,
      size: 0,
    };
    pending.set(messageType, created);
    return created;
  };

  const growPendingQueue = (queueEntry: PendingQueue): void => {
    const newCapacity = Math.max(queueEntry.queue.length * 2, 16);
    const newQueue = Array.from({ length: newCapacity }) as Array<PendingRequest | undefined>;

    for (let i = 0; i < queueEntry.size; i += 1) {
      newQueue[i] = queueEntry.queue[(queueEntry.head + i) % queueEntry.queue.length];
    }

    queueEntry.queue = newQueue;
    queueEntry.head = 0;
    queueEntry.tail = queueEntry.size;
  };

  const enqueuePendingRequest = (queueEntry: PendingQueue, request: PendingRequest): void => {
    if (queueEntry.size >= queueEntry.queue.length) {
      growPendingQueue(queueEntry);
    }

    queueEntry.queue[queueEntry.tail] = request;
    queueEntry.tail = (queueEntry.tail + 1) % queueEntry.queue.length;
    queueEntry.size += 1;
  };

  const dequeuePendingRequest = (queueEntry: PendingQueue): PendingRequest => {
    const request = queueEntry.queue[queueEntry.head];
    if (!request) {
      throw new Error("Pending queue invariant broken");
    }

    queueEntry.queue[queueEntry.head] = undefined;
    queueEntry.head = (queueEntry.head + 1) % queueEntry.queue.length;
    queueEntry.size -= 1;
    return request;
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
    const attributes = hasObservability ? { messageType } : undefined;
    const span = tracer?.startSpan("fitz.request", attributes);
    let spanEnded = false;

    if (signal?.aborted) {
      const error = abortError();
      span?.recordException(error);
      span?.end();
      meter?.counter("fitz.request.failed", 1, {
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
      meter?.counter("fitz.request.failed", 1, {
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
      meter?.counter("fitz.request.timeout", 1, attributes);
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
      sentAt: Date.now(),
    };

    const queue = getOrCreatePendingQueue(messageType);
    enqueuePendingRequest(queue, requestEntry);

    requestsInFlight++;
    requestsTotal++;
    meter?.counter("fitz.request.started", 1, attributes);
    meter?.gauge?.("fitz.requests.in_flight", requestsInFlight, attributes);

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
      meter?.counter("fitz.request.failed", 1, {
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

    const payload = await deferred.promise;
    finalize();
    const durationMs = Date.now() - requestEntry.sentAt!;
    span?.setAttribute("fitz.request.duration_ms", durationMs);
    meter?.histogram("fitz.request.duration", durationMs, attributes);
    if (!spanEnded) {
      span?.end();
      spanEnded = true;
    }
    return payload;
  };

  const unregisterRequest = (messageType: number, deferred: Deferred<Uint8Array>): void => {
    const queueEntry = pending.get(messageType);
    if (!queueEntry || queueEntry.size === 0) return;

    let index = -1;
    for (let i = 0; i < queueEntry.size; i += 1) {
      const currentIndex = (queueEntry.head + i) % queueEntry.queue.length;
      if (queueEntry.queue[currentIndex]?.deferred === deferred) {
        index = currentIndex;
        break;
      }
    }

    if (index === -1) {
      return;
    }

    if (index === queueEntry.head) {
      queueEntry.queue[queueEntry.head] = undefined;
      queueEntry.head = (queueEntry.head + 1) % queueEntry.queue.length;
      queueEntry.size -= 1;
    } else {
      let currentIndex = index;
      while (true) {
        const nextIndex = (currentIndex + 1) % queueEntry.queue.length;
        if (nextIndex === queueEntry.tail) {
          break;
        }
        queueEntry.queue[currentIndex] = queueEntry.queue[nextIndex];
        currentIndex = nextIndex;
      }

      const lastIndex = queueEntry.tail === 0 ? queueEntry.queue.length - 1 : queueEntry.tail - 1;
      queueEntry.queue[lastIndex] = undefined;
      queueEntry.tail = lastIndex;
      queueEntry.size -= 1;
    }

    requestsInFlight--;
    meter?.gauge?.("fitz.requests.in_flight", requestsInFlight, {
      messageType,
    });

    if (queueEntry.size === 0) {
      pending.delete(messageType);
    }
  };

  const dispatch = (messageType: number, payload: Uint8Array): void => {
    const queueEntry = pending.get(messageType);
    if (queueEntry && queueEntry.size > 0) {
      const request = dequeuePendingRequest(queueEntry);
      if (queueEntry.size === 0) {
        pending.delete(messageType);
      }

      clearTimeout(request.timeout);
      requestsInFlight--;
      responsesTotal++;
      meter?.counter("fitz.response.received", 1, {
        messageType,
      });
      meter?.gauge?.("fitz.requests.in_flight", requestsInFlight, {
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
      meter?.counter("fitz.response.ignored", 1, {
        messageType,
      });
      return;
    }

    if (state !== ConnectionState.Authenticated) {
      responsesIgnored++;
      meter?.counter("fitz.response.ignored", 1, {
        messageType,
        state,
      });
      return;
    }

    responsesDropped++;
    meter?.counter("fitz.response.dropped", 1, {
      messageType,
    });
  };

  const cancelAll = (): void => {
    for (const [, queueEntry] of pending) {
      for (let i = 0; i < queueEntry.size; i += 1) {
        const index = (queueEntry.head + i) % queueEntry.queue.length;
        const request = queueEntry.queue[index];
        if (!request) {
          continue;
        }
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
    meter?.gauge?.("fitz.requests.in_flight", 0);
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
