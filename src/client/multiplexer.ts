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
  deadline: number;
  timeoutIndex: number;
  queueIndex: number;
  sentAt: number | undefined;
  rejectRequest: (error: Error) => void;
  onComplete?: () => void;
}

/**
 * Handler for server-pushed frames.
 *
 * Most pushed traffic uses dedicated `*_NOTIFY` message types. Domains that
 * share request/response message types with pushed traffic can register a
 * classifier to route only domain-owned push frames ahead of FIFO matching.
 */
export type NotificationHandler = (payload: Uint8Array) => void;
export type PushFrameClassifier = (payload: Uint8Array) => boolean;
export type PushFrameClassifierRegistration = () => void;

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

  const pending: Record<number, PendingQueue | undefined> = {};
  const notificationHandlers: Map<number, NotificationHandler> = new Map();
  const pushClassifiers: Map<number, PushFrameClassifier> = new Map();
  const optionalResponses: Map<number, number> = new Map();
  let state: ConnectionState = ConnectionState.Disconnected;

  let timeoutEntries: PendingRequest[] = [];
  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  let nextTimeoutDeadline = Infinity;

  const swapTimeoutEntries = (leftIndex: number, rightIndex: number): void => {
    const left = timeoutEntries[leftIndex];
    const right = timeoutEntries[rightIndex];
    timeoutEntries[leftIndex] = right;
    timeoutEntries[rightIndex] = left;
    left.timeoutIndex = rightIndex;
    right.timeoutIndex = leftIndex;
  };

  const heapifyUp = (index: number): void => {
    while (index > 0) {
      const parentIndex = (index - 1) >> 1;
      if (timeoutEntries[index].deadline >= timeoutEntries[parentIndex].deadline) {
        return;
      }
      swapTimeoutEntries(index, parentIndex);
      index = parentIndex;
    }
  };

  const heapifyDown = (index: number): void => {
    const length = timeoutEntries.length;
    while (true) {
      const leftIndex = index * 2 + 1;
      const rightIndex = index * 2 + 2;
      let smallest = index;

      if (
        leftIndex < length &&
        timeoutEntries[leftIndex].deadline < timeoutEntries[smallest].deadline
      ) {
        smallest = leftIndex;
      }
      if (
        rightIndex < length &&
        timeoutEntries[rightIndex].deadline < timeoutEntries[smallest].deadline
      ) {
        smallest = rightIndex;
      }
      if (smallest === index) {
        return;
      }
      swapTimeoutEntries(index, smallest);
      index = smallest;
    }
  };

  const getNextTimeoutDeadline = (): number => {
    return timeoutEntries.length === 0 ? Infinity : timeoutEntries[0].deadline;
  };

  const removeTimeoutEntry = (request: PendingRequest): void => {
    const index = request.timeoutIndex;
    if (index < 0 || index >= timeoutEntries.length) {
      return;
    }

    const last = timeoutEntries.pop();
    if (!last || index === timeoutEntries.length) {
      request.timeoutIndex = -1;
      return;
    }

    timeoutEntries[index] = last;
    last.timeoutIndex = index;
    request.timeoutIndex = -1;
    heapifyDown(index);
    heapifyUp(index);
  };

  let requestsInFlight = 0;
  let requestsTotal = 0;
  let responsesTotal = 0;
  let responsesDropped = 0;
  let responsesIgnored = 0;

  const meter = observability.meter;
  const tracer = observability.tracer;
  const hasObservability = meter !== undefined || tracer !== undefined;

  const getOrCreatePendingQueue = (messageType: number): PendingQueue => {
    const existing = pending[messageType];
    if (existing) {
      return existing;
    }

    const created: PendingQueue = {
      queue: Array.from({ length: 128 }) as Array<PendingRequest | undefined>,
      head: 0,
      tail: 0,
      size: 0,
    };
    pending[messageType] = created;
    return created;
  };

  const growPendingQueue = (queueEntry: PendingQueue): void => {
    const newCapacity = Math.max(queueEntry.queue.length * 2, 16);
    const newQueue = Array.from({ length: newCapacity }) as Array<PendingRequest | undefined>;

    for (let i = 0; i < queueEntry.size; i += 1) {
      const entry = queueEntry.queue[(queueEntry.head + i) % queueEntry.queue.length];
      newQueue[i] = entry;
      if (entry) {
        entry.queueIndex = i;
      }
    }

    queueEntry.queue = newQueue;
    queueEntry.head = 0;
    queueEntry.tail = queueEntry.size;
  };

  const enqueuePendingRequest = (queueEntry: PendingQueue, request: PendingRequest): void => {
    if (queueEntry.size >= queueEntry.queue.length) {
      growPendingQueue(queueEntry);
    }

    request.queueIndex = queueEntry.tail;
    queueEntry.queue[queueEntry.tail] = request;
    queueEntry.tail = (queueEntry.tail + 1) % queueEntry.queue.length;
    queueEntry.size += 1;
  };

  const dequeuePendingRequest = (queueEntry: PendingQueue): PendingRequest => {
    while (queueEntry.size > 0) {
      const request = queueEntry.queue[queueEntry.head];
      queueEntry.queue[queueEntry.head] = undefined;
      queueEntry.head = (queueEntry.head + 1) % queueEntry.queue.length;
      if (request) {
        queueEntry.size -= 1;
        return request;
      }
    }

    throw new Error("Pending queue invariant broken");
  };

  const setConnected = (): void => {
    state = ConnectionState.Authenticated;
  };

  const setDisconnected = (): void => {
    state = ConnectionState.Disconnected;
    optionalResponses.clear();
    cancelAll();
  };

  const scheduleTimeout = (deadline: number): void => {
    if (timeoutHandle !== undefined && deadline >= nextTimeoutDeadline) {
      return;
    }

    if (timeoutHandle !== undefined) {
      clearTimeout(timeoutHandle);
      timeoutHandle = undefined;
    }

    nextTimeoutDeadline = deadline;
    const delay = Math.max(0, deadline - Date.now());
    timeoutHandle = setTimeout(handleTimeouts, delay);
  };

  const handleTimeouts = (): void => {
    timeoutHandle = undefined;
    nextTimeoutDeadline = Infinity;

    const nowMs = Date.now();

    while (timeoutEntries.length > 0 && timeoutEntries[0].deadline <= nowMs) {
      const request = timeoutEntries[0];
      removeTimeoutEntry(request);
      request.deadline = -1;
      meter?.counter("fitz.request.timeout", 1);
      request.rejectRequest(
        new TimeoutError(`Request timeout after ${nowMs - (request.sentAt ?? nowMs)}ms`, undefined),
      );
    }

    nextTimeoutDeadline = getNextTimeoutDeadline();
    if (nextTimeoutDeadline !== Infinity) {
      const delay = Math.max(0, nextTimeoutDeadline - Date.now());
      timeoutHandle = setTimeout(handleTimeouts, delay);
    }
  };

  const registerNotificationHandler = (messageType: number, handler: NotificationHandler): void => {
    notificationHandlers.set(messageType, handler);
  };

  const unregisterNotificationHandler = (messageType: number): void => {
    notificationHandlers.delete(messageType);
  };

  const registerPushFrameClassifier = (
    messageType: number,
    classifier: PushFrameClassifier,
  ): PushFrameClassifierRegistration => {
    pushClassifiers.set(messageType, classifier);
    return () => {
      if (pushClassifiers.get(messageType) === classifier) {
        pushClassifiers.delete(messageType);
      }
    };
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

  const request = (
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
      return Promise.reject(error);
    }

    const deferred = Deferred<Uint8Array>();
    let settled = false;
    let onAbort: (() => void) | undefined;

    const nowMs = Date.now();
    let requestEntry: PendingRequest;

    const finalize = (): boolean => {
      if (settled) {
        return false;
      }

      settled = true;
      removeTimeoutEntry(requestEntry);
      requestEntry.deadline = -1;
      if (signal && onAbort) {
        signal.removeEventListener("abort", onAbort);
      }
      return true;
    };

    const failRequest = (error: Error): void => {
      if (!finalize()) {
        return;
      }

      unregisterRequest(messageType, requestEntry);
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

    requestEntry = {
      deferred,
      deadline: nowMs + timeoutMs,
      timeoutIndex: timeoutEntries.length,
      sentAt: nowMs,
      queueIndex: -1,
      rejectRequest: (error: Error) => {
        if (!finalize()) {
          return;
        }

        removeTimeoutEntry(requestEntry);
        unregisterRequest(messageType, requestEntry);
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
      },
      onComplete: () => {
        if (!finalize()) {
          return;
        }

        const durationMs = Date.now() - requestEntry.sentAt!;
        span?.setAttribute("fitz.request.duration_ms", durationMs);
        meter?.histogram("fitz.request.duration", durationMs, attributes);
        if (!spanEnded) {
          span?.end();
          spanEnded = true;
        }
      },
    };

    timeoutEntries.push(requestEntry);
    heapifyUp(requestEntry.timeoutIndex);

    if (signal) {
      onAbort = () => {
        failRequest(abortError());
      };
      signal.addEventListener("abort", onAbort, { once: true });
    }

    scheduleTimeout(requestEntry.deadline);

    const queue = getOrCreatePendingQueue(messageType);
    enqueuePendingRequest(queue, requestEntry);

    requestsInFlight++;
    requestsTotal++;
    meter?.counter("fitz.request.started", 1, attributes);
    meter?.gauge?.("fitz.requests.in_flight", requestsInFlight, attributes);

    let sendResult: Promise<void>;
    try {
      sendResult = send(frameData);
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      failRequest(error);
      return Promise.reject(error);
    }

    const responsePromise = deferred.promise;
    const sendPromise = sendResult.then(
      () => {
        if (state !== ConnectionState.Authenticated) {
          const error = new ConnectionError("Connection closed or reset", { state });
          failRequest(error);
          throw error;
        }
        return responsePromise;
      },
      (err) => {
        const alreadySettled = !finalize();
        if (alreadySettled) {
          if (signal?.aborted) {
            throw abortError();
          }
          throw err;
        }

        unregisterRequest(messageType, requestEntry);
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
      },
    );

    return Promise.race([sendPromise, responsePromise]);
  };

  const unregisterRequest = (messageType: number, requestEntry: PendingRequest): void => {
    const queueEntry = pending[messageType];
    if (!queueEntry || queueEntry.size === 0) return;

    const index = requestEntry.queueIndex;
    if (index < 0 || index >= queueEntry.queue.length) {
      return;
    }

    if (queueEntry.queue[index] !== requestEntry) {
      return;
    }

    queueEntry.queue[index] = undefined;
    queueEntry.size -= 1;

    if (index === queueEntry.head) {
      while (queueEntry.size > 0 && queueEntry.queue[queueEntry.head] === undefined) {
        queueEntry.head = (queueEntry.head + 1) % queueEntry.queue.length;
      }
    } else {
      const expectedTailIndex =
        queueEntry.tail === 0 ? queueEntry.queue.length - 1 : queueEntry.tail - 1;
      if (index === expectedTailIndex) {
        queueEntry.tail = index;
      }
    }

    if (queueEntry.size === 0) {
      delete pending[messageType];
    }

    requestsInFlight--;
    meter?.gauge?.("fitz.requests.in_flight", requestsInFlight, {
      messageType,
    });
  };

  const dispatch = (messageType: number, payload: Uint8Array): void => {
    const handler = notificationHandlers.get(messageType);
    const pushClassifier = pushClassifiers.get(messageType);
    if (handler && pushClassifier?.(payload)) {
      try {
        handler(payload);
      } catch {
        // Best-effort notification dispatch.
      }
      return;
    }

    const queueEntry = pending[messageType];
    if (queueEntry && queueEntry.size > 0) {
      const request = dequeuePendingRequest(queueEntry);
      if (queueEntry.size === 0) {
        delete pending[messageType];
      }

      request.deadline = -1;
      requestsInFlight--;
      responsesTotal++;
      meter?.counter("fitz.response.received", 1, {
        messageType,
      });
      meter?.gauge?.("fitz.requests.in_flight", requestsInFlight, {
        messageType,
      });

      request.deferred.resolve(payload);
      request.onComplete?.();
      return;
    }

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
    if (timeoutHandle !== undefined) {
      clearTimeout(timeoutHandle);
      timeoutHandle = undefined;
    }
    const requestsToCancel: PendingRequest[] = [];

    for (const queueEntry of Object.values(pending)) {
      if (!queueEntry) {
        continue;
      }

      for (const request of queueEntry.queue) {
        if (request) {
          requestsToCancel.push(request);
        }
      }
    }

    for (const request of requestsToCancel) {
      request.rejectRequest(
        new ConnectionError("Connection closed or reset", {
          state,
        }),
      );
    }

    timeoutEntries = [];
    nextTimeoutDeadline = Infinity;

    for (const key in pending) {
      delete pending[key];
    }

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
    registerPushFrameClassifier,
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
