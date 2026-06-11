/**
 * Connection manager for Fitz protocol.
 *
 * CONNECT is a silent-success handshake. A newly opened transport is treated as
 * authenticated only if it remains open for a short settle window after the
 * CONNECT frame is sent.
 */

import { Transport } from "../transport/types";
import {
  AsyncHandlerOptions,
  ConnectionState,
  Deferred,
  FitzObservability,
  HeartbeatOptions,
  RetryOptions,
  TokenProvider,
} from "../core/types";
import { createScope, Scope } from "../core/lifecycle";
import { utf8Encoder } from "../core/buffer";
import { FrameCodec, FrameParser } from "../frame/codec";
import { MSG_CONNECT } from "../frame/types";
import {
  AuthenticationError,
  ConnectionError,
  FitzError,
  RequestQueueFullError,
  TransportError,
} from "../core/errors";
import { Multiplexer } from "./multiplexer";
import {
  attachResilienceMeta,
  classifyFailureKind,
  getResilienceMeta,
  RetryOperation,
  shouldRetryOperation,
} from "./resilience";

export interface ConnectionOptions {
  timeout?: number;
  maxInFlightRequests?: number;
  maxRequestQueueSize?: number;
  reconnect?: {
    enabled?: boolean;
    maxAttempts?: number;
    backoffMs?: number;
    maxBackoffMs?: number;
  };
  retry?: RetryOptions;
  heartbeat?: HeartbeatOptions;
  authSettleDelayMs?: number;
  observability?: FitzObservability;
  asyncHandlers?: AsyncHandlerOptions;
}

export interface ConnectOptions {
  signal?: AbortSignal;
}

type TransportFactory = () => Transport;
type ReconnectListener = () => void | Promise<void>;
type DisconnectListener = () => void;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const sleepWithAbort = async (ms: number, signal?: AbortSignal): Promise<void> => {
  if (signal?.aborted) {
    throw abortError();
  }

  if (ms <= 0) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);

    const onAbort = () => {
      cleanup();
      reject(abortError());
    };

    const cleanup = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };

    signal?.addEventListener("abort", onAbort, { once: true });
  });
};

function createAsyncHandlerDispatcher(
  maxConcurrency: number,
  timeoutMs: number,
  onError: (error: unknown) => void,
) {
  let activeCount = 0;
  let closed = false;
  const queue: Array<() => void> = [];
  const activeTasks = new Set<Promise<void>>();

  const dispatch = (task: () => void | Promise<void>): void => {
    if (closed) {
      return;
    }

    const run = () => {
      if (closed) {
        return;
      }

      activeCount += 1;
      const activeTask = runTask(task).finally(() => {
        activeTasks.delete(activeTask);
        activeCount -= 1;
        flush();
      });
      activeTasks.add(activeTask);
      void activeTask;
    };

    if (activeCount < maxConcurrency) {
      run();
      return;
    }

    queue.push(run);
  };

  const flush = (): void => {
    if (closed) {
      queue.length = 0;
      return;
    }

    if (activeCount >= maxConcurrency) {
      return;
    }

    const next = queue.shift();
    next?.();
  };

  const runTask = async (task: () => void | Promise<void>): Promise<void> => {
    let reported = false;
    const reportOnce = (error: unknown): void => {
      if (reported) {
        return;
      }
      reported = true;
      onError(error);
    };

    const timeoutId = setTimeout(() => {
      reportOnce(new Error(`Async handler timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    try {
      await Promise.resolve().then(task);
    } catch (error) {
      reportOnce(error);
    } finally {
      clearTimeout(timeoutId);
    }
  };

  const close = (): void => {
    closed = true;
    queue.length = 0;
  };

  const drain = async (): Promise<void> => {
    await Promise.allSettled(Array.from(activeTasks));
  };

  return {
    dispatch,
    close,
    drain,
  };
}

function createRequestGate(maxConcurrency: number, maxQueueSize: number) {
  let activeCount = 0;
  let closed = false;
  const queue: Array<{
    resolve: (release: () => void) => void;
    reject: (error: Error) => void;
    signal?: AbortSignal;
    onAbort?: () => void;
  }> = [];

  const acquire = async (signal?: AbortSignal): Promise<() => void> => {
    if (signal?.aborted) {
      throw abortError();
    }

    if (closed) {
      throw connectionClosedError();
    }

    return await new Promise<() => void>((resolve, reject) => {
      const grant = () => {
        if (closed) {
          reject(connectionClosedError());
          return;
        }

        activeCount += 1;
        resolve(() => release());
      };

      const waiter = {
        resolve,
        reject,
        signal,
        onAbort: undefined as (() => void) | undefined,
      };

      const cleanup = () => {
        if (signal && waiter.onAbort) {
          signal.removeEventListener("abort", waiter.onAbort);
        }
      };

      waiter.onAbort = () => {
        removeWaiter(waiter);
        cleanup();
        reject(abortError());
      };

      if (signal) {
        signal.addEventListener("abort", waiter.onAbort, { once: true });
      }

      if (activeCount < maxConcurrency) {
        cleanup();
        grant();
        return;
      }

      if (queue.length >= maxQueueSize) {
        cleanup();
        reject(new RequestQueueFullError());
        return;
      }

      queue.push(waiter);
    });
  };

  const close = (): void => {
    if (closed) {
      return;
    }

    closed = true;
    const error = connectionClosedError();
    for (const waiter of queue.splice(0)) {
      if (waiter.signal && waiter.onAbort) {
        waiter.signal.removeEventListener("abort", waiter.onAbort);
      }
      waiter.reject(error);
    }
  };

  const release = (): void => {
    if (activeCount > 0) {
      activeCount -= 1;
    }

    while (!closed && activeCount < maxConcurrency) {
      const waiter = queue.shift();
      if (!waiter) {
        return;
      }

      if (waiter.signal?.aborted) {
        if (waiter.signal && waiter.onAbort) {
          waiter.signal.removeEventListener("abort", waiter.onAbort);
        }
        continue;
      }

      if (waiter.signal && waiter.onAbort) {
        waiter.signal.removeEventListener("abort", waiter.onAbort);
      }

      activeCount += 1;
      waiter.resolve(() => release());
      return;
    }
  };

  const removeWaiter = (waiter: {
    resolve: (release: () => void) => void;
    reject: (error: Error) => void;
    signal?: AbortSignal;
    onAbort?: () => void;
  }): void => {
    const index = queue.indexOf(waiter);
    if (index >= 0) {
      queue.splice(index, 1);
    }
  };

  return {
    acquire,
    close,
  };
}

function abortError(): Error {
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

function connectionClosedError(): ConnectionError {
  return new ConnectionError("Connection closed", { state: ConnectionState.Closed });
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw abortError();
  }
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

const waitForSharedPromise = async <T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> => {
  if (!signal) {
    return promise;
  }

  if (signal.aborted) {
    throw abortError();
  }

  return await new Promise<T>((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
      signal.removeEventListener("abort", onAbort);
    };

    const settle = (callback: () => void) => {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      callback();
    };

    const onAbort = () => {
      settle(() => reject(abortError()));
    };

    signal.addEventListener("abort", onAbort, { once: true });

    void promise.then(
      (value) => {
        settle(() => resolve(value));
      },
      (error) => {
        settle(() => reject(error));
      },
    );
  });
};

export type Connection = ReturnType<typeof createConnection>;

export function createConnection(
  transportFactory: TransportFactory,
  tokenProvider: TokenProvider,
  options: ConnectionOptions = {},
) {
  const timeout = options.timeout ?? 30000;
  const authSettleDelayMs = options.authSettleDelayMs ?? 100;
  const reconnectEnabled = options.reconnect?.enabled ?? false;
  const reconnectMaxAttempts = options.reconnect?.maxAttempts ?? Infinity;
  const reconnectBackoffMs = options.reconnect?.backoffMs ?? 250;
  const reconnectMaxBackoffMs = options.reconnect?.maxBackoffMs ?? 5000;
  const retryEnabled = options.retry?.enabled ?? true;
  const retryMaxAttempts = options.retry?.maxAttempts ?? 3;
  const retryBackoffMs = options.retry?.backoffMs ?? 100;
  const retryMaxBackoffMs = options.retry?.maxBackoffMs ?? 1000;
  const heartbeatEnabled = options.heartbeat?.enabled ?? true;
  const heartbeatIntervalMs = options.heartbeat?.intervalMs ?? 10000;
  const heartbeatTimeoutMs = options.heartbeat?.timeoutMs ?? 30000;
  const maxInFlightRequests = options.maxInFlightRequests ?? 256;
  const maxRequestQueueSize = options.maxRequestQueueSize ?? 1024;
  const observability = options.observability;

  let transport: Transport | null = null;
  let state: ConnectionState = ConnectionState.Disconnected;
  let requestGate = createRequestGate(maxInFlightRequests, maxRequestQueueSize);
  const frameParser = new FrameParser();
  const reconnectListeners = new Set<ReconnectListener>();
  const disconnectListeners = new Set<DisconnectListener>();
  let writeChain: Promise<void> = Promise.resolve();

  let receiveLoop: Promise<void> | null = null;
  let receiveLoopAbort = false;
  let closeRequested = false;
  let permanentlyClosed = false;
  let connectPromise: Promise<void> | null = null;
  let reconnectPromise: Promise<void> | null = null;
  let connectionLossPromise: Promise<void> | null = null;
  let reconnectRestoreActive = false;
  let authOutcome: Deferred<void> | null = null;
  let authRejected = false;
  let hasEstablishedSession = false;
  let reconnectExhausted = false;
  let readyWaiterCount = 0;
  const closeAbortController = new AbortController();
  const connectionScope = createScope("connection");
  const readyListeners = new Set<() => void>();
  let heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  let heartbeatTransport: Transport | null = null;
  let heartbeatPending = false;
  let lastActivityAt = Date.now();

  const log = (
    level: "debug" | "info" | "warn" | "error",
    event: string,
    fields?: Record<string, unknown>,
  ): void => {
    observability?.logger?.log(level, event, fields);
  };

  const describeError = (error: unknown): string => {
    if (error instanceof Error) {
      return error.message;
    }
    return String(error);
  };

  const describeErrorFields = (error: unknown): Record<string, unknown> => {
    if (error instanceof FitzError) {
      return {
        errorName: error.name,
        code: error.code,
        domainCode: error.domainCode,
      };
    }

    if (error instanceof Error) {
      return {
        errorName: error.name,
        code: (error as Error & { code?: unknown }).code,
        domainCode: (error as Error & { domainCode?: unknown }).domainCode,
      };
    }

    return {
      errorName: typeof error,
      code: undefined,
      domainCode: undefined,
    };
  };

  const describeConnectionLoss = (error: unknown): string => {
    if (error instanceof Error) {
      return error.message;
    }
    return "connection closed during CONNECT";
  };

  const handlePossibleTransportFailure = (error: unknown): void => {
    if (closeRequested) {
      return;
    }

    if (state !== ConnectionState.Authenticated) {
      return;
    }

    const resilienceMeta = getResilienceMeta(error);
    if (resilienceMeta?.boundary === "pre-send") {
      return;
    }

    if (
      error instanceof TransportError ||
      error instanceof ConnectionError ||
      error instanceof AuthenticationError
    ) {
      void handleConnectionLoss(error);
    }
  };

  const multiplexer = new Multiplexer({
    meter: observability?.meter,
    tracer: observability?.tracer,
  });

  const asyncHandlerDispatcher = createAsyncHandlerDispatcher(
    options.asyncHandlers?.maxConcurrency ?? Infinity,
    options.asyncHandlers?.timeoutMs ?? timeout,
    (error) => {
      log("warn", "fitz.connection.handler_failed", {
        error: describeError(error),
      });
    },
  );

  const connect = async (options: ConnectOptions = {}): Promise<void> => {
    if (permanentlyClosed || closeRequested) {
      throw connectionClosedError();
    }

    throwIfAborted(options.signal);

    if (state === ConnectionState.Authenticated) {
      return;
    }

    if (connectPromise) {
      await waitForSharedPromise(connectPromise, options.signal);
      return;
    }

    if (
      reconnectPromise ||
      state === ConnectionState.Connecting ||
      state === ConnectionState.Connected ||
      state === ConnectionState.Authenticating ||
      state === ConnectionState.Reconnecting ||
      (state === ConnectionState.Disconnected && canWaitForReconnect())
    ) {
      await waitForReady(options.signal, timeout);
      return;
    }

    authRejected = false;
    reconnectExhausted = false;

    const sharedConnectPromise = openAndAuthenticate(false, options.signal).finally(() => {
      if (connectPromise === sharedConnectPromise) {
        connectPromise = null;
      }
    });
    connectPromise = sharedConnectPromise;

    await sharedConnectPromise;
  };

  const close = async (): Promise<void> => {
    if (state === ConnectionState.Closed && !transport) {
      await connectionScope.dispose();
      return;
    }

    permanentlyClosed = true;
    closeRequested = true;
    receiveLoopAbort = true;
    stopHeartbeat();
    closeAbortController.abort();
    asyncHandlerDispatcher.close();
    const scopeDisposePromise = connectionScope.dispose();
    setState(ConnectionState.Closed);
    requestGate.close();
    authOutcome?.reject(new ConnectionError("Connection closed", { state }));
    authOutcome = null;
    multiplexer.setDisconnected();
    emitDisconnect();
    emitLifecycleEvent("closed");

    const activeReceiveLoop = receiveLoop;
    receiveLoop = null;
    if (activeReceiveLoop) {
      await Promise.race([activeReceiveLoop.catch(() => undefined), sleep(1000)]);
    }

    if (transport) {
      await transport.close();
      transport = null;
    }

    transport = null;
    await asyncHandlerDispatcher.drain();
    await scopeDisposePromise;
  };

  const waitForRequestReady = async (
    signal?: AbortSignal,
    allowReconnectRestore: boolean = false,
  ): Promise<void> => {
    if (
      allowReconnectRestore &&
      reconnectRestoreActive &&
      state === ConnectionState.Authenticating &&
      !closeRequested &&
      !authRejected &&
      transport
    ) {
      return;
    }

    const releaseReadyWaitSlot = acquireReadyWaitSlot();
    try {
      await waitForReady(signal, timeout);
    } finally {
      releaseReadyWaitSlot?.();
    }
    ensureAuthenticated();
  };

  const requestInternal = async (
    messageType: number,
    requestPayload: Uint8Array,
    signal?: AbortSignal,
    allowReconnectRestore: boolean = false,
  ): Promise<Uint8Array> => {
    let sendStarted = false;
    await waitForRequestReady(signal, allowReconnectRestore);
    const releaseRequestSlot = await requestGate.acquire(signal);
    const startedAt = Date.now();

    try {
      const activeTransport = ensureTransport();
      const frame = FrameCodec.encodeFrame(messageType, requestPayload);

      return await multiplexer.request(
        messageType,
        frame,
        (data) => {
          sendStarted = true;
          return sendSerialized(activeTransport, data);
        },
        timeout,
        signal,
      );
    } catch (error) {
      attachResilienceMeta(error, {
        boundary: sendStarted ? "post-send" : "pre-send",
        failureKind: classifyFailureKind(error),
        explicitNegative: false,
      });
      log("error", "fitz.connection.request_failed", {
        operation: "request",
        state,
        messageType,
        latencyMs: Date.now() - startedAt,
        ...describeErrorFields(error),
        error: describeError(error),
      });
      handlePossibleTransportFailure(error);
      throw error;
    } finally {
      releaseRequestSlot();
    }
  };

  const request = async (
    messageType: number,
    requestPayload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> => {
    return await requestInternal(messageType, requestPayload, signal);
  };

  const requestDuringReconnectRestore = async (
    messageType: number,
    requestPayload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> => {
    return await requestInternal(messageType, requestPayload, signal, true);
  };

  const send = async (
    messageType: number,
    requestPayload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> => {
    let sendStarted = false;
    const releaseReadyWaitSlot = acquireReadyWaitSlot();
    try {
      await waitForReady(signal, timeout);
    } finally {
      releaseReadyWaitSlot?.();
    }
    ensureAuthenticated();
    const releaseRequestSlot = await requestGate.acquire(signal);
    const startedAt = Date.now();

    try {
      const activeTransport = ensureTransport();
      const frame = FrameCodec.encodeFrame(messageType, requestPayload);

      sendStarted = true;
      await sendSerialized(activeTransport, frame);
    } catch (error) {
      attachResilienceMeta(error, {
        boundary: sendStarted ? "post-send" : "pre-send",
        failureKind: classifyFailureKind(error),
        explicitNegative: false,
      });
      log("error", "fitz.connection.send_failed", {
        operation: "send",
        state,
        messageType,
        latencyMs: Date.now() - startedAt,
        ...describeErrorFields(error),
        error: describeError(error),
      });
      handlePossibleTransportFailure(error);
      throw error;
    } finally {
      releaseRequestSlot();
    }
  };

  const sendFireAndForget = async (
    messageType: number,
    requestPayload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> => {
    await send(messageType, requestPayload, signal);
  };

  const registerNotificationHandler = (
    messageType: number,
    handler: (payload: Uint8Array) => void,
  ): void => {
    multiplexer.registerNotificationHandler(messageType, handler);
  };

  const unregisterNotificationHandler = (messageType: number): void => {
    multiplexer.unregisterNotificationHandler(messageType);
  };

  const onReconnect = (listener: ReconnectListener): (() => void) => {
    reconnectListeners.add(listener);
    return () => {
      reconnectListeners.delete(listener);
    };
  };

  const onDisconnect = (listener: DisconnectListener): (() => void) => {
    disconnectListeners.add(listener);
    return () => {
      disconnectListeners.delete(listener);
    };
  };

  const getMultiplexer = (): Multiplexer => multiplexer;

  const getScope = (): Scope => connectionScope;

  const dispatchAsyncHandler = (task: () => void | Promise<void>): void => {
    asyncHandlerDispatcher.dispatch(task);
  };

  const getState = (): ConnectionState => state;

  const isConnected = (): boolean => state === ConnectionState.Authenticated;

  const getUrl = (): string => ensureTransport().getUrl();

  const canWaitForReconnect = (): boolean => {
    return reconnectEnabled && hasEstablishedSession && !reconnectExhausted && !authRejected;
  };

  const readyFailure = (): Error | null => {
    if (state === ConnectionState.Authenticated) {
      return null;
    }

    if (closeRequested || state === ConnectionState.Closed) {
      return connectionClosedError();
    }

    if (authRejected) {
      return new AuthenticationError("Authentication rejected", { state });
    }

    if (
      state === ConnectionState.Connecting ||
      state === ConnectionState.Connected ||
      state === ConnectionState.Authenticating ||
      state === ConnectionState.Reconnecting
    ) {
      return null;
    }

    if (state === ConnectionState.Disconnected && canWaitForReconnect()) {
      return null;
    }

    return new ConnectionError(`Cannot use connection while state is ${state}`, { state });
  };

  const notifyReadyListeners = (): void => {
    for (const listener of readyListeners) {
      listener();
    }
  };

  const acquireReadyWaitSlot = (): (() => void) | null => {
    const failure = readyFailure();
    if (state === ConnectionState.Authenticated || failure) {
      return null;
    }

    if (readyWaiterCount >= maxRequestQueueSize) {
      throw new RequestQueueFullError();
    }

    readyWaiterCount += 1;
    let released = false;

    return () => {
      if (released) {
        return;
      }

      released = true;
      readyWaiterCount = Math.max(readyWaiterCount - 1, 0);
    };
  };

  const waitForReady = async (
    signal?: AbortSignal,
    waitTimeoutMs: number = timeout,
  ): Promise<void> => {
    throwIfAborted(signal);
    const immediateFailure = readyFailure();
    if (!immediateFailure) {
      if (state === ConnectionState.Authenticated) {
        return;
      }
    } else {
      throw immediateFailure;
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      const cleanup = () => {
        readyListeners.delete(onStateChange);
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        signal?.removeEventListener("abort", onAbort);
      };

      const settle = (cb: () => void) => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        cb();
      };

      const onAbort = () => {
        settle(() => reject(abortError()));
      };

      const onStateChange = () => {
        const failure = readyFailure();
        if (state === ConnectionState.Authenticated) {
          settle(resolve);
          return;
        }

        if (failure) {
          settle(() => reject(failure));
        }
      };

      readyListeners.add(onStateChange);
      signal?.addEventListener("abort", onAbort, { once: true });
      timeoutId = setTimeout(() => {
        settle(() =>
          reject(
            new ConnectionError("Timed out waiting for connection to become ready", {
              state,
            }),
          ),
        );
      }, waitTimeoutMs);

      onStateChange();
    });
  };

  const waitUntilReady = async (
    signal?: AbortSignal,
    waitTimeoutMs: number = timeout,
  ): Promise<void> => {
    await waitForReady(signal, waitTimeoutMs);
  };

  const shouldWaitForReconnect = (): boolean => {
    return (
      reconnectPromise !== null ||
      state === ConnectionState.Reconnecting ||
      (state === ConnectionState.Disconnected && canWaitForReconnect())
    );
  };

  const getRetryDelayMs = (baseDelayMs: number): number => {
    const jitter = Math.floor(Math.random() * baseDelayMs * 0.5);
    return Math.min(Math.max(baseDelayMs + jitter, 1), retryMaxBackoffMs);
  };

  const recordRetry = (
    operation: RetryOperation,
    attempt: number,
    delayMs: number,
    error: unknown,
  ): void => {
    const meta = getResilienceMeta(error);
    const fields = {
      domain: operation.domain,
      operation: operation.operation,
      attempt,
      delayMs,
      boundary: meta?.boundary ?? "unknown",
      error: describeError(error),
      ...describeErrorFields(error),
    };

    log("warn", "fitz.request.retry", fields);
    observability?.meter?.counter("fitz.request.retry", 1, {
      domain: operation.domain,
      operation: operation.operation,
      boundary: meta?.boundary ?? "unknown",
    });
  };

  const recordRetryExhausted = (
    operation: RetryOperation,
    attempt: number,
    error: unknown,
  ): void => {
    const meta = getResilienceMeta(error);
    const fields = {
      domain: operation.domain,
      operation: operation.operation,
      attempt,
      boundary: meta?.boundary ?? "unknown",
      error: describeError(error),
      ...describeErrorFields(error),
    };

    log("warn", "fitz.request.retry_exhausted", fields);
    observability?.meter?.counter("fitz.request.retry_exhausted", 1, {
      domain: operation.domain,
      operation: operation.operation,
      boundary: meta?.boundary ?? "unknown",
    });
  };

  const executeWithRetry = async <T>(
    operation: RetryOperation,
    task: () => Promise<T>,
  ): Promise<T> => {
    if (!retryEnabled || operation.retryClass === "wait_only") {
      return task();
    }

    let attempt = 0;
    let delayMs = retryBackoffMs;

    while (true) {
      attempt += 1;
      try {
        return await task();
      } catch (error) {
        if (isAbortError(error)) {
          throw error;
        }

        const retryable = shouldRetryOperation(operation.retryClass, error);
        if (!retryable) {
          throw error;
        }

        if (attempt >= retryMaxAttempts) {
          recordRetryExhausted(operation, attempt, error);
          throw error;
        }

        const actualDelayMs = getRetryDelayMs(delayMs);
        recordRetry(operation, attempt, actualDelayMs, error);
        await sleepWithAbort(actualDelayMs, operation.signal);
        delayMs = Math.min(delayMs * 2, retryMaxBackoffMs);
      }
    }
  };

  const withWriteLock = async <T>(operation: () => Promise<T>): Promise<T> => {
    const prior = writeChain;
    let release!: () => void;
    writeChain = new Promise<void>((resolve) => {
      release = resolve;
    });

    await prior;
    try {
      return await operation();
    } finally {
      release();
    }
  };

  const markOutboundActivity = (): void => {
    lastActivityAt = Date.now();
  };

  const markRemoteActivity = (): void => {
    lastActivityAt = Date.now();
  };

  const stopHeartbeat = (): void => {
    if (heartbeatTimer) {
      clearTimeout(heartbeatTimer);
      heartbeatTimer = null;
    }
    heartbeatTransport = null;
    heartbeatPending = false;
  };

  const startHeartbeat = (activeTransport: Transport): void => {
    if (!heartbeatEnabled) {
      return;
    }

    stopHeartbeat();
    activeTransport.enableKeepAlive?.(heartbeatIntervalMs);
    heartbeatTransport = activeTransport;
    markRemoteActivity();

    const scheduleNext = (): void => {
      if (closeRequested || receiveLoopAbort || heartbeatTransport !== activeTransport) {
        return;
      }

      heartbeatTimer = setTimeout(tick, heartbeatIntervalMs);
    };

    const tick = (): void => {
      heartbeatTimer = null;
      if (closeRequested || receiveLoopAbort || heartbeatTransport !== activeTransport) {
        return;
      }

      const now = Date.now();
      if (now - lastActivityAt < heartbeatIntervalMs) {
        scheduleNext();
        return;
      }

      const supportsHeartbeat = activeTransport.supportsHeartbeat?.() ?? true;
      if (!heartbeatPending && supportsHeartbeat && activeTransport.sendHeartbeat) {
        heartbeatPending = true;
        const heartbeatSentAt = now;
        const dispatchHeartbeat = (heartbeat: { timeoutMs: number }) =>
          activeTransport.sendHeartbeat!(heartbeat);

        void withWriteLock(async () => {
          await dispatchHeartbeat({ timeoutMs: heartbeatTimeoutMs });
        })
          .then(() => {
            if (heartbeatTransport !== activeTransport) {
              return;
            }

            heartbeatPending = false;
            markRemoteActivity();
          })
          .catch((error: unknown) => {
            if (heartbeatTransport !== activeTransport) {
              return;
            }

            heartbeatPending = false;
            if (lastActivityAt > heartbeatSentAt) {
              return;
            }

            const heartbeatError = new TransportError(`Heartbeat failed: ${describeError(error)}`);
            void activeTransport.close().catch(() => undefined);
            void handleConnectionLoss(heartbeatError);
          });
      }

      scheduleNext();
    };

    scheduleNext();
  };

  const openAndAuthenticate = async (isReconnect: boolean, signal?: AbortSignal): Promise<void> => {
    throwIfAborted(signal);
    receiveLoopAbort = false;
    frameParser.parseFrames(new Uint8Array(0));
    requestGate = createRequestGate(maxInFlightRequests, maxRequestQueueSize);
    const activeTransport = transportFactory();
    transport = activeTransport;
    stopHeartbeat();
    setState(isReconnect ? ConnectionState.Reconnecting : ConnectionState.Connecting);
    emitLifecycleEvent(isReconnect ? "reconnect_start" : "connect_start");

    await activeTransport.connect();
    markRemoteActivity();
    if (closeRequested) {
      stopHeartbeat();
      await activeTransport.close().catch(() => undefined);
      if (transport === activeTransport) {
        transport = null;
      }
      throw connectionClosedError();
    }
    throwIfAborted(signal);
    receiveLoop = startReceiveLoop();

    setState(ConnectionState.Connected);
    setState(ConnectionState.Authenticating);
    emitLifecycleEvent("auth_start");
    authOutcome = new Deferred<void>();

    try {
      await sendConnect();
      if (closeRequested) {
        throw connectionClosedError();
      }
      throwIfAborted(signal);
      await Promise.race([authOutcome.promise, sleep(authSettleDelayMs)]);
      if (closeRequested) {
        throw connectionClosedError();
      }
      throwIfAborted(signal);
      authOutcome?.resolve();
      authOutcome = null;
      if (isReconnect) {
        multiplexer.setConnected();
        reconnectRestoreActive = true;
        try {
          await restoreReconnectState();
          if (closeRequested) {
            throw connectionClosedError();
          }
        } finally {
          reconnectRestoreActive = false;
        }
      }
      hasEstablishedSession = true;
      reconnectExhausted = false;
      setState(ConnectionState.Authenticated);
      startHeartbeat(activeTransport);
      if (!isReconnect) {
        multiplexer.setConnected();
      }
      emitLifecycleEvent(isReconnect ? "reconnect_succeeded" : "connect_succeeded");
    } catch (error) {
      authOutcome = null;
      reconnectRestoreActive = false;
      multiplexer.setDisconnected();
      emitDisconnect();
      stopHeartbeat();
      if (activeTransport) {
        await activeTransport.close().catch(() => undefined);
      }
      if (transport === activeTransport) {
        transport = null;
      }
      const rejectedAuth = error instanceof AuthenticationError;
      authRejected = rejectedAuth;
      if (closeRequested) {
        setState(ConnectionState.Closed);
      } else {
        setState(rejectedAuth ? ConnectionState.Closed : ConnectionState.Disconnected);
      }
      emitLifecycleEvent(isReconnect ? "reconnect_failed" : "connect_failed", error);
      if (isAbortError(error)) {
        throw abortError();
      }
      throw error;
    }
  };

  const sendConnect = async (): Promise<void> => {
    const token = await tokenProvider();
    const frame = FrameCodec.encodeFrame(MSG_CONNECT, utf8Encoder.encode(token));
    await ensureTransport().send(frame);
    markOutboundActivity();
  };

  const startReceiveLoop = async (): Promise<void> => {
    while (!receiveLoopAbort && !closeRequested) {
      try {
        const activeTransport = ensureTransport();
        const data = await activeTransport.receive();
        markRemoteActivity();
        const frames = frameParser.parseFrames(data);

        for (const frame of frames) {
          multiplexer.dispatch(frame.messageType, frame.payload);
        }
      } catch (error) {
        if (receiveLoopAbort || closeRequested) {
          return;
        }

        await handleConnectionLoss(error);
        return;
      }
    }
  };

  const handleConnectionLoss = async (error: unknown): Promise<void> => {
    if (connectionLossPromise) {
      await connectionLossPromise;
      return;
    }

    connectionLossPromise = handleConnectionLossOnce(error).finally(() => {
      connectionLossPromise = null;
    });
    await connectionLossPromise;
  };

  const handleConnectionLossOnce = async (error: unknown): Promise<void> => {
    stopHeartbeat();
    multiplexer.setDisconnected();
    requestGate.close();
    emitDisconnect();
    log("warn", "fitz.connection.lost", {
      error: describeError(error),
      state,
    });

    if (state === ConnectionState.Authenticating && authOutcome) {
      authRejected = true;
      authOutcome.reject(
        new AuthenticationError(describeConnectionLoss(error), {
          state,
        }),
      );
    }

    if (closeRequested) {
      setState(ConnectionState.Closed);
      return;
    }

    if (authRejected) {
      setState(ConnectionState.Closed);
      emitLifecycleEvent("auth_rejected", error);
      return;
    }

    reconnectExhausted = false;
    setState(ConnectionState.Disconnected);
    emitLifecycleEvent("connection_lost", error);

    if (!reconnectEnabled) {
      return;
    }

    if (!reconnectPromise) {
      reconnectPromise = reconnectLoop().finally(() => {
        reconnectPromise = null;
      });
    }

    await reconnectPromise;
  };

  const getReconnectDelayMs = (baseDelayMs: number): number => {
    const jitter = Math.floor(Math.random() * baseDelayMs * 0.5);
    return Math.min(Math.max(baseDelayMs + jitter, 1), reconnectMaxBackoffMs);
  };

  const reconnectLoop = async (): Promise<void> => {
    let attempts = 0;
    let delayMs = reconnectBackoffMs;

    while (!closeRequested && attempts < reconnectMaxAttempts) {
      attempts += 1;
      setState(ConnectionState.Reconnecting);
      emitLifecycleEvent("reconnect_scheduled", undefined, attempts);

      const actualDelayMs = getReconnectDelayMs(delayMs);
      try {
        await sleepWithAbort(actualDelayMs, closeAbortController.signal);
        if (closeRequested) {
          return;
        }
        await openAndAuthenticate(true);
        return;
      } catch (error) {
        if (closeRequested) {
          return;
        }
        if (isAbortError(error)) {
          return;
        }
        log("warn", "fitz.connection.reconnect_retry", {
          attempts,
          delayMs: actualDelayMs,
          baseDelayMs: delayMs,
          error: describeError(error),
        });
        delayMs = Math.min(delayMs * 2, reconnectMaxBackoffMs);
      }
    }

    if (closeRequested) {
      setState(ConnectionState.Closed);
      return;
    }

    reconnectExhausted = true;
    setState(ConnectionState.Disconnected);
    emitLifecycleEvent("reconnect_exhausted", undefined, attempts);
  };

  const restoreReconnectState = async (): Promise<void> => {
    for (const listener of reconnectListeners) {
      try {
        await listener();
      } catch (error) {
        log("warn", "fitz.connection.reconnect_restore_failed", {
          error: describeError(error),
        });
        emitLifecycleEvent("reconnect_restore_failed", error);
      }
    }
  };

  const ensureTransport = (): Transport => {
    if (!transport) {
      throw new ConnectionError("No active transport", { state });
    }
    return transport;
  };

  const ensureAuthenticated = (): void => {
    if (closeRequested || state !== ConnectionState.Authenticated) {
      throw new ConnectionError(`Cannot use connection while state is ${state}`, {
        state,
      });
    }
  };

  const setState = (newState: ConnectionState): void => {
    state = newState;
    notifyReadyListeners();
  };

  const sendSerialized = async (transport: Transport, data: Uint8Array): Promise<void> => {
    await withWriteLock(async () => {
      await transport.send(data);
      markOutboundActivity();
    });
  };

  const emitLifecycleEvent = (event: string, error?: unknown, attempt?: number): void => {
    const payload = {
      event,
      state,
      transport: transport?.constructor.name,
      url: transport?.getUrl(),
      attempt,
      error: error ? describeError(error) : undefined,
    };

    observability?.onLifecycleEvent?.(payload);
    log("info", `fitz.connection.${event}`, payload);
    observability?.meter?.counter("fitz.connection.lifecycle", 1, {
      event,
      state,
    });
  };

  const emitDisconnect = (): void => {
    for (const listener of disconnectListeners) {
      try {
        listener();
      } catch {
        // Best-effort disconnect fanout.
      }
    }
  };

  return {
    connect,
    close,
    request,
    requestDuringReconnectRestore,
    send,
    sendFireAndForget,
    registerNotificationHandler,
    unregisterNotificationHandler,
    onReconnect,
    onDisconnect,
    getMultiplexer,
    dispatchAsyncHandler,
    executeWithRetry,
    waitUntilReady,
    shouldWaitForReconnect,
    getScope,
    getState,
    isConnected,
    getUrl,
  };
}

interface ConnectionConstructor {
  new (
    transportFactory: TransportFactory,
    tokenProvider: TokenProvider,
    options?: ConnectionOptions,
  ): Connection;
  (
    transportFactory: TransportFactory,
    tokenProvider: TokenProvider,
    options?: ConnectionOptions,
  ): Connection;
  prototype: any;
}

export const Connection: ConnectionConstructor = function (
  transportFactory: TransportFactory,
  tokenProvider: TokenProvider,
  options: ConnectionOptions = {},
) {
  const connection = createConnection(transportFactory, tokenProvider, options);
  Object.setPrototypeOf(connection, Connection.prototype);
  return connection;
} as unknown as ConnectionConstructor;
