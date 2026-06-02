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
  TokenProvider,
} from "../core/types";
import { utf8Encoder } from "../core/buffer";
import { FrameCodec, FrameParser } from "../frame/codec";
import { MSG_CONNECT } from "../frame/types";
import { AuthenticationError, ConnectionError, FitzError, TransportError } from "../core/errors";
import { Multiplexer } from "./multiplexer";

export interface ConnectionOptions {
  timeout?: number;
  maxInFlightRequests?: number;
  reconnect?: {
    enabled?: boolean;
    maxAttempts?: number;
    backoffMs?: number;
    maxBackoffMs?: number;
  };
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

function createAsyncHandlerDispatcher(
  maxConcurrency: number,
  timeoutMs: number,
  onError: (error: unknown) => void,
) {
  let activeCount = 0;
  const queue: Array<() => void> = [];

  const dispatch = (task: () => void | Promise<void>): void => {
    const run = () => {
      activeCount += 1;
      void runTask(task).finally(() => {
        activeCount -= 1;
        flush();
      });
    };

    if (activeCount < maxConcurrency) {
      run();
      return;
    }

    queue.push(run);
  };

  const flush = (): void => {
    if (activeCount >= maxConcurrency) {
      return;
    }

    const next = queue.shift();
    next?.();
  };

  const runTask = async (task: () => void | Promise<void>): Promise<void> => {
    try {
      await Promise.race([
        Promise.resolve().then(task),
        sleep(timeoutMs).then(() => {
          throw new Error(`Async handler timeout after ${timeoutMs}ms`);
        }),
      ]);
    } catch (error) {
      onError(error);
    }
  };

  return {
    dispatch,
  };
}

function createRequestGate(maxConcurrency: number) {
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
  const maxInFlightRequests = options.maxInFlightRequests ?? 256;
  const observability = options.observability;

  let transport: Transport | null = null;
  let state: ConnectionState = ConnectionState.Disconnected;
  let requestGate = createRequestGate(maxInFlightRequests);
  const frameParser = new FrameParser();
  const reconnectListeners = new Set<ReconnectListener>();
  const disconnectListeners = new Set<DisconnectListener>();
  let writeChain: Promise<void> = Promise.resolve();

  let receiveLoop: Promise<void> | null = null;
  let receiveLoopAbort = false;
  let closeRequested = false;
  let reconnectPromise: Promise<void> | null = null;
  let authOutcome: Deferred<void> | null = null;
  let authRejected = false;

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
    closeRequested = false;
    authRejected = false;
    await openAndAuthenticate(false, options.signal);
  };

  const close = async (): Promise<void> => {
    if (state === ConnectionState.Closed && !transport) {
      return;
    }

    closeRequested = true;
    receiveLoopAbort = true;
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
  };

  const request = async (
    messageType: number,
    requestPayload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> => {
    ensureAuthenticated();
    const releaseRequestSlot = await requestGate.acquire(signal);
    const startedAt = Date.now();

    try {
      const activeTransport = ensureTransport();
      const frame = FrameCodec.encodeFrame(messageType, requestPayload);

      return await multiplexer.request(
        messageType,
        frame,
        (data) => sendSerialized(activeTransport, data),
        timeout,
        signal,
      );
    } catch (error) {
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

  const send = async (messageType: number, requestPayload: Uint8Array): Promise<void> => {
    ensureAuthenticated();
    const releaseRequestSlot = await requestGate.acquire();
    const startedAt = Date.now();

    try {
      const activeTransport = ensureTransport();
      const frame = FrameCodec.encodeFrame(messageType, requestPayload);

      await sendSerialized(activeTransport, frame);
    } catch (error) {
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
  ): Promise<void> => {
    await send(messageType, requestPayload);
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

  const dispatchAsyncHandler = (task: () => void | Promise<void>): void => {
    asyncHandlerDispatcher.dispatch(task);
  };

  const getState = (): ConnectionState => state;

  const isConnected = (): boolean => state === ConnectionState.Authenticated;

  const getUrl = (): string => ensureTransport().getUrl();

  const openAndAuthenticate = async (isReconnect: boolean, signal?: AbortSignal): Promise<void> => {
    throwIfAborted(signal);
    receiveLoopAbort = false;
    frameParser.parseFrames(new Uint8Array(0));
    requestGate = createRequestGate(maxInFlightRequests);
    const activeTransport = transportFactory();
    transport = activeTransport;
    setState(isReconnect ? ConnectionState.Reconnecting : ConnectionState.Connecting);
    emitLifecycleEvent(isReconnect ? "reconnect_start" : "connect_start");

    await activeTransport.connect();
    if (closeRequested) {
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
        await restoreReconnectState();
        if (closeRequested) {
          throw connectionClosedError();
        }
      }
      setState(ConnectionState.Authenticated);
      multiplexer.setConnected();
      emitLifecycleEvent(isReconnect ? "reconnect_succeeded" : "connect_succeeded");
    } catch (error) {
      authOutcome = null;
      multiplexer.setDisconnected();
      emitDisconnect();
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
  };

  const startReceiveLoop = async (): Promise<void> => {
    while (!receiveLoopAbort && !closeRequested) {
      try {
        const activeTransport = ensureTransport();
        const data = await activeTransport.receive();
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

  const reconnectLoop = async (): Promise<void> => {
    let attempts = 0;
    let delayMs = reconnectBackoffMs;

    while (!closeRequested && attempts < reconnectMaxAttempts) {
      attempts += 1;
      setState(ConnectionState.Reconnecting);
      emitLifecycleEvent("reconnect_scheduled", undefined, attempts);

      try {
        await sleep(delayMs);
        if (closeRequested) {
          return;
        }
        await openAndAuthenticate(true);
        return;
      } catch (error) {
        if (closeRequested) {
          return;
        }
        log("warn", "fitz.connection.reconnect_retry", {
          attempts,
          delayMs,
          error: describeError(error),
        });
        delayMs = Math.min(delayMs * 2, reconnectMaxBackoffMs);
      }
    }

    if (closeRequested) {
      setState(ConnectionState.Closed);
      return;
    }

    setState(ConnectionState.Disconnected);
    emitLifecycleEvent("reconnect_exhausted", undefined, attempts);
  };

  const restoreReconnectState = async (): Promise<void> => {
    for (const listener of reconnectListeners) {
      await listener();
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
  };

  const sendSerialized = async (transport: Transport, data: Uint8Array): Promise<void> => {
    const prior = writeChain;
    let release!: () => void;
    writeChain = new Promise<void>((resolve) => {
      release = resolve;
    });

    await prior;
    try {
      await transport.send(data);
    } finally {
      release();
    }
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
    send,
    sendFireAndForget,
    registerNotificationHandler,
    unregisterNotificationHandler,
    onReconnect,
    onDisconnect,
    getMultiplexer,
    dispatchAsyncHandler,
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
