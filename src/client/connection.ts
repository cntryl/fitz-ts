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
import { FrameCodec, FrameParser } from "../frame/codec";
import { MSG_CONNECT } from "../frame/types";
import {
  AuthenticationError,
  ConnectionError,
  FitzError,
  TransportError,
} from "../core/errors";
import { Multiplexer } from "./multiplexer";

export interface ConnectionOptions {
  timeout?: number;
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

const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

class AsyncHandlerDispatcher {
  private activeCount = 0;
  private readonly queue: Array<() => void> = [];

  constructor(
    private readonly maxConcurrency: number,
    private readonly timeoutMs: number,
    private readonly onError: (error: unknown) => void,
  ) {}

  dispatch(task: () => void | Promise<void>): void {
    const run = () => {
      this.activeCount += 1;
      void this.runTask(task).finally(() => {
        this.activeCount -= 1;
        this.flush();
      });
    };

    if (this.activeCount < this.maxConcurrency) {
      run();
      return;
    }

    this.queue.push(run);
  }

  private flush(): void {
    if (this.activeCount >= this.maxConcurrency) {
      return;
    }

    const next = this.queue.shift();
    next?.();
  }

  private async runTask(task: () => void | Promise<void>): Promise<void> {
    try {
      await Promise.race([
        Promise.resolve().then(task),
        sleep(this.timeoutMs).then(() => {
          throw new Error(`Async handler timeout after ${this.timeoutMs}ms`);
        }),
      ]);
    } catch (error) {
      this.onError(error);
    }
  }
}

function abortError(): Error {
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw abortError();
  }
}

export class Connection {
  private readonly transportFactory: TransportFactory;
  private transport: Transport | null = null;
  private state: ConnectionState = ConnectionState.Disconnected;
  private readonly tokenProvider: TokenProvider;
  private readonly timeout: number;
  private readonly authSettleDelayMs: number;
  private readonly reconnectEnabled: boolean;
  private readonly reconnectMaxAttempts: number;
  private readonly reconnectBackoffMs: number;
  private readonly reconnectMaxBackoffMs: number;
  private readonly observability?: FitzObservability;
  private readonly multiplexer: Multiplexer;
  private readonly asyncHandlerDispatcher: AsyncHandlerDispatcher;
  private readonly frameParser = new FrameParser();
  private readonly reconnectListeners = new Set<ReconnectListener>();
  private readonly disconnectListeners = new Set<DisconnectListener>();
  private writeChain: Promise<void> = Promise.resolve();

  private receiveLoop: Promise<void> | null = null;
  private receiveLoopAbort = false;
  private closeRequested = false;
  private reconnectPromise: Promise<void> | null = null;
  private authOutcome: Deferred<void> | null = null;
  private authRejected = false;

  constructor(
    transportFactory: TransportFactory,
    tokenProvider: TokenProvider,
    options: ConnectionOptions = {},
  ) {
    this.transportFactory = transportFactory;
    this.tokenProvider = tokenProvider;
    this.timeout = options.timeout ?? 30000;
    this.authSettleDelayMs = options.authSettleDelayMs ?? 100;
    this.reconnectEnabled = options.reconnect?.enabled ?? false;
    this.reconnectMaxAttempts = options.reconnect?.maxAttempts ?? Infinity;
    this.reconnectBackoffMs = options.reconnect?.backoffMs ?? 250;
    this.reconnectMaxBackoffMs = options.reconnect?.maxBackoffMs ?? 5000;
    this.observability = options.observability;
    this.asyncHandlerDispatcher = new AsyncHandlerDispatcher(
      options.asyncHandlers?.maxConcurrency ?? Infinity,
      options.asyncHandlers?.timeoutMs ?? this.timeout,
      (error) => {
        this.log("warn", "fitz.connection.handler_failed", {
          error: this.describeError(error),
        });
      },
    );
    this.multiplexer = new Multiplexer({
      meter: this.observability?.meter,
      tracer: this.observability?.tracer,
    });
  }

  async connect(options: ConnectOptions = {}): Promise<void> {
    this.closeRequested = false;
    this.authRejected = false;
    await this.openAndAuthenticate(false, options.signal);
  }

  async close(): Promise<void> {
    if (this.state === ConnectionState.Closed && !this.transport) {
      return;
    }

    this.closeRequested = true;
    this.receiveLoopAbort = true;
    this.setState(ConnectionState.Closed);
    this.authOutcome?.reject(
      new ConnectionError("Connection closed", { state: this.state }),
    );
    this.authOutcome = null;
    this.multiplexer.setDisconnected();
    this.emitDisconnect();
    this.emitLifecycleEvent("closed");

    const receiveLoop = this.receiveLoop;
    this.receiveLoop = null;
    if (receiveLoop) {
      await Promise.race([receiveLoop.catch(() => undefined), sleep(1000)]);
    }

    if (this.transport) {
      await this.transport.close();
      this.transport = null;
    }

    this.transport = null;
  }

  async request(
    messageType: number,
    requestPayload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    this.ensureAuthenticated();
    const transport = this.ensureTransport();
    const frame = FrameCodec.encodeFrame(messageType, requestPayload);
    const startedAt = Date.now();

    try {
      return await this.multiplexer.request(
        messageType,
        frame,
        (data) => this.sendSerialized(transport, data),
        this.timeout,
        signal,
      );
    } catch (error) {
      this.log("error", "fitz.connection.request_failed", {
        operation: "request",
        state: this.state,
        messageType,
        latencyMs: Date.now() - startedAt,
        ...this.describeErrorFields(error),
        error: this.describeError(error),
      });
      this.handlePossibleTransportFailure(error);
      throw error;
    }
  }

  async send(messageType: number, requestPayload: Uint8Array): Promise<void> {
    this.ensureAuthenticated();
    const transport = this.ensureTransport();
    const frame = FrameCodec.encodeFrame(messageType, requestPayload);
    const startedAt = Date.now();

    try {
      await this.sendSerialized(transport, frame);
    } catch (error) {
      this.log("error", "fitz.connection.send_failed", {
        operation: "send",
        state: this.state,
        messageType,
        latencyMs: Date.now() - startedAt,
        ...this.describeErrorFields(error),
        error: this.describeError(error),
      });
      this.handlePossibleTransportFailure(error);
      throw error;
    }
  }

  async sendFireAndForget(
    messageType: number,
    requestPayload: Uint8Array,
  ): Promise<void> {
    await this.send(messageType, requestPayload);
  }

  registerNotificationHandler(
    messageType: number,
    handler: (payload: Uint8Array) => void,
  ): void {
    this.multiplexer.registerNotificationHandler(messageType, handler);
  }

  unregisterNotificationHandler(messageType: number): void {
    this.multiplexer.unregisterNotificationHandler(messageType);
  }

  onReconnect(listener: ReconnectListener): () => void {
    this.reconnectListeners.add(listener);
    return () => {
      this.reconnectListeners.delete(listener);
    };
  }

  onDisconnect(listener: DisconnectListener): () => void {
    this.disconnectListeners.add(listener);
    return () => {
      this.disconnectListeners.delete(listener);
    };
  }

  getMultiplexer(): Multiplexer {
    return this.multiplexer;
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    this.asyncHandlerDispatcher.dispatch(task);
  }

  getState(): ConnectionState {
    return this.state;
  }

  isConnected(): boolean {
    return this.state === ConnectionState.Authenticated;
  }

  getUrl(): string {
    return this.ensureTransport().getUrl();
  }

  private async openAndAuthenticate(
    isReconnect: boolean,
    signal?: AbortSignal,
  ): Promise<void> {
    throwIfAborted(signal);
    this.receiveLoopAbort = false;
    this.frameParser.parseFrames(new Uint8Array(0));
    this.transport = this.transportFactory();
    this.setState(
      isReconnect ? ConnectionState.Reconnecting : ConnectionState.Connecting,
    );
    this.emitLifecycleEvent(isReconnect ? "reconnect_start" : "connect_start");

    await this.transport.connect();
    throwIfAborted(signal);
    this.receiveLoop = this.startReceiveLoop();

    this.setState(ConnectionState.Connected);
    this.setState(ConnectionState.Authenticating);
    this.emitLifecycleEvent("auth_start");
    this.authOutcome = new Deferred<void>();

    try {
      await this.sendConnect();
      throwIfAborted(signal);
      await Promise.race([
        this.authOutcome.promise,
        sleep(this.authSettleDelayMs),
      ]);
      throwIfAborted(signal);
      this.authOutcome?.resolve();
      this.authOutcome = null;
      if (isReconnect) {
        await this.restoreReconnectState();
      }
      this.setState(ConnectionState.Authenticated);
      this.multiplexer.setConnected();
      this.emitLifecycleEvent(
        isReconnect ? "reconnect_succeeded" : "connect_succeeded",
      );
    } catch (error) {
      this.authOutcome = null;
      this.multiplexer.setDisconnected();
      this.emitDisconnect();
      if (this.transport) {
        await this.transport.close().catch(() => undefined);
        this.transport = null;
      }
      const rejectedAuth =
        error instanceof AuthenticationError ||
        (this.state === ConnectionState.Authenticating && !isReconnect);
      this.authRejected = rejectedAuth;
      this.setState(
        rejectedAuth ? ConnectionState.Closed : ConnectionState.Disconnected,
      );
      this.emitLifecycleEvent(
        isReconnect ? "reconnect_failed" : "connect_failed",
        error,
      );
      throw error;
    }
  }

  private async sendConnect(): Promise<void> {
    const token = await this.tokenProvider();
    const frame = FrameCodec.encodeFrame(
      MSG_CONNECT,
      new TextEncoder().encode(token),
    );
    await this.ensureTransport().send(frame);
  }

  private async startReceiveLoop(): Promise<void> {
    while (!this.receiveLoopAbort && !this.closeRequested) {
      try {
        const transport = this.ensureTransport();
        const data = await transport.receive();
        const frames = this.frameParser.parseFrames(data);

        for (const frame of frames) {
          this.multiplexer.dispatch(frame.messageType, frame.payload);
        }
      } catch (error) {
        if (this.receiveLoopAbort || this.closeRequested) {
          return;
        }

        await this.handleConnectionLoss(error);
        return;
      }
    }
  }

  private async handleConnectionLoss(error: unknown): Promise<void> {
    this.multiplexer.setDisconnected();
    this.emitDisconnect();
    this.log("warn", "fitz.connection.lost", {
      error: this.describeError(error),
      state: this.state,
    });

    if (this.state === ConnectionState.Authenticating && this.authOutcome) {
      this.authRejected = true;
      this.authOutcome.reject(
        new AuthenticationError(this.describeConnectionLoss(error), {
          state: this.state,
        }),
      );
    }

    if (this.closeRequested) {
      this.setState(ConnectionState.Closed);
      return;
    }

    if (this.authRejected) {
      this.setState(ConnectionState.Closed);
      this.emitLifecycleEvent("auth_rejected", error);
      return;
    }

    this.setState(ConnectionState.Disconnected);
    this.emitLifecycleEvent("connection_lost", error);

    if (!this.reconnectEnabled) {
      return;
    }

    if (!this.reconnectPromise) {
      this.reconnectPromise = this.reconnectLoop().finally(() => {
        this.reconnectPromise = null;
      });
    }

    await this.reconnectPromise;
  }

  private async reconnectLoop(): Promise<void> {
    let attempts = 0;
    let delayMs = this.reconnectBackoffMs;

    while (!this.closeRequested && attempts < this.reconnectMaxAttempts) {
      attempts += 1;
      this.setState(ConnectionState.Reconnecting);
      this.emitLifecycleEvent("reconnect_scheduled", undefined, attempts);

      try {
        await sleep(delayMs);
        await this.openAndAuthenticate(true);
        return;
      } catch (error) {
        this.log("warn", "fitz.connection.reconnect_retry", {
          attempts,
          delayMs,
          error: this.describeError(error),
        });
        delayMs = Math.min(delayMs * 2, this.reconnectMaxBackoffMs);
      }
    }

    this.setState(ConnectionState.Disconnected);
    this.emitLifecycleEvent("reconnect_exhausted", undefined, attempts);
  }

  private async restoreReconnectState(): Promise<void> {
    for (const listener of this.reconnectListeners) {
      await listener();
    }
  }

  private ensureTransport(): Transport {
    if (!this.transport) {
      throw new ConnectionError("No active transport", { state: this.state });
    }
    return this.transport;
  }

  private ensureAuthenticated(): void {
    if (this.closeRequested || this.state !== ConnectionState.Authenticated) {
      throw new ConnectionError(
        `Cannot use connection while state is ${this.state}`,
        { state: this.state },
      );
    }
  }

  private setState(newState: ConnectionState): void {
    this.state = newState;
  }

  private async sendSerialized(
    transport: Transport,
    data: Uint8Array,
  ): Promise<void> {
    const prior = this.writeChain;
    let release!: () => void;
    this.writeChain = new Promise<void>((resolve) => {
      release = resolve;
    });

    await prior;
    try {
      await transport.send(data);
    } finally {
      release();
    }
  }

  private emitLifecycleEvent(
    event: string,
    error?: unknown,
    attempt?: number,
  ): void {
    const payload = {
      event,
      state: this.state,
      transport: this.transport?.constructor.name,
      url: this.transport?.getUrl(),
      attempt,
      error: error ? this.describeError(error) : undefined,
    };

    this.observability?.onLifecycleEvent?.(payload);
    this.log("info", `fitz.connection.${event}`, payload);
    this.observability?.meter?.counter("fitz.connection.lifecycle", 1, {
      event,
      state: this.state,
    });
  }

  private emitDisconnect(): void {
    for (const listener of this.disconnectListeners) {
      try {
        listener();
      } catch {
        // Best-effort disconnect fanout.
      }
    }
  }

  private log(
    level: "debug" | "info" | "warn" | "error",
    event: string,
    fields?: Record<string, unknown>,
  ): void {
    this.observability?.logger?.log(level, event, fields);
  }

  private describeError(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }
    return String(error);
  }

  private describeErrorFields(error: unknown): Record<string, unknown> {
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
  }

  private handlePossibleTransportFailure(error: unknown): void {
    if (this.closeRequested) {
      return;
    }

    if (
      error instanceof TransportError ||
      error instanceof ConnectionError ||
      error instanceof AuthenticationError
    ) {
      void this.handleConnectionLoss(error);
    }
  }

  private describeConnectionLoss(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }
    return "connection closed during CONNECT";
  }
}
