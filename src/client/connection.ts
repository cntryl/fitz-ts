/**
 * Connection manager for Fitz protocol.
 *
 * CONNECT is a silent-success handshake. A newly opened transport is treated as
 * authenticated only if it remains open for a short settle window after the
 * CONNECT frame is sent.
 */

import { Transport } from "../transport/types";
import { ConnectionState, Deferred, TokenProvider } from "../core/types";
import { FrameCodec, FrameParser } from "../frame/codec";
import { MSG_CONNECT } from "../frame/types";
import {
  AuthenticationError,
  ConnectionError,
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
}

export interface ConnectOptions {
  signal?: AbortSignal;
}

type TransportFactory = () => Transport;
type ReconnectListener = () => void | Promise<void>;

const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

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
  private readonly multiplexer = new Multiplexer();
  private readonly frameParser = new FrameParser();
  private readonly reconnectListeners = new Set<ReconnectListener>();

  private receiveLoop: Promise<void> | null = null;
  private receiveLoopAbort = false;
  private closeRequested = false;
  private reconnectPromise: Promise<void> | null = null;
  private authOutcome: Deferred<void> | null = null;

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
  }

  async connect(options: ConnectOptions = {}): Promise<void> {
    this.closeRequested = false;
    await this.openAndAuthenticate(false, options.signal);
  }

  async close(): Promise<void> {
    if (this.state === ConnectionState.Closed && !this.transport) {
      return;
    }

    this.closeRequested = true;
    this.receiveLoopAbort = true;
    this.setState(ConnectionState.Closed);
    this.authOutcome?.reject(new ConnectionError("Connection closed"));
    this.authOutcome = null;
    this.multiplexer.setDisconnected();

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
  ): Promise<Uint8Array> {
    this.ensureAuthenticated();
    const transport = this.ensureTransport();
    const frame = FrameCodec.encodeFrame(messageType, requestPayload);

    try {
      return await this.multiplexer.request(
        messageType,
        frame,
        (data) => transport.send(data),
        this.timeout,
      );
    } catch (error) {
      this.handlePossibleTransportFailure(error);
      throw error;
    }
  }

  async send(messageType: number, requestPayload: Uint8Array): Promise<void> {
    this.ensureAuthenticated();
    const transport = this.ensureTransport();
    const frame = FrameCodec.encodeFrame(messageType, requestPayload);

    try {
      await transport.send(frame);
    } catch (error) {
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

  getMultiplexer(): Multiplexer {
    return this.multiplexer;
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

    await this.transport.connect();
    throwIfAborted(signal);
    this.receiveLoop = this.startReceiveLoop();

    this.setState(ConnectionState.Authenticating);
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
      this.setState(ConnectionState.Authenticated);
      this.multiplexer.setConnected();
      if (isReconnect) {
        await this.restoreReconnectState();
      }
    } catch (error) {
      this.authOutcome = null;
      this.multiplexer.setDisconnected();
      if (this.transport) {
        await this.transport.close().catch(() => undefined);
        this.transport = null;
      }
      this.setState(ConnectionState.Disconnected);
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

    if (this.state === ConnectionState.Authenticating && this.authOutcome) {
      this.authOutcome.reject(
        new AuthenticationError(this.describeConnectionLoss(error)),
      );
    }

    if (this.closeRequested) {
      this.setState(ConnectionState.Closed);
      return;
    }

    this.setState(ConnectionState.Disconnected);

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

      try {
        await sleep(delayMs);
        await this.openAndAuthenticate(true);
        return;
      } catch {
        delayMs = Math.min(delayMs * 2, this.reconnectMaxBackoffMs);
      }
    }

    this.setState(ConnectionState.Disconnected);
  }

  private async restoreReconnectState(): Promise<void> {
    for (const listener of this.reconnectListeners) {
      await listener();
    }
  }

  private ensureTransport(): Transport {
    if (!this.transport) {
      throw new ConnectionError("No active transport");
    }
    return this.transport;
  }

  private ensureAuthenticated(): void {
    if (this.closeRequested || this.state !== ConnectionState.Authenticated) {
      throw new ConnectionError(
        `Cannot use connection while state is ${this.state}`,
      );
    }
  }

  private setState(newState: ConnectionState): void {
    this.state = newState;
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
