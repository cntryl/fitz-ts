/**
 * Common type definitions for Fitz client
 */

export type TransportType = "ws" | "tcp" | "auto";

export type TokenProvider = () => string | Promise<string>;

export type FitzLogLevel = "debug" | "info" | "warn" | "error";

export interface FitzLogger {
  log(level: FitzLogLevel, event: string, fields?: Record<string, unknown>): void;
}

export interface FitzTracer {
  startSpan(name: string, attributes?: Record<string, unknown>): FitzSpan;
}

export interface FitzSpan {
  setAttribute(key: string, value: unknown): void;
  recordException(error: unknown): void;
  end(): void;
}

export interface FitzMeter {
  counter(name: string, value: number, attributes?: Record<string, unknown>): void;
  histogram(name: string, value: number, attributes?: Record<string, unknown>): void;
  gauge?(name: string, value: number, attributes?: Record<string, unknown>): void;
}

export interface FitzLifecycleEvent {
  event: string;
  state: ConnectionState;
  transport?: string;
  url?: string;
  attempt?: number;
  error?: string;
}

export interface FitzObservability {
  logger?: FitzLogger;
  tracer?: FitzTracer;
  meter?: FitzMeter;
  onLifecycleEvent?: (event: FitzLifecycleEvent) => void;
}

export interface AsyncHandlerOptions {
  maxConcurrency?: number;
  timeoutMs?: number;
}

export interface ReconnectOptions {
  enabled?: boolean;
  maxAttempts?: number;
  backoffMs?: number;
  maxBackoffMs?: number;
}

export interface RetryOptions {
  enabled?: boolean;
  maxAttempts?: number;
  backoffMs?: number;
  maxBackoffMs?: number;
}

export interface HeartbeatOptions {
  enabled?: boolean;
  intervalMs?: number;
  timeoutMs?: number;
}

export interface ClientConfig {
  url: string;
  tokenProvider?: TokenProvider;
  timeout?: number;
  transport?: TransportType;
  reconnect?: ReconnectOptions;
  retry?: RetryOptions;
  heartbeat?: HeartbeatOptions;
  maxFrameSize?: number;
  authSettleDelayMs?: number;
  maxInFlightRequests?: number;
  maxRequestQueueSize?: number;
  observability?: FitzObservability;
  asyncHandlers?: AsyncHandlerOptions;
}

export interface ClientConnectOptions {
  signal?: AbortSignal;
}

export type TxMode = "ReadOnly" | "ReadWrite";
export type DurabilityMode = "None" | "Async" | "Sync";

/**
 * Deferred is a Promise wrapper that exposes resolve/reject
 */
export interface Deferred<T = unknown> {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
}

export function createDeferred<T = unknown>(): Deferred<T> {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;

  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}

type DeferredConstructor = {
  new <T = unknown>(): Deferred<T>;
  <T = unknown>(): Deferred<T>;
};

export const Deferred: DeferredConstructor = createDeferred as unknown as DeferredConstructor;

/**
 * Connection state machine
 */
export enum ConnectionState {
  Disconnected = "DISCONNECTED",
  Connecting = "CONNECTING",
  Connected = "CONNECTED",
  Authenticating = "AUTHENTICATING",
  Authenticated = "AUTHENTICATED",
  Reconnecting = "RECONNECTING",
  Closed = "CLOSED",
}
