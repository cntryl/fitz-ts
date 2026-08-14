/**
 * Common type definitions for Fitz client
 */

/** Transport used to reach Fitz. `auto` selects TCP for `tcp:` URLs and WebSocket otherwise. */
export type TransportType = "ws" | "tcp" | "auto";

/** Supplies the current bearer token for each authentication attempt. May refresh asynchronously. */
export type TokenProvider = () => string | Promise<string>;

/** Severity passed to a {@link FitzLogger}. */
export type FitzLogLevel = "debug" | "info" | "warn" | "error";

/** Structured logging adapter. Implementations must not throw or mutate `fields`. */
export interface FitzLogger {
  /** Records a Fitz lifecycle or operation event at `level`. */
  log(level: FitzLogLevel, event: string, fields?: Record<string, unknown>): void;
}

/** Tracing adapter used to create spans around client operations. */
export interface FitzTracer {
  /** Starts a span. Return a cheap no-op span when the event is not sampled. */
  startSpan(name: string, attributes?: Record<string, unknown>): FitzSpan;
}

/** Mutable tracing span owned and ended by the Fitz client. */
export interface FitzSpan {
  /** Adds or replaces an attribute on the active span. */
  setAttribute(key: string, value: unknown): void;
  /** Associates a caught exception with the span without rethrowing it. */
  recordException(error: unknown): void;
  /** Finishes the span. Implementations should tolerate exactly one call. */
  end(): void;
}

/** Metrics adapter. Names and attributes are controlled by Fitz; implementations should not throw. */
export interface FitzMeter {
  /** Adds `value` to a counter. */
  counter(name: string, value: number, attributes?: Record<string, unknown>): void;
  /** Records `value` in a histogram. */
  histogram(name: string, value: number, attributes?: Record<string, unknown>): void;
  /** Records the current value of a gauge when the backend supports gauges. */
  gauge?(name: string, value: number, attributes?: Record<string, unknown>): void;
}

/** Structured notification describing a connection lifecycle transition. */
export interface FitzLifecycleEvent {
  /** Stable event name suitable for machine processing. */
  event: string;
  /** Connection state after the event. */
  state: ConnectionState;
  /** Transport involved in the event, when one has been selected. */
  transport?: string;
  /** Endpoint involved in the event. Treat credentials embedded in URLs as sensitive. */
  url?: string;
  /** One-based retry or reconnect attempt number, when applicable. */
  attempt?: number;
  /** Redacted error summary. Use normal caught errors for programmatic recovery. */
  error?: string;
}

/** Optional observability adapters. Omit this object when telemetry is not required. */
export interface FitzObservability {
  /** Structured logger. */
  logger?: FitzLogger;
  /** Distributed tracing adapter. */
  tracer?: FitzTracer;
  /** Metrics adapter. */
  meter?: FitzMeter;
  /** Receives connection lifecycle events; must return quickly and must not throw. */
  onLifecycleEvent?: (event: FitzLifecycleEvent) => void;
}

/** Limits for asynchronously dispatched subscription and worker handlers. */
export interface AsyncHandlerOptions {
  /** Maximum handlers run concurrently. Defaults to `Infinity`; must be at least 1. */
  maxConcurrency?: number;
  /** Per-handler deadline in milliseconds. Defaults to 30,000. */
  timeoutMs?: number;
}

/** Automatic reconnection policy for an established client. */
export interface ReconnectOptions {
  /** Enables reconnect after an unexpected disconnect. Defaults to `true`. */
  enabled?: boolean;
  /** Maximum reconnect attempts. Defaults to `Infinity`. */
  maxAttempts?: number;
  /** Initial randomized backoff in milliseconds. Defaults to 250. */
  backoffMs?: number;
  /** Backoff ceiling in milliseconds. Defaults to 5,000. */
  maxBackoffMs?: number;
}

/** Retry policy for operations classified by Fitz as safe to replay. */
export interface RetryOptions {
  /** Enables safe operation retries. Defaults to `true`. */
  enabled?: boolean;
  /** Maximum total attempts, including the first. Defaults to 3. */
  maxAttempts?: number;
  /** Initial randomized backoff in milliseconds. Defaults to 100. */
  backoffMs?: number;
  /** Backoff ceiling in milliseconds. Defaults to 1,000. */
  maxBackoffMs?: number;
}

/** Heartbeat policy used to detect connections that stopped making progress. */
export interface HeartbeatOptions {
  /** Enables heartbeats. Defaults to `true`. */
  enabled?: boolean;
  /** Time in milliseconds between heartbeat probes. Defaults to 10,000. */
  intervalMs?: number;
  /** Time in milliseconds without a valid heartbeat before failure. Defaults to 30,000. */
  timeoutMs?: number;
}

/** Node.js WebSocket upgrade configuration. Do not use this for browser-only clients. */
export interface WebSocketOptions {
  /**
   * Extra HTTP headers for Node.js WebSocket upgrade requests.
   *
   * Browser WebSocket implementations do not allow callers to set upgrade
   * headers, so these are applied only when the Node `ws` transport is used.
   */
  headers?: Record<string, string>;
}

/** Complete configuration accepted by the Node.js client factory. */
export interface ClientConfig {
  /** Fitz endpoint URL. Use `tcp://` in Node or `ws://`/`wss://` in either runtime. */
  url: string;
  /** Bearer-token supplier. Omit only when the server permits anonymous clients. */
  tokenProvider?: TokenProvider;
  /** Default request timeout in milliseconds. Defaults to 30,000. */
  timeout?: number;
  /** Transport selection. Defaults to `auto`; browsers support only `ws` and `auto`. */
  transport?: TransportType;
  /** Node WebSocket upgrade options. */
  webSocket?: WebSocketOptions;
  /** Reconnection policy for established connections. */
  reconnect?: ReconnectOptions;
  /** Retry policy for operations that are safe to replay. */
  retry?: RetryOptions;
  /** Heartbeat policy. */
  heartbeat?: HeartbeatOptions;
  /** Largest accepted complete wire frame in bytes. Defaults to 65,540. */
  maxFrameSize?: number;
  /** Delay in milliseconds allowed for authentication settlement. Defaults to 1,000. */
  authSettleDelayMs?: number;
  /** Maximum requests concurrently awaiting responses. Defaults to 256. */
  maxInFlightRequests?: number;
  /** Maximum requests waiting for an in-flight slot. Defaults to 1,024. */
  maxRequestQueueSize?: number;
  /** Optional logging, tracing, metrics, and lifecycle adapters. */
  observability?: FitzObservability;
  /** Concurrency and timeout limits for user callbacks. */
  asyncHandlers?: AsyncHandlerOptions;
}

/** Options for one explicit connection attempt. */
export interface ClientConnectOptions {
  /** Cancels waiting for this attempt; it does not close an already connected client. */
  signal?: AbortSignal;
}

/** Options for repeatedly connecting until Fitz becomes ready. */
export interface ConnectWhenReadyOptions {
  /** Cancels readiness waiting. */
  signal?: AbortSignal;
  /** Overall deadline in milliseconds. Use `Infinity` for no deadline. */
  timeoutMs?: number;
  /** Initial retry delay in milliseconds. Defaults to 250. */
  backoffMs?: number;
  /** Retry-delay ceiling in milliseconds. Defaults to 2,000. */
  maxBackoffMs?: number;
}

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

/**
 * Connection state machine
 */
export enum ConnectionState {
  /** No transport is connected; a later explicit connect is allowed. */
  Disconnected = "DISCONNECTED",
  /** Transport connection is in progress. */
  Connecting = "CONNECTING",
  /** Transport is open but authentication may not yet be complete. */
  Connected = "CONNECTED",
  /** Authentication request is in progress. */
  Authenticating = "AUTHENTICATING",
  /** Connection is authenticated and domain operations may be issued. */
  Authenticated = "AUTHENTICATED",
  /** Automatic recovery is attempting to restore the connection and subscriptions. */
  Reconnecting = "RECONNECTING",
  /** Client was permanently closed and cannot reconnect. */
  Closed = "CLOSED",
}
