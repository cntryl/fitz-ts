/**
 * Common type definitions for Fitz client
 */

export type TransportType = "ws" | "tcp" | "auto";

export type TokenProvider = () => string | Promise<string>;

export interface ReconnectOptions {
  enabled?: boolean;
  maxAttempts?: number;
  backoffMs?: number;
  maxBackoffMs?: number;
}

export interface ClientConfig {
  url: string;
  tokenProvider?: TokenProvider;
  timeout?: number;
  transport?: TransportType;
  reconnect?: ReconnectOptions;
  maxFrameSize?: number;
  authSettleDelayMs?: number;
}

export type TxMode = "ReadOnly" | "ReadWrite";
export type DurabilityMode = "None" | "Async" | "Sync";

/**
 * Deferred is a Promise wrapper that exposes resolve/reject
 */
export class Deferred<T = unknown> {
  promise: Promise<T>;
  resolve!: (value: T | PromiseLike<T>) => void;
  reject!: (reason?: unknown) => void;

  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

/**
 * Connection state machine
 */
export enum ConnectionState {
  Disconnected = "DISCONNECTED",
  Connecting = "CONNECTING",
  Authenticating = "AUTHENTICATING",
  Authenticated = "AUTHENTICATED",
  Reconnecting = "RECONNECTING",
  Closed = "CLOSED",
}
