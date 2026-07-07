/**
 * Transport abstraction for WebSocket and TCP
 */

export interface Transport {
  connect(options?: TransportConnectOptions): Promise<void>;
  send(data: Uint8Array): Promise<void>;
  receive(): Promise<Uint8Array>;
  sendHeartbeat?(options: TransportHeartbeatOptions): Promise<void>;
  supportsHeartbeat?(): boolean;
  enableKeepAlive?(intervalMs: number): void;
  close(): Promise<void>;
  getUrl(): string;
  isConnected(): boolean;
}

export interface TransportConnectOptions {
  signal?: AbortSignal;
}

export type TransportConstructor = new (url: string, options?: TransportOptions) => Transport;

export interface TransportHeartbeatOptions {
  timeoutMs: number;
}

export interface TransportOptions {
  timeout?: number;
  maxFrameSize?: number;
  receiveTimeout?: boolean;
  webSocket?: WebSocketTransportOptions;
}

export interface WebSocketTransportOptions {
  /**
   * Extra HTTP headers for Node.js WebSocket upgrade requests.
   *
   * Browser WebSocket implementations do not allow callers to set upgrade
   * headers, so these are applied only when the Node `ws` transport is used.
   */
  headers?: Record<string, string>;
}
