/**
 * Transport abstraction for WebSocket and TCP
 */

export interface Transport {
  connect(): Promise<void>;
  send(data: Uint8Array): Promise<void>;
  receive(): Promise<Uint8Array>;
  sendHeartbeat?(options: TransportHeartbeatOptions): Promise<void>;
  supportsHeartbeat?(): boolean;
  enableKeepAlive?(intervalMs: number): void;
  close(): Promise<void>;
  getUrl(): string;
  isConnected(): boolean;
}

export type TransportConstructor = new (url: string, options?: TransportOptions) => Transport;

export interface TransportHeartbeatOptions {
  timeoutMs: number;
}

export interface TransportOptions {
  timeout?: number;
  maxFrameSize?: number;
  receiveTimeout?: boolean;
}
