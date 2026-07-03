/**
 * Browser WebSocket transport implementation.
 */

import { Transport, TransportOptions } from "./types";
import { TransportError, TimeoutError } from "../core/errors";

type BrowserWebSocketMessageData = ArrayBuffer | ArrayBufferView | Blob;
type BrowserWebSocketMessageEvent = {
  data: BrowserWebSocketMessageData;
};
type BrowserWebSocketLike = {
  binaryType: BinaryType;
  onopen: (() => void) | null;
  onmessage: ((event: BrowserWebSocketMessageEvent) => void) | null;
  onerror: ((event: Event | { message?: string }) => void) | null;
  onclose: (() => void) | null;
  send(data: Uint8Array): void;
  close(code?: number, reason?: string): void;
};
type BrowserWebSocketConstructor = new (
  url: string,
  protocols?: string | string[],
) => BrowserWebSocketLike;

function getWebSocketConstructor(): BrowserWebSocketConstructor {
  const browserWebSocket = globalThis.WebSocket as unknown as
    | BrowserWebSocketConstructor
    | undefined;

  if (!browserWebSocket) {
    throw new TransportError("WebSocket is not available in this environment");
  }

  return browserWebSocket;
}

function describeWebSocketError(event: Event | { message?: string }): string {
  return "message" in event && event.message ? event.message : "unknown error";
}

function normalizeWebSocketUrl(url: string): string {
  if (url.startsWith("ws://") || url.startsWith("wss://")) {
    return url;
  }

  if (url.startsWith("https://")) {
    return url.replace(/^https:\/\//, "wss://");
  }

  if (url.startsWith("http://")) {
    return url.replace(/^http:\/\//, "ws://");
  }

  return `ws://${url}`;
}

function isBlob(data: BrowserWebSocketMessageData): data is Blob {
  return typeof Blob !== "undefined" && data instanceof Blob;
}

function toUint8Array(data: Exclude<BrowserWebSocketMessageData, Blob>): Uint8Array {
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }

  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
}

export function createWebSocketTransport(url: string, options: TransportOptions = {}): Transport {
  let ws: BrowserWebSocketLike | null = null;
  let connected = false;
  const receiveQueue: Uint8Array[] = [];
  let receiverResolve: ((data: Uint8Array | null) => void) | null = null;
  const timeout = options.timeout ?? 30000;
  const maxFrameSize = options.maxFrameSize ?? 65535;
  const receiveTimeoutEnabled = options.receiveTimeout ?? true;

  const enqueueMessage = (data: Uint8Array) => {
    if (data.length > maxFrameSize) {
      ws?.close(1009, "Frame too large");
      return;
    }

    if (receiverResolve) {
      receiverResolve(data);
      receiverResolve = null;
    } else {
      receiveQueue.push(data);
    }
  };

  const connect = async (): Promise<void> => {
    return new Promise((resolve, reject) => {
      try {
        const wsUrl = normalizeWebSocketUrl(url);
        ws = new (getWebSocketConstructor())(wsUrl);
        ws.binaryType = "arraybuffer";

        let settled = false;
        let connectTimeout: ReturnType<typeof setTimeout> | null = null;
        const settle = (callback: () => void): void => {
          if (settled) {
            return;
          }

          settled = true;
          if (connectTimeout) {
            clearTimeout(connectTimeout);
            connectTimeout = null;
          }
          callback();
        };

        connectTimeout = setTimeout(() => {
          settle(() => {
            connected = false;
            ws?.close();
            reject(new TimeoutError(`WebSocket connection timeout after ${timeout}ms`));
          });
        }, timeout);

        ws.onopen = () => {
          settle(() => {
            connected = true;
            resolve();
          });
        };

        ws.onmessage = (event: BrowserWebSocketMessageEvent) => {
          if (isBlob(event.data)) {
            void event.data.arrayBuffer().then((arrayBuffer) => {
              enqueueMessage(new Uint8Array(arrayBuffer));
            });
            return;
          }

          enqueueMessage(toUint8Array(event.data));
        };

        ws.onerror = (event: Event | { message?: string }) => {
          settle(() => {
            connected = false;
            reject(new TransportError(`WebSocket error: ${describeWebSocketError(event)}`));
          });
        };

        ws.onclose = () => {
          connected = false;
          if (receiverResolve) {
            receiverResolve(null);
          }
        };
      } catch (err) {
        reject(
          new TransportError(
            `Failed to create WebSocket: ${err instanceof Error ? err.message : String(err)}`,
          ),
        );
      }
    });
  };

  const send = async (data: Uint8Array): Promise<void> => {
    if (!connected) {
      throw new TransportError("WebSocket is not connected");
    }

    try {
      ws?.send(data);
    } catch (err) {
      throw new TransportError(
        `WebSocket send error: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  };

  const sendHeartbeat = async (): Promise<void> => {
    throw new TransportError("WebSocket heartbeat is not supported in browsers");
  };

  const supportsHeartbeat = (): boolean => false;

  const enableKeepAlive = (): void => undefined;

  const receive = async (): Promise<Uint8Array> => {
    if (receiveQueue.length > 0) {
      const message = receiveQueue.shift();
      if (!message) {
        throw new TransportError("WebSocket receive queue was unexpectedly empty");
      }
      return message;
    }

    if (!connected) {
      throw new TransportError("Connection closed");
    }

    return new Promise((resolve, reject) => {
      const timeoutId = receiveTimeoutEnabled
        ? setTimeout(() => {
            receiverResolve = null;
            reject(new TimeoutError(`WebSocket receive timeout after ${timeout}ms`));
          }, timeout)
        : null;

      receiverResolve = (data: Uint8Array | null) => {
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        receiverResolve = null;
        if (data === null) {
          reject(new TransportError("Connection closed"));
          return;
        }
        resolve(data);
      };
    });
  };

  const close = async (): Promise<void> => {
    if (ws) {
      const activeWs = ws;
      return new Promise<void>((resolve) => {
        const timeoutId = setTimeout(() => {
          connected = false;
          resolve();
        }, 5000);

        activeWs.onclose = () => {
          clearTimeout(timeoutId);
          connected = false;
          resolve();
        };

        activeWs.close(1000, "Normal closure");
      });
    }

    connected = false;
  };

  const getUrl = (): string => normalizeWebSocketUrl(url);
  const isConnected = (): boolean => connected;

  return {
    connect,
    send,
    receive,
    sendHeartbeat,
    supportsHeartbeat,
    enableKeepAlive,
    close,
    getUrl,
    isConnected,
  };
}
