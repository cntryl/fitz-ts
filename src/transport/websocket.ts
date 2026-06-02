/**
 * WebSocket transport implementation for Node.js and browser
 */

import { Transport, TransportOptions } from "./types";
import { TransportError, TimeoutError } from "../core/errors";

type NodeLikeProcess = {
  versions?: {
    node?: string;
  };
};

type WebSocketConstructor = new (url: string) => WebSocketLike;
type WebSocketMessageEvent = {
  data: ArrayBuffer | Uint8Array | Blob;
};
type WebSocketLike = {
  binaryType: string;
  onopen: (() => void) | null;
  onmessage: ((event: WebSocketMessageEvent) => void) | null;
  onerror: ((event: { message?: string }) => void) | null;
  onclose: (() => void) | null;
  send(data: Uint8Array, callback?: (err?: Error) => void): void;
  close(code?: number, reason?: string): void;
  terminate?(): void;
};

let cachedWebSocketConstructor: WebSocketConstructor | null = null;

const isNodeEnv = (): boolean => {
  try {
    const candidate = globalThis as typeof globalThis & {
      process?: NodeLikeProcess;
    };
    return (
      typeof candidate.process !== "undefined" &&
      typeof candidate.process?.versions?.node === "string"
    );
  } catch {
    return false;
  }
};

function getWebSocketConstructor(): WebSocketConstructor {
  if (cachedWebSocketConstructor) {
    return cachedWebSocketConstructor;
  }

  if (isNodeEnv()) {
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      cachedWebSocketConstructor = require("ws") as WebSocketConstructor;
      return cachedWebSocketConstructor;
    } catch {
      throw new TransportError("ws package is required for Node.js. Install with: npm install ws");
    }
  }

  const browserWebSocket = globalThis.WebSocket as unknown as WebSocketConstructor | undefined;
  if (!browserWebSocket) {
    throw new TransportError("WebSocket is not available in this environment");
  }

  cachedWebSocketConstructor = browserWebSocket;
  return cachedWebSocketConstructor;
}

export function createWebSocketTransport(url: string, options: TransportOptions = {}): Transport {
  let ws: WebSocketLike | null = null;
  let connected = false;
  const receiveQueue: Uint8Array[] = [];
  let receiverResolve: ((data: Uint8Array | null) => void) | null = null;
  const timeout = options.timeout ?? 30000;
  const maxFrameSize = options.maxFrameSize ?? 65535;

  const enqueueMessage = (data: Uint8Array) => {
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
        let wsUrl = url;
        if (!wsUrl.startsWith("ws://") && !wsUrl.startsWith("wss://")) {
          if (wsUrl.startsWith("https://")) {
            wsUrl = wsUrl.replace("https://", "wss://");
          } else if (wsUrl.startsWith("http://")) {
            wsUrl = wsUrl.replace("http://", "ws://");
          } else {
            wsUrl = `ws://${wsUrl}`;
          }
        }

        ws = new (getWebSocketConstructor())(wsUrl);
        ws.binaryType = "arraybuffer";

        const connectTimeout = setTimeout(() => {
          ws?.close?.();
          reject(new TimeoutError(`WebSocket connection timeout after ${timeout}ms`));
        }, timeout);

        ws.onopen = () => {
          clearTimeout(connectTimeout);
          connected = true;
          resolve();
        };

        ws.onmessage = (event: WebSocketMessageEvent) => {
          let data: Uint8Array;
          if (event.data instanceof ArrayBuffer) {
            data = new Uint8Array(event.data);
          } else if (event.data instanceof Uint8Array) {
            data = event.data;
          } else {
            if (event.data instanceof Blob) {
              void event.data.arrayBuffer().then((ab: ArrayBuffer) => {
                enqueueMessage(new Uint8Array(ab));
              });
              return;
            }
            return;
          }
          if (data.length > maxFrameSize) {
            ws?.close?.(1009, "Frame too large");
            return;
          }
          enqueueMessage(data);
        };

        ws.onerror = (event: { message?: string }) => {
          clearTimeout(connectTimeout);
          const error = new TransportError(`WebSocket error: ${event.message || "unknown error"}`);
          reject(error);
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

    return new Promise((resolve, reject) => {
      try {
        const timeoutId = setTimeout(() => {
          reject(new TimeoutError(`WebSocket send timeout after ${timeout}ms`));
        }, timeout);

        ws?.send(data, (err?: Error) => {
          clearTimeout(timeoutId);
          if (err) {
            reject(new TransportError(`WebSocket send failed: ${err.message}`));
          } else {
            resolve();
          }
        });
      } catch (err) {
        reject(
          new TransportError(
            `WebSocket send error: ${err instanceof Error ? err.message : String(err)}`,
          ),
        );
      }
    });
  };

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
      const timeoutId = setTimeout(() => {
        receiverResolve = null;
        reject(new TimeoutError(`WebSocket receive timeout after ${timeout}ms`));
      }, timeout);

      receiverResolve = (data: Uint8Array | null) => {
        clearTimeout(timeoutId);
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
          activeWs.terminate?.();
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

  const getUrl = (): string => url;
  const isConnected = (): boolean => connected;

  return {
    connect,
    send,
    receive,
    close,
    getUrl,
    isConnected,
  };
}
