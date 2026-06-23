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

type WebSocketConstructor = new (
  url: string,
  protocols?: string | string[],
  options?: NodeWebSocketOptions,
) => WebSocketLike;
type NodeWebSocketOptions = {
  headers?: Record<string, string>;
};
type WebSocketMessageEvent = {
  data: ArrayBuffer | Uint8Array | Blob;
};
type WebSocketLike = {
  binaryType: string;
  onopen: (() => void) | null;
  onmessage: ((event: WebSocketMessageEvent) => void) | null;
  onerror: ((event: { message?: string }) => void) | null;
  onclose: (() => void) | null;
  ping?(data?: Uint8Array, mask?: boolean, callback?: (err?: Error) => void): void;
  once?(event: "pong" | "close" | "error", listener: (...args: unknown[]) => void): void;
  removeListener?(event: "pong" | "close" | "error", listener: (...args: unknown[]) => void): void;
  send(data: Uint8Array, callback?: (err?: Error) => void): void;
  close(code?: number, reason?: string): void;
  terminate?(): void;
};

let cachedWebSocketConstructor: WebSocketConstructor | null = null;
const nodeDefaultUpgradeHeaders: Record<string, string> = {
  "User-Agent": "@cntryl/fitz",
  Accept: "*/*",
};

function mergeNodeUpgradeHeaders(headers?: Record<string, string>): Record<string, string> {
  const merged = { ...nodeDefaultUpgradeHeaders };
  const keyIndex = new Map(Object.keys(merged).map((key) => [key.toLowerCase(), key]));

  for (const [key, value] of Object.entries(headers ?? {})) {
    const existingKey = keyIndex.get(key.toLowerCase());
    if (existingKey && existingKey !== key) {
      delete merged[existingKey];
    }

    merged[key] = value;
    keyIndex.set(key.toLowerCase(), key);
  }

  return merged;
}

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
  const receiveTimeoutEnabled = options.receiveTimeout ?? true;
  const nodeUpgradeHeaders = mergeNodeUpgradeHeaders(options.webSocket?.headers);

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

        ws = isNodeEnv()
          ? new (getWebSocketConstructor())(wsUrl, undefined, { headers: nodeUpgradeHeaders })
          : new (getWebSocketConstructor())(wsUrl);
        ws.binaryType = "arraybuffer";

        let settled = false;
        const connectTimeout = setTimeout(() => {
          settle(() => {
            connected = false;
            ws?.close?.();
            reject(new TimeoutError(`WebSocket connection timeout after ${timeout}ms`));
          });
        }, timeout);

        const settle = (callback: () => void): void => {
          if (settled) {
            return;
          }

          settled = true;
          clearTimeout(connectTimeout);
          callback();
        };

        ws.onopen = () => {
          settle(() => {
            connected = true;
            resolve();
          });
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
          settle(() => {
            connected = false;
            const error = new TransportError(
              `WebSocket error: ${event.message || "unknown error"}`,
            );
            reject(error);
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

    return new Promise((resolve, reject) => {
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      const settle = (callback: () => void): void => {
        if (timeoutId) {
          clearTimeout(timeoutId);
          timeoutId = null;
        }
        callback();
      };

      try {
        timeoutId = setTimeout(() => {
          reject(new TimeoutError(`WebSocket send timeout after ${timeout}ms`));
        }, timeout);

        if (isNodeEnv()) {
          ws?.send(data, (err?: Error) => {
            settle(() => {
              if (err) {
                reject(new TransportError(`WebSocket send failed: ${err.message}`));
              } else {
                resolve();
              }
            });
          });
          return;
        }

        ws?.send(data);
        settle(resolve);
      } catch (err) {
        settle(() =>
          reject(
            new TransportError(
              `WebSocket send error: ${err instanceof Error ? err.message : String(err)}`,
            ),
          ),
        );
      }
    });
  };

  const sendHeartbeat = async (heartbeatOptions: { timeoutMs: number }): Promise<void> => {
    if (!connected || !ws) {
      throw new TransportError("WebSocket is not connected");
    }

    const activeWs = ws;

    if (
      typeof activeWs.ping !== "function" ||
      typeof activeWs.once !== "function" ||
      typeof activeWs.removeListener !== "function"
    ) {
      throw new TransportError("WebSocket heartbeat is not supported");
    }

    const socket = activeWs as unknown as WebSocketLike &
      Required<Pick<WebSocketLike, "ping" | "once" | "removeListener">>;

    return new Promise((resolve, reject) => {
      let settled = false;
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      const cleanup = () => {
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        socket.removeListener("pong", onPong);
        socket.removeListener("close", onClose);
        socket.removeListener("error", onError);
      };

      const settle = (callback: () => void) => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        callback();
      };

      const onPong = () => {
        settle(resolve);
      };

      const onClose = () => {
        settle(() => reject(new TransportError("WebSocket closed during heartbeat")));
      };

      const onError = (...args: unknown[]) => {
        const event = args[0] as { message?: string } | Error | undefined;
        const message = event instanceof Error ? event.message : event?.message || "unknown error";
        settle(() => reject(new TransportError(`WebSocket heartbeat failed: ${message}`)));
      };

      timeoutId = setTimeout(() => {
        settle(() =>
          reject(
            new TimeoutError(`WebSocket heartbeat timeout after ${heartbeatOptions.timeoutMs}ms`),
          ),
        );
      }, heartbeatOptions.timeoutMs);

      socket.once("pong", onPong);
      socket.once("close", onClose);
      socket.once("error", onError);

      try {
        socket.ping(new Uint8Array(), undefined, (err?: Error) => {
          if (err) {
            settle(() => reject(new TransportError(`WebSocket ping failed: ${err.message}`)));
          }
        });
      } catch (err) {
        settle(() =>
          reject(
            new TransportError(
              `WebSocket heartbeat error: ${err instanceof Error ? err.message : String(err)}`,
            ),
          ),
        );
      }
    });
  };

  const supportsHeartbeat = (): boolean => {
    return (
      typeof ws?.ping === "function" &&
      typeof ws?.once === "function" &&
      typeof ws?.removeListener === "function"
    );
  };

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
    sendHeartbeat,
    supportsHeartbeat,
    enableKeepAlive,
    close,
    getUrl,
    isConnected,
  };
}
