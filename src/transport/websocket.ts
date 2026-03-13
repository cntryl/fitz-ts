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

let WS: WebSocketConstructor;

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

if (isNodeEnv()) {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    WS = require("ws") as WebSocketConstructor;
  } catch {
    throw new Error(
      "ws package is required for Node.js. Install with: npm install ws",
    );
  }
} else {
  const browserWebSocket = globalThis.WebSocket as unknown as
    | WebSocketConstructor
    | undefined;
  if (!browserWebSocket) {
    throw new Error("WebSocket is not available in this environment");
  }
  WS = browserWebSocket;
}

export class WebSocketTransport implements Transport {
  private ws: WebSocketLike | null = null;
  private readonly url: string;
  private connected = false;
  private receiveQueue: Uint8Array[] = [];
  private receiverResolve: ((data: Uint8Array | null) => void) | null = null;
  private readonly timeout: number;
  private readonly maxFrameSize: number;

  constructor(url: string, options: TransportOptions = {}) {
    this.url = url;
    this.timeout = options.timeout ?? 30000;
    this.maxFrameSize = options.maxFrameSize ?? 65535;
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        // Ensure URL has proper protocol
        let wsUrl = this.url;
        if (!wsUrl.startsWith("ws://") && !wsUrl.startsWith("wss://")) {
          if (wsUrl.startsWith("https://")) {
            wsUrl = wsUrl.replace("https://", "wss://");
          } else if (wsUrl.startsWith("http://")) {
            wsUrl = wsUrl.replace("http://", "ws://");
          } else {
            wsUrl = `ws://${wsUrl}`;
          }
        }

        this.ws = new WS(wsUrl);
        this.ws.binaryType = "arraybuffer";

        const connectTimeout = setTimeout(() => {
          this.ws?.close?.();
          reject(
            new TimeoutError(
              `WebSocket connection timeout after ${this.timeout}ms`,
            ),
          );
        }, this.timeout);

        this.ws.onopen = () => {
          clearTimeout(connectTimeout);
          this.connected = true;
          resolve();
        };

        this.ws.onmessage = (event: WebSocketMessageEvent) => {
          let data: Uint8Array;
          if (event.data instanceof ArrayBuffer) {
            data = new Uint8Array(event.data);
          } else if (event.data instanceof Uint8Array) {
            data = event.data;
          } else {
            // Browser might send Blob
            if (event.data instanceof Blob) {
              event.data.arrayBuffer().then((ab: ArrayBuffer) => {
                this.enqueueMessage(new Uint8Array(ab));
              });
              return;
            }
            return;
          }
          if (data.length > this.maxFrameSize) {
            this.ws?.close?.(1009, "Frame too large");
            return;
          }
          this.enqueueMessage(data);
        };

        this.ws.onerror = (event: { message?: string }) => {
          clearTimeout(connectTimeout);
          const error = new TransportError(
            `WebSocket error: ${event.message || "unknown error"}`,
          );
          reject(error);
        };

        this.ws.onclose = () => {
          this.connected = false;
          if (this.receiverResolve) {
            this.receiverResolve(null);
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
  }

  private enqueueMessage(data: Uint8Array) {
    if (this.receiverResolve) {
      this.receiverResolve(data);
      this.receiverResolve = null;
    } else {
      this.receiveQueue.push(data);
    }
  }

  async send(data: Uint8Array): Promise<void> {
    if (!this.connected) {
      throw new TransportError("WebSocket is not connected");
    }

    return new Promise((resolve, reject) => {
      try {
        const timeout = setTimeout(() => {
          reject(
            new TimeoutError(`WebSocket send timeout after ${this.timeout}ms`),
          );
        }, this.timeout);

        this.ws?.send(data, (err?: Error) => {
          clearTimeout(timeout);
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
  }

  async receive(): Promise<Uint8Array> {
    // Return queued message if available
    if (this.receiveQueue.length > 0) {
      const message = this.receiveQueue.shift();
      if (!message) {
        throw new TransportError(
          "WebSocket receive queue was unexpectedly empty",
        );
      }
      return message;
    }

    if (!this.connected) {
      throw new TransportError("Connection closed");
    }

    // Create new receiver promise
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.receiverResolve = null;
        reject(
          new TimeoutError(`WebSocket receive timeout after ${this.timeout}ms`),
        );
      }, this.timeout);

      this.receiverResolve = (data: Uint8Array | null) => {
        clearTimeout(timeout);
        this.receiverResolve = null;
        if (data === null) {
          reject(new TransportError("Connection closed"));
          return;
        }
        resolve(data);
      };
    });
  }

  async close(): Promise<void> {
    if (this.ws) {
      const ws = this.ws;
      return new Promise<void>((resolve) => {
        const timeout = setTimeout(() => {
          ws.terminate?.();
          this.connected = false;
          resolve();
        }, 5000);

        ws.onclose = () => {
          clearTimeout(timeout);
          this.connected = false;
          resolve();
        };

        ws.close(1000, "Normal closure");
      });
    }

    this.connected = false;
  }

  getUrl(): string {
    return this.url;
  }

  isConnected(): boolean {
    return this.connected;
  }
}
