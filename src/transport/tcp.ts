/**
 * TCP transport implementation (Node.js only)
 */

import { Transport, TransportOptions } from "./types";
import { TransportError, TimeoutError } from "../core/errors";

type NodeLikeProcess = {
  versions?: {
    node?: string;
  };
};

type NetModule = {
  createConnection(options: {
    host: string;
    port: number;
    timeout: number;
  }): TcpSocket;
};

type TcpSocket = {
  on(event: "connect", listener: () => void): void;
  on(event: "data", listener: (chunk: Uint8Array | Buffer) => void): void;
  on(event: "error", listener: (err: Error) => void): void;
  on(event: "close", listener: () => void): void;
  on(event: "timeout", listener: () => void): void;
  write(data: Uint8Array, callback: (err?: Error | null) => void): void;
  setNoDelay(noDelay?: boolean): void;
  setTimeout(timeout: number): void;
  end(): void;
  destroy(error?: Error): void;
};

const isNode = (): boolean => {
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

let netModule: NetModule | undefined;
if (isNode()) {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  netModule = require("net") as NetModule;
}

export class TcpTransport implements Transport {
  private socket: TcpSocket | null = null;
  private readonly url: string;
  private connected = false;
  private receiveQueue: Uint8Array[] = [];
  private receiverResolve: ((data: Uint8Array) => void) | null = null;
  private readonly timeout: number;
  private readonly maxFrameSize: number;
  private readonly host: string;
  private readonly port: number;
  private lengthBuffer: Uint8Array = new Uint8Array(4);
  private lengthOffset = 0;
  private currentMessageLength: number | null = null;
  private messageBuffer: Uint8Array | null = null;
  private messageOffset = 0;

  constructor(url: string, options: TransportOptions = {}) {
    if (!isNode()) {
      throw new Error("TCP transport is only available in Node.js");
    }

    this.url = url;
    this.timeout = options.timeout ?? 30000;
    this.maxFrameSize = options.maxFrameSize ?? 65535;

    // Parse URL: tcp://host:port
    const urlObj = new URL(url.startsWith("tcp://") ? url : `tcp://${url}`);
    this.host = urlObj.hostname || "localhost";
    this.port = parseInt(urlObj.port || "4090", 10);
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        if (!netModule) {
          reject(new TransportError("TCP transport module unavailable"));
          return;
        }

        this.socket = netModule.createConnection({
          host: this.host,
          port: this.port,
          timeout: this.timeout,
        });

        const connectTimeout = setTimeout(() => {
          this.socket?.destroy();
          reject(
            new TimeoutError(`TCP connection timeout after ${this.timeout}ms`),
          );
        }, this.timeout);

        const socket = this.socket;

        socket.on("connect", () => {
          clearTimeout(connectTimeout);
          this.connected = true;
          socket.setNoDelay(true);
          socket.setTimeout(this.timeout);
          resolve();
        });

        socket.on("data", (chunk: Uint8Array | Buffer) => {
          this.handleData(
            chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk),
          );
        });

        socket.on("error", (err: Error) => {
          clearTimeout(connectTimeout);
          this.connected = false;
          const error = new TransportError(
            `TCP error: ${err.message || "unknown error"}`,
          );
          if (this.receiverResolve) {
            this.receiverResolve(new Uint8Array(0)); // Signal error
          }
          reject(error);
        });

        socket.on("close", () => {
          this.connected = false;
          if (this.receiverResolve) {
            this.receiverResolve(new Uint8Array(0)); // Signal close
          }
        });

        socket.on("timeout", () => {
          socket.destroy();
          this.connected = false;
        });
      } catch (err) {
        reject(
          new TransportError(
            `Failed to create TCP socket: ${err instanceof Error ? err.message : String(err)}`,
          ),
        );
      }
    });
  }

  private handleData(chunk: Uint8Array) {
    let offset = 0;

    while (offset < chunk.length) {
      // Read length header if we don't have a message length yet
      if (this.currentMessageLength === null) {
        const needed = 4 - this.lengthOffset;
        const available = chunk.length - offset;
        const toCopy = Math.min(needed, available);

        this.lengthBuffer.set(
          chunk.slice(offset, offset + toCopy),
          this.lengthOffset,
        );
        this.lengthOffset += toCopy;
        offset += toCopy;

        if (this.lengthOffset === 4) {
          // We have the full length header
          const lengthView = new DataView(this.lengthBuffer.buffer);
          this.currentMessageLength = lengthView.getUint32(0, false); // Big-endian
          if (this.currentMessageLength > this.maxFrameSize) {
            this.socket?.destroy(
              new Error(
                `TCP frame length ${this.currentMessageLength} exceeds max frame size ${this.maxFrameSize}`,
              ),
            );
            this.currentMessageLength = null;
            this.lengthOffset = 0;
            return;
          }
          this.messageBuffer = new Uint8Array(this.currentMessageLength);
          this.messageOffset = 0;
        }
      }

      // Read message body if we have a message length
      if (this.currentMessageLength !== null && this.messageBuffer !== null) {
        const needed = this.currentMessageLength - this.messageOffset;
        const available = chunk.length - offset;
        const toCopy = Math.min(needed, available);

        this.messageBuffer.set(
          chunk.slice(offset, offset + toCopy),
          this.messageOffset,
        );
        this.messageOffset += toCopy;
        offset += toCopy;

        if (this.messageOffset === this.currentMessageLength) {
          // Message is complete
          this.enqueueMessage(this.messageBuffer);
          this.currentMessageLength = null;
          this.lengthOffset = 0;
          this.messageBuffer = null;
          this.messageOffset = 0;
        }
      }
    }
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
      throw new TransportError("TCP socket is not connected");
    }

    return new Promise((resolve, reject) => {
      // Prepare length-prefixed message
      const lengthBuffer = new Uint8Array(4);
      const lengthView = new DataView(lengthBuffer.buffer);
      lengthView.setUint32(0, data.length, false); // Big-endian

      const fullMessage = new Uint8Array(lengthBuffer.length + data.length);
      fullMessage.set(lengthBuffer, 0);
      fullMessage.set(data, lengthBuffer.length);

      const timeout = setTimeout(() => {
        reject(new TimeoutError(`TCP send timeout after ${this.timeout}ms`));
      }, this.timeout);

      this.socket?.write(fullMessage, (err?: Error | null) => {
        clearTimeout(timeout);
        if (err) {
          reject(new TransportError(`TCP send failed: ${err.message}`));
        } else {
          resolve();
        }
      });
    });
  }

  async receive(): Promise<Uint8Array> {
    // Return queued message if available
    if (this.receiveQueue.length > 0) {
      const message = this.receiveQueue.shift();
      if (!message) {
        throw new TransportError("TCP receive queue was unexpectedly empty");
      }
      return message;
    }

    // Wait for next message
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.receiverResolve = null;
        reject(new TimeoutError(`TCP receive timeout after ${this.timeout}ms`));
      }, this.timeout);

      this.receiverResolve = (data: Uint8Array) => {
        clearTimeout(timeout);
        this.receiverResolve = null;
        if (data.length === 0) {
          reject(new TransportError("Connection closed"));
        } else {
          resolve(data);
        }
      };
    });
  }

  async close(): Promise<void> {
    if (this.socket) {
      const socket = this.socket;
      return new Promise<void>((resolve) => {
        const timeout = setTimeout(() => {
          socket.destroy();
          this.connected = false;
          resolve();
        }, 5000);

        socket.on("close", () => {
          clearTimeout(timeout);
          this.connected = false;
          resolve();
        });

        socket.end();
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
