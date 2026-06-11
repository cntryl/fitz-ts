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
  createConnection(options: { host: string; port: number; timeout: number }): TcpSocket;
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
  setKeepAlive(enable?: boolean, initialDelay?: number): void;
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

export function createTcpTransport(url: string, options: TransportOptions = {}): Transport {
  if (!isNode()) {
    throw new Error("TCP transport is only available in Node.js");
  }

  let socket: TcpSocket | null = null;
  let connected = false;
  const receiveQueue: Uint8Array[] = [];
  let receiverResolve: ((data: Uint8Array) => void) | null = null;
  const timeout = options.timeout ?? 30000;
  const maxFrameSize = options.maxFrameSize ?? 65535;
  const receiveTimeoutEnabled = options.receiveTimeout ?? true;
  let lengthBuffer = new Uint8Array(4);
  let lengthOffset = 0;
  let currentMessageLength: number | null = null;
  let messageBuffer: Uint8Array | null = null;
  let messageOffset = 0;

  const urlObj = new URL(url.startsWith("tcp://") ? url : `tcp://${url}`);
  const host = urlObj.hostname || "localhost";
  const port = parseInt(urlObj.port || "4090", 10);

  const handleData = (chunk: Uint8Array) => {
    let offset = 0;

    while (offset < chunk.length) {
      if (currentMessageLength === null) {
        const needed = 4 - lengthOffset;
        const available = chunk.length - offset;
        const toCopy = Math.min(needed, available);

        lengthBuffer.set(chunk.slice(offset, offset + toCopy), lengthOffset);
        lengthOffset += toCopy;
        offset += toCopy;

        if (lengthOffset === 4) {
          const lengthView = new DataView(lengthBuffer.buffer);
          currentMessageLength = lengthView.getUint32(0, false);
          if (currentMessageLength > maxFrameSize) {
            socket?.destroy(
              new Error(
                `TCP frame length ${currentMessageLength} exceeds max frame size ${maxFrameSize}`,
              ),
            );
            currentMessageLength = null;
            lengthOffset = 0;
            return;
          }
          messageBuffer = new Uint8Array(currentMessageLength);
          messageOffset = 0;
        }
      }

      if (currentMessageLength !== null && messageBuffer !== null) {
        const needed = currentMessageLength - messageOffset;
        const available = chunk.length - offset;
        const toCopy = Math.min(needed, available);

        messageBuffer.set(chunk.slice(offset, offset + toCopy), messageOffset);
        messageOffset += toCopy;
        offset += toCopy;

        if (messageOffset === currentMessageLength) {
          enqueueMessage(messageBuffer);
          currentMessageLength = null;
          lengthOffset = 0;
          messageBuffer = null;
          messageOffset = 0;
        }
      }
    }
  };

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
        if (!netModule) {
          reject(new TransportError("TCP transport module unavailable"));
          return;
        }

        socket = netModule.createConnection({
          host,
          port,
          timeout,
        });

        const connectTimeout = setTimeout(() => {
          socket?.destroy();
          reject(new TimeoutError(`TCP connection timeout after ${timeout}ms`));
        }, timeout);

        const activeSocket = socket;

        activeSocket.on("connect", () => {
          clearTimeout(connectTimeout);
          connected = true;
          activeSocket.setNoDelay(true);
          activeSocket.setTimeout(receiveTimeoutEnabled ? timeout : 0);
          resolve();
        });

        activeSocket.on("data", (chunk: Uint8Array | Buffer) => {
          handleData(chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk));
        });

        activeSocket.on("error", (err: Error) => {
          clearTimeout(connectTimeout);
          connected = false;
          const error = new TransportError(`TCP error: ${err.message || "unknown error"}`);
          if (receiverResolve) {
            receiverResolve(new Uint8Array(0));
          }
          reject(error);
        });

        activeSocket.on("close", () => {
          connected = false;
          if (receiverResolve) {
            receiverResolve(new Uint8Array(0));
          }
        });

        activeSocket.on("timeout", () => {
          if (receiveTimeoutEnabled) {
            activeSocket.destroy();
            connected = false;
          }
        });
      } catch (err) {
        reject(
          new TransportError(
            `Failed to create TCP socket: ${err instanceof Error ? err.message : String(err)}`,
          ),
        );
      }
    });
  };

  const send = async (data: Uint8Array): Promise<void> => {
    if (!connected) {
      throw new TransportError("TCP socket is not connected");
    }

    return new Promise((resolve, reject) => {
      const lengthBuffer = new Uint8Array(4);
      const lengthView = new DataView(lengthBuffer.buffer);
      lengthView.setUint32(0, data.length, false);

      const fullMessage = new Uint8Array(lengthBuffer.length + data.length);
      fullMessage.set(lengthBuffer, 0);
      fullMessage.set(data, lengthBuffer.length);

      const timeoutId = setTimeout(() => {
        reject(new TimeoutError(`TCP send timeout after ${timeout}ms`));
      }, timeout);

      socket?.write(fullMessage, (err?: Error | null) => {
        clearTimeout(timeoutId);
        if (err) {
          reject(new TransportError(`TCP send failed: ${err.message}`));
        } else {
          resolve();
        }
      });
    });
  };

  const enableKeepAlive = (intervalMs: number): void => {
    socket?.setKeepAlive(true, intervalMs);
  };

  const supportsHeartbeat = (): boolean => false;

  const receive = async (): Promise<Uint8Array> => {
    if (receiveQueue.length > 0) {
      const message = receiveQueue.shift();
      if (!message) {
        throw new TransportError("TCP receive queue was unexpectedly empty");
      }
      return message;
    }

    return new Promise((resolve, reject) => {
      const timeoutId = receiveTimeoutEnabled
        ? setTimeout(() => {
            receiverResolve = null;
            reject(new TimeoutError(`TCP receive timeout after ${timeout}ms`));
          }, timeout)
        : null;

      receiverResolve = (data: Uint8Array) => {
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        receiverResolve = null;
        if (data.length === 0) {
          reject(new TransportError("Connection closed"));
        } else {
          resolve(data);
        }
      };
    });
  };

  const close = async (): Promise<void> => {
    if (socket) {
      const activeSocket = socket;
      return new Promise<void>((resolve) => {
        const timeoutId = setTimeout(() => {
          activeSocket.destroy();
          connected = false;
          resolve();
        }, 5000);

        activeSocket.on("close", () => {
          clearTimeout(timeoutId);
          connected = false;
          resolve();
        });

        activeSocket.end();
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
    supportsHeartbeat,
    enableKeepAlive,
    close,
    getUrl,
    isConnected,
  };
}
