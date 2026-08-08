/**
 * TCP transport implementation (Node.js only)
 */

import { Transport, TransportOptions } from "./types";
import { TransportError, TimeoutError } from "../core/errors";
import { abortError } from "../core/abort";

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
  once(event: "drain", listener: () => void): void;
  removeListener(event: "drain", listener: () => void): void;
  write(data: Uint8Array, callback: (err?: Error | null) => void): boolean;
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
  let receiver: {
    resolve: (data: Uint8Array) => void;
    reject: (error: Error) => void;
  } | null = null;
  let terminalError: Error | null = null;
  const timeout = options.timeout ?? 30000;
  const maxFrameSize = options.maxFrameSize ?? 65540;
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
    if (receiver) {
      receiver.resolve(data);
      receiver = null;
    } else {
      receiveQueue.push(data);
    }
  };

  const failReceive = (error: Error): void => {
    // First error wins. `close()` on an idle socket, the 'error' listener
    // re-firing after `destroy(error)` re-emits it, and a live receive-loop
    // timeout can all call this for the same underlying failure — without
    // this guard, a later, less-specific re-wrap (e.g. a generic
    // TransportError from the 'error' listener) would silently overwrite a
    // more specific one already recorded (e.g. a TimeoutError from the
    // 'timeout' listener), breaking downstream `instanceof` classification.
    terminalError ??= error;
    if (receiver) {
      receiver.reject(error);
      receiver = null;
    }
  };

  const connect = async (options: { signal?: AbortSignal } = {}): Promise<void> => {
    return new Promise((resolve, reject) => {
      try {
        if (options.signal?.aborted) {
          reject(abortError());
          return;
        }

        if (!netModule) {
          reject(new TransportError("TCP transport module unavailable"));
          return;
        }

        socket = netModule.createConnection({
          host,
          port,
          timeout,
        });

        const activeSocket = socket;
        let connectSettled = false;
        let connectTimeout: ReturnType<typeof setTimeout> | null = null;
        let onAbort: (() => void) | null = null;

        const cleanupConnectListeners = (): void => {
          if (onAbort) {
            options.signal?.removeEventListener("abort", onAbort);
          }
        };

        const settleConnect = (callback: () => void): void => {
          if (connectSettled) {
            return;
          }

          connectSettled = true;
          if (connectTimeout) {
            clearTimeout(connectTimeout);
            connectTimeout = null;
          }
          cleanupConnectListeners();
          callback();
        };

        onAbort = (): void => {
          settleConnect(() => {
            connected = false;
            activeSocket.destroy();
            reject(abortError());
          });
        };

        options.signal?.addEventListener("abort", onAbort, { once: true });
        connectTimeout = setTimeout(() => {
          settleConnect(() => {
            activeSocket.destroy();
            reject(new TimeoutError(`TCP connection timeout after ${timeout}ms`));
          });
        }, timeout);

        activeSocket.on("connect", () => {
          settleConnect(() => {
            connected = true;
            activeSocket.setNoDelay(true);
            activeSocket.setTimeout(receiveTimeoutEnabled ? timeout : 0);
            resolve();
          });
        });

        activeSocket.on("data", (chunk: Uint8Array | Buffer) => {
          handleData(chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk));
        });

        activeSocket.on("error", (err: Error) => {
          connected = false;
          const error = new TransportError(`TCP error: ${err.message || "unknown error"}`);
          failReceive(error);
          settleConnect(() => {
            reject(error);
          });
        });

        activeSocket.on("close", () => {
          connected = false;
          if (!terminalError) {
            failReceive(new TransportError("Connection closed"));
          }
          settleConnect(() => {
            reject(terminalError ?? new TransportError("Connection closed"));
          });
        });

        activeSocket.on("timeout", () => {
          if (receiveTimeoutEnabled) {
            const error = new TimeoutError(`TCP receive timeout after ${timeout}ms`);
            failReceive(error);
            activeSocket.destroy(error);
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

      let settled = false;
      let drainListener: (() => void) | null = null;

      const activeSocket = socket;
      if (!activeSocket) {
        reject(new TransportError("TCP socket is not connected"));
        return;
      }

      const finish = (error: Error | null): void => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeoutId);
        // A send that hasn't drained yet by the time we settle (via timeout
        // or a write error) must not leave its 'drain' listener dangling —
        // it would otherwise linger until an eventual drain event, or
        // accumulate one more listener per timed-out retry on the same
        // still-backpressured socket.
        if (drainListener) {
          activeSocket.removeListener("drain", drainListener);
          drainListener = null;
        }
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      };

      const timeoutId = setTimeout(() => {
        finish(new TimeoutError(`TCP send timeout after ${timeout}ms`));
      }, timeout);

      let writeCompleted = false;
      let drainCompleted = true;
      const settle = (error?: Error | null): void => {
        if (error) {
          finish(new TransportError(`TCP send failed: ${error.message}`));
          return;
        }

        if (!writeCompleted || !drainCompleted) {
          return;
        }

        finish(null);
      };

      const accepted = activeSocket.write(fullMessage, (err?: Error | null) => {
        writeCompleted = true;
        settle(err);
      });

      if (!accepted) {
        drainCompleted = false;
        drainListener = () => {
          drainCompleted = true;
          settle();
        };
        activeSocket.once("drain", drainListener);
      }
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

    if (terminalError) {
      throw terminalError;
    }

    if (!connected) {
      throw new TransportError("Connection closed");
    }

    return new Promise((resolve, reject) => {
      const timeoutId = receiveTimeoutEnabled
        ? setTimeout(() => {
            receiver = null;
            reject(new TimeoutError(`TCP receive timeout after ${timeout}ms`));
          }, timeout)
        : null;

      receiver = {
        resolve: (data: Uint8Array) => {
          if (timeoutId) {
            clearTimeout(timeoutId);
          }
          receiver = null;
          resolve(data);
        },
        reject: (error: Error) => {
          if (timeoutId) {
            clearTimeout(timeoutId);
          }
          receiver = null;
          reject(error);
        },
      };
    });
  };

  const close = async (): Promise<void> => {
    if (socket) {
      const activeSocket = socket;
      if (!connected) {
        if (socket === activeSocket) {
          socket = null;
        }
        terminalError ??= new TransportError("Connection closed");
        failReceive(terminalError);
        activeSocket.destroy();
        return;
      }

      connected = false;
      return new Promise<void>((resolve) => {
        const timeoutId = setTimeout(() => {
          activeSocket.destroy();
          if (socket === activeSocket) {
            socket = null;
          }
          failReceive(new TransportError("Connection closed"));
          resolve();
        }, 5000);

        activeSocket.on("close", () => {
          clearTimeout(timeoutId);
          if (socket === activeSocket) {
            socket = null;
          }
          terminalError ??= new TransportError("Connection closed");
          resolve();
        });

        activeSocket.end();
      });
    }

    connected = false;
    terminalError = new TransportError("Connection closed");
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
