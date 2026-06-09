/**
 * RPC domain client.
 */

import { createDomainClient } from "../base";
import { RpcCodec, acquirePooledCorrelationId, releasePooledCorrelationId } from "./codec";
import {
  RequestOptions,
  ResponseFrame,
  RpcHandler,
  RpcSubscription,
  ResponseWriter,
  RpcStatus,
  createRpcSubscription,
} from "./types";
import {
  MSG_RPC_REQUEST,
  MSG_RPC_RESPONSE,
  MSG_RPC_ACK,
  MSG_RPC_SUBSCRIBE_WORKER,
  MSG_RPC_UNSUBSCRIBE_WORKER,
} from "../../frame/types";
import {
  ConnectionError,
  ErrCodeRpcWorkerNotFound,
  RpcError,
  TransportError,
} from "../../core/errors";
import { ConnectionState } from "../../core/types";
import { readU128BEAt, utf8Encoder } from "../../core/buffer";
import { isConcreteRouteShape } from "../_routes";
import type { Connection } from "../../client/connection";

type DecodedInboundRequest = {
  correlationId: Uint8Array;
  route: string;
  replyRoute: string;
  body: Uint8Array;
};

type ManagedResponseWriter = ResponseWriter & {
  dispose(): void;
};

function createRpcResponseWriter(
  connection: Connection,
  correlationId: Uint8Array,
): ManagedResponseWriter {
  let sequence = 0n;
  let stale = false;
  let unsubscribeDisconnect: () => void = () => undefined;

  const dispose = (): void => {
    if (stale) {
      return;
    }

    stale = true;
    unsubscribeDisconnect();
    unsubscribeDisconnect = () => undefined;
  };

  unsubscribeDisconnect = connection.onDisconnect(() => {
    dispose();
  });

  const send = async (body: Uint8Array, isEnd: boolean): Promise<void> => {
    if (stale) {
      throw new ConnectionError("RPC response writer is no longer valid");
    }

    const payload = RpcCodec.encodeResponse(correlationId, sequence++, body, isEnd);

    try {
      await connection.send(MSG_RPC_RESPONSE, payload);
      if (isEnd) {
        dispose();
      }
    } catch (error) {
      if (isBenignShutdownError(error, connection)) {
        dispose();
        return;
      }
      throw error;
    }
  };

  return {
    send,
    dispose,
  };
}

function isBenignShutdownError(error: unknown, connection: Connection): boolean {
  if (connection.getState() !== ConnectionState.Authenticated) {
    return true;
  }

  if (error instanceof ConnectionError) {
    return true;
  }

  if (!(error instanceof TransportError)) {
    return false;
  }

  return /closed|not connected|reset/i.test(error.message);
}

type RpcIterator = AsyncIterableIterator<ResponseFrame> & {
  push(frame: ResponseFrame): void;
  end(): void;
  fail(reason: unknown): void;
};

function createRpcIterator(
  cleanupPendingRpc: () => void,
  timeoutMs: number,
  signal?: AbortSignal,
): RpcIterator {
  const buffer: ResponseFrame[] = [];
  let done = false;
  let failureReason: unknown;
  let resolveNext: ((frame: ResponseFrame | null) => void) | null = null;
  let rejectNext: ((reason?: unknown) => void) | null = null;
  let abortListener: (() => void) | null = null;
  let clearPendingNext: (() => void) | null = null;

  const clearPendingWait = (): void => {
    clearPendingNext?.();
    clearPendingNext = null;
    detachAbortListener();
    resolveNext = null;
    rejectNext = null;
  };

  const push = (frame: ResponseFrame): void => {
    if (done) {
      return;
    }

    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = null;
      rejectNext = null;
      resolve(frame);
    } else {
      buffer.push(frame);
    }
  };

  const end = (): void => {
    done = true;
    if (resolveNext) {
      const resolve = resolveNext;
      clearPendingWait();
      resolve(null);
    }
  };

  const fail = (reason: unknown): void => {
    done = true;
    failureReason = reason;
    cleanupPendingRpc();
    if (rejectNext) {
      const reject = rejectNext;
      clearPendingWait();
      const rejectWithCurrentSignalState = () => {
        reject(signal?.aborted ? abortError() : reason);
      };
      if (signal) {
        setTimeout(rejectWithCurrentSignalState, 0);
      } else {
        void Promise.resolve().then(rejectWithCurrentSignalState);
      }
      return;
    }
  };

  const next = async (): Promise<IteratorResult<ResponseFrame>> => {
    if (signal?.aborted) {
      done = true;
      cleanupPendingRpc();
      throw abortError();
    }

    if (failureReason !== undefined) {
      throw failureReason;
    }

    if (buffer.length > 0) {
      const value = buffer.shift();
      if (!value) {
        return { value: undefined, done: true };
      }
      return { value, done: false };
    }

    if (done) {
      return { value: undefined, done: true };
    }

    const frame = await new Promise<ResponseFrame | null>((resolve, reject) => {
      const timer = setTimeout(() => {
        clearPendingWait();
        done = true;
        cleanupPendingRpc();
        reject(new RpcError("RPC call timeout", "TIMEOUT", RpcStatus.Timeout));
      }, timeoutMs);
      clearPendingNext = () => {
        clearTimeout(timer);
      };

      const onAbort = () => {
        clearPendingWait();
        done = true;
        cleanupPendingRpc();
        reject(abortError());
      };

      if (signal) {
        signal.addEventListener("abort", onAbort, { once: true });
        abortListener = () => {
          signal.removeEventListener("abort", onAbort);
        };
      }

      resolveNext = (f) => {
        clearPendingWait();
        resolve(f);
      };
      rejectNext = reject;
    });

    if (frame === null) {
      return { value: undefined, done: true };
    }

    if (failureReason !== undefined) {
      throw failureReason;
    }

    return { value: frame, done: false };
  };

  const returnMethod = async (): Promise<IteratorResult<ResponseFrame>> => {
    done = true;
    clearPendingWait();
    cleanupPendingRpc();
    return { value: undefined, done: true };
  };

  const detachAbortListener = (): void => {
    abortListener?.();
    abortListener = null;
  };

  const abortError = (): Error => {
    const error = new Error("The operation was aborted");
    error.name = "AbortError";
    return error;
  };

  return {
    push,
    end,
    fail,
    next,
    return: returnMethod,
    [Symbol.asyncIterator]() {
      return this;
    },
  };
}

export type RpcClient = ReturnType<typeof createRpcClient>;

type RpcClientConstructor = {
  new (connection: Connection): RpcClient;
  (connection: Connection): RpcClient;
};

export const RpcClient: RpcClientConstructor = function (connection: Connection) {
  return createRpcClient(connection);
} as unknown as RpcClientConstructor;

export function createRpcClient(connection: Connection) {
  const { requestFrame, requestReconnectFrame } = createDomainClient(connection);
  type PendingRpcEntry = { iterator: RpcIterator; correlationId: Uint8Array };
  const pendingRpcs = new Map<bigint, PendingRpcEntry>();
  const workers = new Map<string, RpcHandler>();
  let initialized = false;

  const cleanupPendingRpc = (correlationKey: bigint, correlationId: Uint8Array): void => {
    if (!pendingRpcs.has(correlationKey)) {
      return;
    }
    pendingRpcs.delete(correlationKey);
    releasePooledCorrelationId(correlationId);
  };

  connection.onDisconnect(() => {
    const pending = Array.from(pendingRpcs.values());
    for (const entry of pending) {
      if (typeof (entry.iterator as any).fail === "function") {
        (entry.iterator as any).fail(
          new ConnectionError("Connection closed while RPC response was pending"),
        );
      }
    }
    pendingRpcs.clear();
  });

  connection.onReconnect(async () => {
    if (workers.size === 0) {
      return;
    }

    const registeredWorkers = Array.from(workers.entries());
    workers.clear();
    for (const [route, handler] of registeredWorkers) {
      await registerWorkerInternal(route, handler, requestReconnectFrame);
    }
  });

  const call = async (
    route: string,
    body: Uint8Array,
    options?: RequestOptions,
  ): Promise<AsyncIterableIterator<ResponseFrame>> => {
    assertRpcRoute(route);
    initRpcHandler();

    const timeoutMs = options?.timeoutMs ?? 30000;
    const correlationId = acquirePooledCorrelationId();
    const correlationKey = correlationIdToKey(correlationId);

    const iterator = createRpcIterator(
      () => cleanupPendingRpc(correlationKey, correlationId),
      timeoutMs,
      options?.signal,
    );
    pendingRpcs.set(correlationKey, { iterator, correlationId });

    try {
      const payload = RpcCodec.encodeCallRequest(correlationId, route, body);
      const response = await requestFrame(MSG_RPC_REQUEST, payload, options?.signal);

      const decoded = RpcCodec.decodeRequestResponse(response);
      if (decoded.status !== RpcStatus.Ok) {
        cleanupPendingRpc(correlationKey, correlationId);
        throw new RpcError(
          `RPC REQUEST failed: status ${decoded.status}`,
          "REQUEST_FAILED",
          decoded.status,
        );
      }

      return iterator;
    } catch (error) {
      cleanupPendingRpc(correlationKey, correlationId);
      throw error;
    }
  };

  const registerWorkerInternal = async (
    route: string,
    handler: RpcHandler,
    request = requestFrame,
  ): Promise<void> => {
    const payload = RpcCodec.encodeSubscribeWorker(route);
    const response = await request(MSG_RPC_SUBSCRIBE_WORKER, payload);
    const decoded = RpcCodec.decodeSubscribeWorkerResponse(response);

    if (decoded.status !== RpcStatus.Ok) {
      throw new RpcError(
        `RPC SUBSCRIBE_WORKER failed: status ${decoded.status}`,
        "SUBSCRIBE_FAILED",
        decoded.status,
      );
    }

    workers.set(route, handler);
  };

  const registerWorker = async (route: string, handler: RpcHandler): Promise<RpcSubscription> => {
    assertRpcRoute(route);
    initRpcHandler();
    await registerWorkerInternal(route, handler);

    const unsubscribeFn = async (registeredRoute: string) => {
      await unregisterWorker(registeredRoute);
    };

    return createRpcSubscription(route, unsubscribeFn);
  };

  const unregisterWorker = async (route: string): Promise<void> => {
    workers.delete(route);

    try {
      const payload = RpcCodec.encodeUnsubscribeWorker(route);
      const response = await requestFrame(MSG_RPC_UNSUBSCRIBE_WORKER, payload);
      const decoded = RpcCodec.decodeUnsubscribeWorkerResponse(response);

      if (decoded.status !== RpcStatus.Ok) {
        return;
      }
    } catch {
      return;
    }
  };

  const initRpcHandler = (): void => {
    if (initialized) {
      return;
    }
    initialized = true;

    connection.registerNotificationHandler(MSG_RPC_RESPONSE, (payload: Uint8Array) => {
      try {
        const { correlationKey, sequence, body, streamEnd } = RpcCodec.decodeResponseKey(payload);
        handleRpcResponse(correlationKey, sequence, body, streamEnd);
      } catch {
        // Best-effort decode for background frames.
      }
    });

    connection.registerNotificationHandler(MSG_RPC_ACK, () => {
      // Worker ACK frames are broker-internal flow control signals. The current
      // public RPC API does not surface them.
    });

    connection.registerNotificationHandler(MSG_RPC_REQUEST, (payload: Uint8Array) => {
      try {
        const request = RpcCodec.decodeInboundRequest(payload);
        handleRpcRequest(request);
      } catch {
        // Best-effort decode for background frames.
      }
    });
  };

  const handleRpcResponse = (
    correlationKey: bigint,
    sequence: bigint,
    body: Uint8Array,
    streamEnd: boolean,
  ): void => {
    const entry = pendingRpcs.get(correlationKey);

    if (!entry) {
      return;
    }
    const iterator = entry.iterator;

    const terminalError = RpcCodec.decodeErrorBody(body);
    if (terminalError?.code === ErrCodeRpcWorkerNotFound) {
      iterator.fail(
        new RpcError(
          terminalError.message || "RPC worker not found",
          "WORKER_NOT_FOUND",
          terminalError.code,
        ),
      );
      return;
    }

    if (streamEnd) {
      if (body.length > 0) {
        iterator.push({ body, sequence });
      }
      cleanupPendingRpc(correlationKey, entry.correlationId);
      iterator.end();
    } else {
      iterator.push({ body, sequence });
    }
  };

  const handleRpcRequest = (req: DecodedInboundRequest): void => {
    const handler = workers.get(req.route);

    if (!handler) {
      return;
    }

    const writer = createRpcResponseWriter(connection, req.correlationId);

    connection.dispatchAsyncHandler(async () => {
      try {
        await handler(
          {
            route: req.route,
            replyRoute: req.replyRoute,
            body: req.body,
          },
          writer,
        );
      } catch (error) {
        if (isBenignShutdownError(error, connection)) {
          return;
        }

        const message = error instanceof Error ? error.message : "Handler error";
        try {
          await writer.send(utf8Encoder.encode(`Handler error: ${message}`), true);
        } catch {
          // Best-effort error response.
        }
      } finally {
        writer.dispose();
      }
    });
  };

  const correlationIdToKey = (correlationId: Uint8Array): bigint => {
    return readU128BEAt(correlationId, 0);
  };

  return {
    call,
    registerWorker,
  };
}

export * from "./types";

function assertRpcRoute(route: string): void {
  if (!isConcreteRouteShape(route, "rpc")) {
    throw new RpcError(
      `Invalid rpc route: ${route} (expected rpc://{realm}/{area}/{resource} or any other concrete rpc route, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}
