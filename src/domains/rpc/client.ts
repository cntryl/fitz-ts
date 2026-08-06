/**
 * RPC domain client.
 */

import { createDomainClient } from "../base";
import type {
  AsyncDispatchPort,
  DisconnectListenerPort,
  NotificationPort,
  PushClassifierPort,
  ReconnectListenerPort,
  ReconnectRestoreRequestPort,
  RequestPort,
  SendPort,
  StateReadPort,
} from "../base";
import { RpcCodec, acquirePooledCorrelationId, releasePooledCorrelationId } from "./codec";
import {
  RequestOptions,
  RegisterWorkerOptions,
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
  MSG_RPC_SUBSCRIBE_WORKER,
  MSG_RPC_UNSUBSCRIBE_WORKER,
} from "../../frame/types";
import {
  ConnectionError,
  ErrCodeRpcBackpressure,
  ErrCodeRpcBackendError,
  ErrCodeRpcCorrelationNotFound,
  ErrCodeRpcDuplicateCorrelation,
  ErrCodeRpcInvalidSequence,
  ErrCodeRpcRouteNotRegistered,
  ErrCodeRpcTimeout,
  ErrCodeRpcUnauthorized,
  ErrCodeRpcWorkerNotFound,
  ErrCodeRpcWrongWorker,
  RpcError,
  TransportError,
} from "../../core/errors";
import { ConnectionState } from "../../core/types";
import { createBufferWriter, readU128BEAt, utf8Encoder } from "../../core/buffer";
import { isConcreteRouteShape, isRegistrationPatternShape, routeMatchesPattern } from "../_routes";
import { restoreMapEntriesAtomically } from "../internal/restore";
import { parseStandardResponse } from "../../protocol/response";

type RpcConnectionPort = RequestPort &
  SendPort &
  NotificationPort &
  ReconnectListenerPort &
  DisconnectListenerPort &
  AsyncDispatchPort &
  StateReadPort &
  PushClassifierPort &
  Partial<ReconnectRestoreRequestPort>;

type DecodedInboundRequest = {
  correlationId: Uint8Array;
  route: string;
  body: Uint8Array;
};

type RegisteredWorker = {
  handler: RpcHandler;
  options: Required<RegisterWorkerOptions>;
};

type RpcPatternSpecificity = readonly [
  literalSegments: number,
  singleWildcards: number,
  doubleWildcards: number,
  segmentCount: number,
];

const DEFAULT_WORKER_MAX_CONCURRENCY = 1;
const MAX_WORKER_MAX_CONCURRENCY = 1024;

function rpcPatternSpecificity(pattern: string): RpcPatternSpecificity {
  const segments = pattern.slice(pattern.indexOf("://") + 3).split("/");
  let literalSegments = 0;
  let singleWildcards = 0;
  let doubleWildcards = 0;

  for (const segment of segments) {
    if (segment === "*") singleWildcards++;
    else if (segment === "**") doubleWildcards++;
    else literalSegments++;
  }

  return [literalSegments, singleWildcards, doubleWildcards, segments.length];
}

function isMoreSpecificRpcPattern(candidate: string, current: string): boolean {
  const candidateScore = rpcPatternSpecificity(candidate);
  const currentScore = rpcPatternSpecificity(current);

  if (candidateScore[0] !== currentScore[0]) return candidateScore[0] > currentScore[0];
  if (candidateScore[1] !== currentScore[1]) return candidateScore[1] > currentScore[1];
  if (candidateScore[2] !== currentScore[2]) return candidateScore[2] < currentScore[2];
  if (candidateScore[3] !== currentScore[3]) return candidateScore[3] > currentScore[3];
  return candidate < current;
}

type ManagedResponseWriter = ResponseWriter & {
  dispose(): void;
};

function createRpcResponseWriter(
  connection: SendPort & DisconnectListenerPort & StateReadPort,
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

function isBenignShutdownError(error: unknown, connection: StateReadPort): boolean {
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

export interface RpcClient {
  call(
    route: string,
    body: Uint8Array,
    options?: RequestOptions,
  ): Promise<AsyncIterableIterator<ResponseFrame>>;
  registerWorker(
    route: string,
    handler: RpcHandler,
    options?: RegisterWorkerOptions,
  ): Promise<RpcSubscription>;
}

export function createRpcClient(connection: RpcConnectionPort): RpcClient {
  const { requestFrame, requestReconnectFrame } = createDomainClient(connection);
  type PendingRpcEntry = { iterator: RpcIterator; correlationId: Uint8Array };
  const pendingRpcs = new Map<bigint, PendingRpcEntry>();
  const workers = new Map<string, RegisteredWorker>();
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
      entry.iterator.fail(new ConnectionError("Connection closed while RPC response was pending"));
    }
    pendingRpcs.clear();
  });

  connection.onReconnect(async () => {
    if (workers.size === 0) {
      return;
    }

    await restoreMapEntriesAtomically(workers, async (route, registration) => {
      await registerWorkerInternal(
        route,
        registration.handler,
        registration.options,
        requestReconnectFrame,
      );
      return registration;
    });
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
      await connection.send(MSG_RPC_REQUEST, payload, options?.signal);
      return iterator;
    } catch (error) {
      cleanupPendingRpc(correlationKey, correlationId);
      throw error;
    }
  };

  const registerWorkerInternal = async (
    route: string,
    handler: RpcHandler,
    options: Required<RegisterWorkerOptions>,
    request = requestFrame,
  ): Promise<void> => {
    const payload = RpcCodec.encodeSubscribeWorker(route, options.maxConcurrency);
    const parsed = parseStandardResponse(await request(MSG_RPC_SUBSCRIBE_WORKER, payload));
    if (!parsed.success) {
      throw new RpcError(
        `RPC SUBSCRIBE_WORKER failed: ${parsed.error ?? "unknown error"}`,
        "SUBSCRIBE_FAILED",
        parsed.errorCode,
      );
    }

    workers.set(route, { handler, options });
  };

  const registerWorker = async (
    route: string,
    handler: RpcHandler,
    options?: RegisterWorkerOptions,
  ): Promise<RpcSubscription> => {
    assertRpcRegistrationPattern(route);
    initRpcHandler();
    const normalizedOptions = normalizeRegisterWorkerOptions(options);
    await registerWorkerInternal(route, handler, normalizedOptions);

    const unsubscribeFn = async (registeredRoute: string) => {
      await unregisterWorker(registeredRoute);
    };

    return createRpcSubscription(route, unsubscribeFn);
  };

  const unregisterWorker = async (route: string): Promise<void> => {
    workers.delete(route);
    const payload = RpcCodec.encodeUnsubscribeWorker(route);
    const parsed = parseStandardResponse(await requestFrame(MSG_RPC_UNSUBSCRIBE_WORKER, payload));
    if (!parsed.success) {
      throw new RpcError(
        `RPC UNSUBSCRIBE_WORKER failed: ${parsed.error ?? "unknown error"}`,
        "UNSUBSCRIBE_FAILED",
        parsed.errorCode,
      );
    }
  };

  const initRpcHandler = (): void => {
    if (initialized) {
      return;
    }
    initialized = true;

    connection.registerPushFrameClassifier?.(MSG_RPC_RESPONSE, (payload) =>
      RpcCodec.isStreamResponsePayload(payload),
    );
    connection.registerPushFrameClassifier?.(MSG_RPC_REQUEST, (payload) =>
      RpcCodec.isInboundRequestPayload(payload),
    );

    connection.registerNotificationHandler(MSG_RPC_RESPONSE, (payload: Uint8Array) => {
      try {
        const { correlationKey, sequence, body, streamEnd } = RpcCodec.decodeResponseKey(payload);
        handleRpcResponse(correlationKey, sequence, body, streamEnd);
      } catch {
        // Best-effort decode for background frames.
      }
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

    const terminalError = streamEnd ? RpcCodec.tryDecodeTerminalErrorBody(body) : null;
    if (terminalError) {
      iterator.fail(
        new RpcError(
          terminalError.message || "RPC error",
          rpcErrorCodeName(terminalError.code),
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
    let registration = workers.get(req.route);

    if (!registration) {
      let selectedPattern: string | undefined;
      for (const [pattern, candidate] of workers) {
        if (
          routeMatchesPattern(req.route, pattern) &&
          (selectedPattern === undefined || isMoreSpecificRpcPattern(pattern, selectedPattern))
        ) {
          selectedPattern = pattern;
          registration = candidate;
        }
      }
    }

    if (!registration) {
      return;
    }

    const writer = createRpcResponseWriter(connection, req.correlationId);

    const accepted = tryDispatchRpcHandler(async () => {
      try {
        await registration.handler(
          {
            route: req.route,
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

    if (!accepted) {
      void sendBackpressureResponse(writer);
    }
  };

  const tryDispatchRpcHandler = (task: () => void | Promise<void>): boolean => {
    if (typeof connection.tryDispatchAsyncHandler === "function") {
      return connection.tryDispatchAsyncHandler(task);
    }

    connection.dispatchAsyncHandler(task);
    return true;
  };

  const sendBackpressureResponse = async (writer: ManagedResponseWriter): Promise<void> => {
    try {
      await writer.send(
        encodeRpcErrorBody(ErrCodeRpcBackpressure, "Local RPC worker is overloaded"),
        true,
      );
    } catch {
      // Best-effort overload response.
    } finally {
      writer.dispose();
    }
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

function encodeRpcErrorBody(code: number, message: string): Uint8Array {
  const writer = createBufferWriter(64);
  writer.writeU8(1);
  writer.writeU32BE(code);
  writer.writeString(message);
  return writer.getBuffer();
}

function assertRpcRoute(route: string): void {
  if (!isConcreteRouteShape(route, "rpc")) {
    throw new RpcError(
      `Invalid rpc route: ${route} (expected rpc://{realm}/{area}/{resource} or any other concrete rpc route, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}

function assertRpcRegistrationPattern(pattern: string): void {
  if (!isRegistrationPatternShape(pattern, "rpc")) {
    throw new RpcError(
      `Invalid rpc worker pattern: ${pattern} (wildcards must be whole * or ** segments)`,
      "INVALID_ROUTE",
    );
  }
}

function normalizeRegisterWorkerOptions(
  options: RegisterWorkerOptions | undefined,
): Required<RegisterWorkerOptions> {
  const maxConcurrency = options?.maxConcurrency ?? DEFAULT_WORKER_MAX_CONCURRENCY;

  if (
    !Number.isInteger(maxConcurrency) ||
    maxConcurrency < 1 ||
    maxConcurrency > MAX_WORKER_MAX_CONCURRENCY
  ) {
    throw new RpcError(
      `Invalid rpc worker maxConcurrency: ${maxConcurrency} (expected integer in 1..=${MAX_WORKER_MAX_CONCURRENCY})`,
      "INVALID_OPTIONS",
    );
  }

  return { maxConcurrency };
}

function rpcErrorCodeName(domainCode: number): string {
  switch (domainCode) {
    case ErrCodeRpcTimeout:
      return "TIMEOUT";
    case ErrCodeRpcWorkerNotFound:
      return "WORKER_NOT_FOUND";
    case ErrCodeRpcBackpressure:
      return "BACKPRESSURE";
    case ErrCodeRpcRouteNotRegistered:
      return "ROUTE_NOT_REGISTERED";
    case ErrCodeRpcCorrelationNotFound:
      return "CORRELATION_NOT_FOUND";
    case ErrCodeRpcDuplicateCorrelation:
      return "DUPLICATE_CORRELATION";
    case ErrCodeRpcInvalidSequence:
      return "INVALID_SEQUENCE";
    case ErrCodeRpcWrongWorker:
      return "WRONG_WORKER";
    case ErrCodeRpcUnauthorized:
      return "UNAUTHORIZED";
    case ErrCodeRpcBackendError:
      return "BACKEND_ERROR";
    default:
      return "DOMAIN_ERROR";
  }
}
