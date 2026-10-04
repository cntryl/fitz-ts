import { createRpcIterator, type RpcIterator } from "./iterator";
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
  ServerCapabilitiesPort,
  SendPort,
  StateReadPort,
} from "../base";
import {
  MAX_RPC_BUDGET_MS,
  RpcCodec,
  acquirePooledCorrelationId,
  releasePooledCorrelationId,
} from "./codec";
import {
  RequestOptions,
  RegisterWorkerOptions,
  RpcCallIterator,
  RpcCancellationOutcome,
  RpcHandlerContext,
  RpcHandler,
  RpcSubscription,
  ResponseWriter,
  createRpcSubscription,
} from "./types";
import {
  CAP_RPC_CANCELLATION,
  MSG_RPC_REQUEST,
  MSG_RPC_RESPONSE,
  MSG_RPC_CANCELLATION,
  MSG_RPC_LIFECYCLE,
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
import { createBufferWriter, readU128BEAt } from "../../core/buffer";
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
  ServerCapabilitiesPort &
  PushClassifierPort &
  Partial<ReconnectRestoreRequestPort>;

type DecodedInboundRequest = {
  correlationId: Uint8Array;
  route: string;
  body: Uint8Array;
  remainingBudgetMs?: number;
};

type ActiveRpcInvocation = {
  controller: AbortController;
  deadlineAt?: number;
  timer?: ReturnType<typeof setTimeout>;
  cancellationRequested: boolean;
  started: boolean;
};

type PendingCancellation = {
  correlationId: Uint8Array;
  resolve: (outcome: RpcCancellationOutcome) => void;
  timeout: ReturnType<typeof setTimeout>;
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
const DEFAULT_RPC_TIMEOUT_MS = 30000;
const CANCELLATION_RESULT_TIMEOUT_MS = 5000;

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
  // False when the writer became unusable for a reason the handler couldn't
  // have avoided (the connection dropped, or a send failed for a benign
  // shutdown reason) — only a handler that settles with the writer still
  // live and never sent isEnd:true warrants the L11 warning below.
  needsTerminalWarning(): boolean;
};

function createRpcResponseWriter(
  connection: SendPort & DisconnectListenerPort & StateReadPort,
  correlationId: Uint8Array,
): ManagedResponseWriter {
  let sequence = 0n;
  let stale = false;
  let ended = false;
  let benignlyDisposed = false;
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
    benignlyDisposed = true;
    dispose();
  });

  const send = async (body: Uint8Array, isEnd: boolean, signal?: AbortSignal): Promise<void> => {
    if (stale) {
      throw new ConnectionError("RPC response writer is no longer valid");
    }

    const payload = RpcCodec.encodeResponse(correlationId, sequence++, body, isEnd);

    try {
      await connection.send(MSG_RPC_RESPONSE, payload, signal);
      if (isEnd) {
        ended = true;
        dispose();
      }
    } catch (error) {
      if (isBenignShutdownError(error, connection)) {
        benignlyDisposed = true;
        dispose();
        return;
      }
      throw error;
    }
  };

  return {
    write: (options) => send(options.body, false, options.signal),
    end: (options = {}) => send(options.body ?? new Uint8Array(), true, options.signal),
    dispose,
    needsTerminalWarning: () => !ended && !benignlyDisposed,
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

/**
 * Streaming RPC facade for callers and workers. Routes start with `rpc://`
 * followed by one or more non-empty segments. Calls require a concrete route;
 * worker registrations may use whole-segment `*` and `**` patterns.
 */
export interface RpcClient {
  /**
   * Starts an RPC call and returns its ordered response stream. Consume until
   * completion or call `return()`/break iteration to release local state.
   * Do not automatically replay after a request may have reached a worker.
   */
  call(route: string, options: RequestOptions): RpcCallIterator;
  /**
   * Registers one handler for a route pattern. Use a concrete route for one
   * endpoint, or whole-segment `*` and `**` wildcards to match multiple routes.
   * Keep and dispose the returned subscription; registrations are restored
   * automatically after reconnect.
   */
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
  const pendingCancellations = new Map<bigint, PendingCancellation>();
  const abandonedCorrelationIds = new Set<bigint>();
  const activeInvocations = new Map<bigint, ActiveRpcInvocation>();
  const workers = new Map<string, RegisteredWorker>();
  const workerMutationTails = new Map<string, Promise<void>>();
  let initialized = false;

  const withWorkerMutation = async <T>(route: string, task: () => Promise<T>): Promise<T> => {
    const previous = workerMutationTails.get(route) ?? Promise.resolve();
    let release: () => void = () => undefined;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    workerMutationTails.set(route, current);

    await previous;
    try {
      return await task();
    } finally {
      release();
      if (workerMutationTails.get(route) === current) {
        workerMutationTails.delete(route);
      }
    }
  };

  const cleanupPendingRpc = (correlationKey: bigint, correlationId: Uint8Array): void => {
    if (!pendingRpcs.has(correlationKey)) {
      return;
    }
    pendingRpcs.delete(correlationKey);
    if (abandonedCorrelationIds.delete(correlationKey)) {
      return;
    }
    releasePooledCorrelationId(correlationId);
  };

  connection.onDisconnect(() => {
    const pending = Array.from(pendingRpcs.values());
    for (const entry of pending) {
      entry.iterator.fail(new ConnectionError("Connection closed while RPC response was pending"));
    }
    pendingRpcs.clear();
    abandonedCorrelationIds.clear();
    for (const [key, pending] of pendingCancellations) {
      clearTimeout(pending.timeout);
      pending.resolve("connection_closed");
      pendingCancellations.delete(key);
    }
    for (const invocation of activeInvocations.values()) {
      clearTimeout(invocation.timer);
      invocation.controller.abort(new ConnectionError("Connection closed during RPC handler"));
    }
    activeInvocations.clear();
  });

  connection.onReconnect(async () => {
    if (workers.size === 0) {
      return;
    }

    await restoreMapEntriesAtomically(
      workers,
      async (route, registration) => {
        await registerWorkerInternal(
          route,
          registration.handler,
          registration.options,
          requestReconnectFrame,
        );
        return registration;
      },
      async (route) => {
        parseStandardResponse(
          await requestReconnectFrame(
            MSG_RPC_UNSUBSCRIBE_WORKER,
            RpcCodec.encodeUnsubscribeWorker(route),
          ),
        );
      },
    );
  });

  const call = (route: string, options: RequestOptions): RpcCallIterator => {
    assertRpcRoute(route);
    initRpcHandler();
    const timeoutMs = options.timeoutMs ?? DEFAULT_RPC_TIMEOUT_MS;
    validateCallTimeout(timeoutMs);
    const deadlineAt = performance.now() + timeoutMs;
    const supportsCancellation = hasRpcCancellationCapability(connection);
    const correlationId = acquirePooledCorrelationId();
    const correlationKey = correlationIdToKey(correlationId);
    let settleRequestSent!: (sent: boolean) => void;
    const requestSent = new Promise<boolean>((resolve) => {
      settleRequestSent = resolve;
    });
    const iterator = createRpcIterator(
      deadlineAt,
      async (reason) => {
        abandonedCorrelationIds.add(correlationKey);
        const sent = await requestSent;
        if (!sent) {
          cleanupPendingRpc(correlationKey, correlationId);
          return "request_not_sent";
        }
        if (!supportsCancellation) {
          cleanupPendingRpc(correlationKey, correlationId);
          return "unsupported";
        }
        return await sendCancellationRequest(correlationKey, correlationId, reason);
      },
      () => cleanupPendingRpc(correlationKey, correlationId),
      options.signal,
    );
    pendingRpcs.set(correlationKey, { iterator, correlationId });

    if (options.signal?.aborted) {
      const payload = RpcCodec.encodeCallRequest(correlationId, route, options.body);
      void connection.send(MSG_RPC_REQUEST, payload, options.signal).catch(() => undefined);
      settleRequestSent(false);
      return iterator;
    }

    const remainingBudgetMs = supportsCancellation
      ? Math.max(0, Math.floor(deadlineAt - performance.now()))
      : undefined;
    const payload = RpcCodec.encodeCallRequest(
      correlationId,
      route,
      options.body,
      remainingBudgetMs,
    );
    void connection.send(MSG_RPC_REQUEST, payload).then(
      () => settleRequestSent(true),
      (error: unknown) => {
        settleRequestSent(false);
        iterator.fail(error);
      },
    );
    return iterator;
  };

  const sendCancellationRequest = async (
    correlationKey: bigint,
    correlationId: Uint8Array,
    reason: 1 | 2,
  ): Promise<RpcCancellationOutcome> => {
    let resolveOutcome!: (outcome: RpcCancellationOutcome) => void;
    const outcome = new Promise<RpcCancellationOutcome>((resolve) => {
      resolveOutcome = resolve;
    });
    const timeout = setTimeout(
      () => settleCancellationResult(correlationKey, "unconfirmed"),
      CANCELLATION_RESULT_TIMEOUT_MS,
    );
    pendingCancellations.set(correlationKey, {
      correlationId,
      resolve: resolveOutcome,
      timeout,
    });
    try {
      await connection.send(
        MSG_RPC_CANCELLATION,
        RpcCodec.encodeCallerCancellation(correlationId, reason),
      );
    } catch {
      settleCancellationResult(
        correlationKey,
        connection.getState() === ConnectionState.Closed ? "connection_closed" : "unconfirmed",
      );
    }
    return await outcome;
  };

  const settleCancellationResult = (
    correlationKey: bigint,
    result: RpcCancellationOutcome,
  ): void => {
    const pending = pendingCancellations.get(correlationKey);
    if (!pending) {
      return;
    }
    pendingCancellations.delete(correlationKey);
    clearTimeout(pending.timeout);
    pending.resolve(result);
    cleanupPendingRpc(correlationKey, pending.correlationId);
  };

  const registerWorkerInternal = async (
    route: string,
    handler: RpcHandler,
    options: Required<RegisterWorkerOptions>,
    request = requestFrame,
  ): Promise<RegisteredWorker> => {
    const payload = RpcCodec.encodeSubscribeWorker(
      route,
      options.maxConcurrency,
      hasRpcCancellationCapability(connection),
    );
    const parsed = parseStandardResponse(await request(MSG_RPC_SUBSCRIBE_WORKER, payload));
    if (!parsed.success) {
      throw new RpcError(
        `RPC SUBSCRIBE_WORKER failed: ${parsed.error ?? "unknown error"}`,
        "SUBSCRIBE_FAILED",
        parsed.errorCode,
      );
    }

    const registration = { handler, options };
    workers.set(route, registration);
    return registration;
  };

  const registerWorker = async (
    route: string,
    handler: RpcHandler,
    options?: RegisterWorkerOptions,
  ): Promise<RpcSubscription> => {
    assertRpcRegistrationPattern(route);
    initRpcHandler();
    const normalizedOptions = normalizeRegisterWorkerOptions(options);
    const registration = await withWorkerMutation(route, async () => {
      return await registerWorkerInternal(route, handler, normalizedOptions);
    });

    return createRpcSubscription(async () => unregisterWorker(route, registration));
  };

  const unregisterWorker = async (route: string, registration: RegisteredWorker): Promise<void> => {
    await withWorkerMutation(route, async () => {
      // A handle superseded by a newer registration no longer owns the wire
      // route. Sending UNSUBSCRIBE_WORKER from that stale handle would remove
      // the replacement on the broker while leaving it present locally.
      if (workers.get(route) !== registration) {
        return;
      }

      // Confirm the wire UNSUBSCRIBE_WORKER before dropping local tracking —
      // deleting first would permanently orphan the worker locally when the
      // wire request fails even though the broker may still consider it live.
      const payload = RpcCodec.encodeUnsubscribeWorker(route);
      const parsed = parseStandardResponse(await requestFrame(MSG_RPC_UNSUBSCRIBE_WORKER, payload));
      if (!parsed.success) {
        throw new RpcError(
          `RPC UNSUBSCRIBE_WORKER failed: ${parsed.error ?? "unknown error"}`,
          "UNSUBSCRIBE_FAILED",
          parsed.errorCode,
        );
      }
      workers.delete(route);
    });
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

    connection.registerNotificationHandler(MSG_RPC_LIFECYCLE, (payload: Uint8Array) => {
      try {
        if (!RpcCodec.isLifecycleControlPayload(payload)) {
          return;
        }
        const control = RpcCodec.decodeLifecycleControl(payload);
        const correlationKey = correlationIdToKey(control.correlationId);
        if (control.kind === "worker_cancellation") {
          const invocation = activeInvocations.get(correlationKey);
          if (invocation) {
            invocation.cancellationRequested = true;
            clearTimeout(invocation.timer);
            invocation.controller.abort(new RpcError("RPC call was cancelled", "CANCELLED"));
            if (!invocation.started) {
              activeInvocations.delete(correlationKey);
              void connection
                .send(MSG_RPC_CANCELLATION, RpcCodec.encodeWorkerCleanupAck(control.correlationId))
                .catch(() => undefined);
            }
          }
          return;
        }
        const results: Record<1 | 2 | 3 | 4 | 5 | 6, RpcCancellationOutcome> = {
          1: "queued_removed",
          2: "forwarded",
          3: "worker_unsupported",
          4: "already_terminal",
          5: "unknown_or_unauthorized",
          6: "forwarding_failed",
        };
        settleCancellationResult(correlationKey, results[control.status]);
      } catch {
        // Best-effort lifecycle control dispatch.
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
    const correlationKey = correlationIdToKey(req.correlationId);
    const deadlineAt =
      req.remainingBudgetMs === undefined ? undefined : performance.now() + req.remainingBudgetMs;
    const invocation: ActiveRpcInvocation = {
      controller: new AbortController(),
      deadlineAt,
      cancellationRequested: false,
      started: false,
    };
    if (deadlineAt !== undefined) {
      invocation.timer = setTimeout(
        () => {
          invocation.controller.abort(
            new RpcError("RPC request deadline elapsed", "TIMEOUT", ErrCodeRpcTimeout),
          );
        },
        Math.max(0, deadlineAt - performance.now()),
      );
    }
    activeInvocations.set(correlationKey, invocation);

    const accepted = tryDispatchRpcHandler(async () => {
      try {
        invocation.started = true;
        if (invocation.cancellationRequested) return;
        await registration.handler(
          {
            route: req.route,
            body: req.body,
          },
          writer,
          createHandlerContext(invocation),
        );
      } catch (error) {
        if (isBenignShutdownError(error, connection)) {
          return;
        }

        const message = error instanceof Error ? error.message : "Handler error";
        try {
          await writer.end({
            body: encodeRpcErrorBody(ErrCodeRpcBackendError, message.slice(0, 512)),
          });
        } catch {
          // Best-effort error response.
        }
      } finally {
        clearTimeout(invocation.timer);
        activeInvocations.delete(correlationKey);
        // A handler that settles without ever sending a terminal frame
        // leaves the caller's iterator waiting until it hits the generic
        // call timeout, with nothing pointing at the actual cause — warn
        // here, with the route, so it's diagnosable instead of mysterious.
        if (writer.needsTerminalWarning() && !invocation.controller.signal.aborted) {
          console.warn(
            `[fitz] RPC worker handler for route "${req.route}" completed without sending a terminal response (isEnd: true); the caller will hang until the call times out.`,
          );
        }
        writer.dispose();
        // The broker may have ordered cancellation before a terminal response
        // while its notification is still in transit. Always acknowledge cleanup
        // when negotiated so that race cannot strand execution credit.
        if (hasRpcCancellationCapability(connection)) {
          try {
            await connection.send(
              MSG_RPC_CANCELLATION,
              RpcCodec.encodeWorkerCleanupAck(req.correlationId),
            );
          } catch {
            // The broker will reclaim this worker's credit when its session closes.
          }
        }
      }
    });

    if (!accepted) {
      clearTimeout(invocation.timer);
      activeInvocations.delete(correlationKey);
      void sendBackpressureResponse(writer, req.correlationId);
    }
  };

  const tryDispatchRpcHandler = (task: () => void | Promise<void>): boolean => {
    if (typeof connection.tryDispatchAsyncHandler === "function") {
      return connection.tryDispatchAsyncHandler(task);
    }

    connection.dispatchAsyncHandler(task);
    return true;
  };

  const sendBackpressureResponse = async (
    writer: ManagedResponseWriter,
    id: Uint8Array,
  ): Promise<void> => {
    try {
      await writer.end({
        body: encodeRpcErrorBody(ErrCodeRpcBackpressure, "Local RPC worker is overloaded"),
      });
    } catch {
      // Best-effort overload response.
    } finally {
      writer.dispose();
      if (hasRpcCancellationCapability(connection)) {
        await connection
          .send(MSG_RPC_CANCELLATION, RpcCodec.encodeWorkerCleanupAck(id))
          .catch(() => undefined);
      }
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

function hasRpcCancellationCapability(connection: ServerCapabilitiesPort): boolean {
  return ((connection.getServerCapabilities?.().capabilities ?? 0) & CAP_RPC_CANCELLATION) !== 0;
}

function validateCallTimeout(timeoutMs: number): void {
  if (!Number.isInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > MAX_RPC_BUDGET_MS) {
    throw new RpcError(
      `Invalid rpc timeoutMs: ${timeoutMs} (expected integer in 0..=${MAX_RPC_BUDGET_MS})`,
      "INVALID_OPTIONS",
    );
  }
}

function createHandlerContext(invocation: ActiveRpcInvocation): RpcHandlerContext {
  return {
    signal: invocation.controller.signal,
    remainingTimeMs: () =>
      invocation.deadlineAt === undefined
        ? undefined
        : Math.max(0, Math.floor(invocation.deadlineAt - performance.now())),
  };
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
