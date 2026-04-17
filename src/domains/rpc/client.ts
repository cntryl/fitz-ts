/**
 * RPC domain client.
 */

import { DomainClient } from "../base";
import { RpcCodec } from "./codec";
import {
  RequestOptions,
  ResponseFrame,
  InboundRequest,
  RpcHandler,
  RpcSubscription,
  ResponseWriter,
  RpcStatus,
} from "./types";
import {
  MSG_RPC_REQUEST,
  MSG_RPC_RESPONSE,
  MSG_RPC_ACK,
  MSG_RPC_SUBSCRIBE_WORKER,
  MSG_RPC_UNSUBSCRIBE_WORKER,
} from "../../frame/types";
import { ConnectionError, RpcError, TransportError } from "../../core/errors";
import { ConnectionState } from "../../core/types";
import { utf8Encoder } from "../../core/buffer";
import { isConcreteRouteShape } from "../_routes";

/**
 * `ResponseWriter` implementation used by worker handlers.
 */
class RpcResponseWriter implements ResponseWriter {
  private sequence = 0n;

  constructor(
    private readonly connection: import("../../client/connection").Connection,
    private readonly correlationId: Uint8Array,
  ) {}

  async send(body: Uint8Array, isEnd: boolean): Promise<void> {
    const payload = RpcCodec.encodeResponse(this.correlationId, this.sequence++, body, isEnd);

    try {
      await this.connection.send(MSG_RPC_RESPONSE, payload);
    } catch (error) {
      if (isBenignShutdownError(error, this.connection)) {
        return;
      }
      throw error;
    }
  }
}

function isBenignShutdownError(
  error: unknown,
  connection: import("../../client/connection").Connection,
): boolean {
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
 * Async iterator for streaming RPC responses.
 */
class RpcIterator implements AsyncIterableIterator<ResponseFrame> {
  private buffer: ResponseFrame[] = [];
  private done = false;
  private resolveNext: ((frame: ResponseFrame | null) => void) | null = null;
  private rejectNext: ((reason?: unknown) => void) | null = null;
  private abortListener: (() => void) | null = null;

  constructor(
    private readonly correlationId: Uint8Array,
    private readonly client: RpcClient,
    private readonly timeoutMs: number,
    private readonly signal?: AbortSignal,
  ) {}

  /**
   * Push a response frame to the iterator.
   */
  push(frame: ResponseFrame): void {
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      resolve(frame);
    } else {
      this.buffer.push(frame);
    }
  }

  /**
   * Mark the iterator as done (end of stream).
   */
  end(): void {
    this.done = true;
    this.detachAbortListener();
    if (this.resolveNext) {
      const resolve = this.resolveNext;
      this.resolveNext = null;
      this.rejectNext = null;
      resolve(null);
    }
  }

  fail(reason: unknown): void {
    this.done = true;
    this.detachAbortListener();
    if (this.rejectNext) {
      const reject = this.rejectNext;
      this.resolveNext = null;
      this.rejectNext = null;
      reject(reason);
      return;
    }
    this.client.cleanupPendingRpc(this.correlationId);
  }

  async next(): Promise<IteratorResult<ResponseFrame>> {
    if (this.buffer.length > 0) {
      const value = this.buffer.shift();
      if (!value) {
        return { value: undefined, done: true };
      }
      return { value, done: false };
    }

    if (this.done) {
      return { value: undefined, done: true };
    }

    if (this.signal?.aborted) {
      this.done = true;
      this.client.cleanupPendingRpc(this.correlationId);
      throw this.abortError();
    }

    const frame = await new Promise<ResponseFrame | null>((resolve, reject) => {
      this.resolveNext = resolve;
      this.rejectNext = reject;

      const timer = setTimeout(() => {
        this.resolveNext = null;
        this.rejectNext = null;
        this.done = true;
        this.detachAbortListener();
        this.client.cleanupPendingRpc(this.correlationId);
        reject(new RpcError("RPC call timeout", "TIMEOUT", RpcStatus.Timeout));
      }, this.timeoutMs);

      if (this.signal) {
        const onAbort = () => {
          clearTimeout(timer);
          this.resolveNext = null;
          this.rejectNext = null;
          this.done = true;
          this.client.cleanupPendingRpc(this.correlationId);
          reject(this.abortError());
        };
        this.signal.addEventListener("abort", onAbort, { once: true });
        this.abortListener = () => {
          this.signal?.removeEventListener("abort", onAbort);
        };
      }

      const originalResolve = this.resolveNext;
      this.resolveNext = (f) => {
        clearTimeout(timer);
        this.detachAbortListener();
        originalResolve?.(f);
      };
    });

    if (frame === null) {
      return { value: undefined, done: true };
    }

    return { value: frame, done: false };
  }

  async return(): Promise<IteratorResult<ResponseFrame>> {
    this.done = true;
    this.detachAbortListener();
    this.client.cleanupPendingRpc(this.correlationId);
    return { value: undefined, done: true };
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<ResponseFrame> {
    return this;
  }

  private detachAbortListener(): void {
    this.abortListener?.();
    this.abortListener = null;
  }

  private abortError(): Error {
    const error = new Error("The operation was aborted");
    error.name = "AbortError";
    return error;
  }
}

/**
 * RPC client with streaming call support and worker registration.
 */
export class RpcClient extends DomainClient {
  private pendingRpcs: Map<string, RpcIterator> = new Map();
  private workers: Map<string, RpcHandler> = new Map();
  private initialized = false;

  constructor(connection: import("../../client/connection").Connection) {
    super(connection);
    this.connection.onDisconnect(() => {
      const pending = Array.from(this.pendingRpcs.values());
      this.pendingRpcs.clear();
      for (const iterator of pending) {
        iterator.fail(new ConnectionError("Connection closed while RPC response was pending"));
      }
    });

    this.connection.onReconnect(async () => {
      if (this.workers.size === 0) {
        return;
      }

      const workers = Array.from(this.workers.entries());
      this.workers.clear();
      for (const [route, handler] of workers) {
        await this.registerWorker(route, handler);
      }
    });
  }

  /**
   * Send a remote procedure call and return an async iterator of response frames.
   * @param route RPC route (e.g., "rpc://realm/area/method")
   * @param body Request body
   * @param options Call options such as timeout and cancellation.
   * @returns AsyncIterableIterator over response frames
   */
  async call(
    route: string,
    body: Uint8Array,
    options?: RequestOptions,
  ): Promise<AsyncIterableIterator<ResponseFrame>> {
    assertRpcRoute(route);
    this.initRpcHandler();

    const timeoutMs = options?.timeoutMs ?? 30000;
    const correlationId = RpcCodec.generateCorrelationId();
    const correlationKey = this.correlationIdToKey(correlationId);

    const iterator = new RpcIterator(correlationId, this, timeoutMs, options?.signal);
    this.pendingRpcs.set(correlationKey, iterator);
    try {
      const payload = RpcCodec.encodeRequest(correlationId, route, "", body);
      const response = await this.requestFrame(MSG_RPC_REQUEST, payload, options?.signal);

      const decoded = RpcCodec.decodeRequestResponse(response);
      if (decoded.status !== RpcStatus.Ok) {
        this.pendingRpcs.delete(correlationKey);
        throw new RpcError(
          `RPC REQUEST failed: status ${decoded.status}`,
          "REQUEST_FAILED",
          decoded.status,
        );
      }

      return iterator;
    } catch (error) {
      this.pendingRpcs.delete(correlationKey);
      throw error;
    }
  }

  /**
   * Register as a worker to handle incoming RPC requests.
   * @param route Worker route pattern
   * @param handler Handler function to process requests
   * @returns Worker registration with `unsubscribe()`.
   */
  async registerWorker(route: string, handler: RpcHandler): Promise<RpcSubscription> {
    assertRpcRoute(route);
    this.initRpcHandler();
    const payload = RpcCodec.encodeSubscribeWorker(route);
    const response = await this.requestFrame(MSG_RPC_SUBSCRIBE_WORKER, payload);
    const decoded = RpcCodec.decodeSubscribeWorkerResponse(response);

    if (decoded.status !== RpcStatus.Ok) {
      throw new RpcError(
        `RPC SUBSCRIBE_WORKER failed: status ${decoded.status}`,
        "SUBSCRIBE_FAILED",
        decoded.status,
      );
    }

    this.workers.set(route, handler);

    const unsubscribeFn = async (registeredRoute: string) => {
      await this.unregisterWorker(registeredRoute);
    };

    return new RpcSubscription(route, unsubscribeFn);
  }

  /**
   * Remove a worker registration from the current connection.
   */
  private async unregisterWorker(route: string): Promise<void> {
    this.workers.delete(route);

    try {
      const payload = RpcCodec.encodeUnsubscribeWorker(route);
      const response = await this.requestFrame(MSG_RPC_UNSUBSCRIBE_WORKER, payload);
      const decoded = RpcCodec.decodeUnsubscribeWorkerResponse(response);

      if (decoded.status !== RpcStatus.Ok) {
        return;
      }
    } catch {
      return;
    }
  }

  /**
   * Clean up a pending RPC when an iterator is closed or canceled.
   */
  cleanupPendingRpc(correlationId: Uint8Array): void {
    const key = this.correlationIdToKey(correlationId);
    this.pendingRpcs.delete(key);
  }

  /**
   * Initialize RPC handlers lazily on first use.
   */
  private initRpcHandler(): void {
    if (this.initialized) {
      return;
    }
    this.initialized = true;

    this.connection.registerNotificationHandler(MSG_RPC_RESPONSE, (payload: Uint8Array) => {
      try {
        const { correlationId, sequence, body, streamEnd } = RpcCodec.decodeResponse(payload);
        this.handleRpcResponse(correlationId, sequence, body, streamEnd);
      } catch {
        // Best-effort decode for background frames.
      }
    });

    this.connection.registerNotificationHandler(MSG_RPC_ACK, () => {
      // Worker ACK frames are broker-internal flow control signals. The current
      // public RPC API does not surface them.
    });

    this.connection.registerNotificationHandler(MSG_RPC_REQUEST, (payload: Uint8Array) => {
      try {
        const request = RpcCodec.decodeInboundRequest(payload);
        this.handleRpcRequest(request);
      } catch {
        // Best-effort decode for background frames.
      }
    });
  }

  /**
   * Handle an incoming `RPC_RESPONSE` frame.
   */
  private handleRpcResponse(
    correlationId: Uint8Array,
    sequence: bigint,
    body: Uint8Array,
    streamEnd: boolean,
  ): void {
    const key = this.correlationIdToKey(correlationId);
    const iterator = this.pendingRpcs.get(key);

    if (!iterator) {
      return;
    }

    const terminalError = RpcCodec.decodeErrorBody(body);
    if (terminalError?.code === 6002) {
      this.pendingRpcs.delete(key);
      return;
    }

    if (streamEnd) {
      if (body.length > 0) {
        iterator.push({ body, sequence });
      }
      this.pendingRpcs.delete(key);
      iterator.end();
    } else {
      iterator.push({ body, sequence });
    }
  }

  /**
   * Handle an incoming `RPC_REQUEST` frame for worker mode.
   */
  private handleRpcRequest(req: InboundRequest): void {
    const handler = this.workers.get(req.route);

    if (!handler) {
      return;
    }

    const writer = new RpcResponseWriter(this.connection, req.correlationId);

    this.connection.dispatchAsyncHandler(async () => {
      try {
        await handler(req, writer);
      } catch (error) {
        if (isBenignShutdownError(error, this.connection)) {
          return;
        }

        const message = error instanceof Error ? error.message : "Handler error";
        try {
          await writer.send(utf8Encoder.encode(`Handler error: ${message}`), true);
        } catch {
          // Best-effort error response.
        }
      }
    });
  }

  /**
   * Convert a correlation ID into a stable string key for the pending map.
   */
  private correlationIdToKey(correlationId: Uint8Array): string {
    return Array.from(correlationId)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }
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
