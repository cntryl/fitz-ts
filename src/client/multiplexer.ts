/**
 * Multiplexer for correlating in-flight requests with responses
 * Per CLIENT_SPEC.md and fitz-go/internal/core/connection/mux.go
 *
 * Key Pattern: FIFO ordering per MessageType
 * - Responses matched to requests in order received per MessageType
 * - Matches server's sequential processing model per actor/route
 * - No correlation IDs for most operations (except RPC streaming)
 */

import { Deferred, ConnectionState } from "../core/types";
import { TimeoutError } from "../core/errors";

export interface PendingRequest {
  deferred: Deferred<Uint8Array>;
  timeout: ReturnType<typeof setTimeout>;
  sentAt: Date;
}

/**
 * Handler for server-pushed frames.
 *
 * Most pushed traffic uses dedicated `*_NOTIFY` message types. RPC is the
 * protocol exception: inbound worker requests and streaming responses arrive on
 * `MSG_RPC_REQUEST` and `MSG_RPC_RESPONSE`, so those frames are only treated as
 * pushes when there is no matching FIFO request waiting on the same type.
 */
export type NotificationHandler = (payload: Uint8Array) => void;

/**
 * RPC correlation handler for streamed responses
 * @param correlationId 16-byte correlation ID
 * @param payload Response payload
 */
export type RpcCorrelationHandler = (
  correlationId: Uint8Array,
  payload: Uint8Array,
) => void;

export class Multiplexer {
  // FIFO queue of pending requests per MessageType
  private pending: Map<number, PendingRequest[]> = new Map();

  // Handlers for pushed frames, including the RPC same-type exception.
  private notificationHandlers: Map<number, NotificationHandler> = new Map();
  private optionalResponses: Map<number, number> = new Map();

  // RPC correlation handler for streaming responses (future use)
  // private rpcCorrelationHandler?: RpcCorrelationHandler;

  private state: ConnectionState = ConnectionState.Disconnected;

  // Metrics
  private requestsInFlight = 0;
  private requestsTotal = 0;
  private responsesTotal = 0;
  private responsesDropped = 0;
  private responsesIgnored = 0;

  private getOrCreatePendingQueue(messageType: number): PendingRequest[] {
    const existing = this.pending.get(messageType);
    if (existing) {
      return existing;
    }

    const created: PendingRequest[] = [];
    this.pending.set(messageType, created);
    return created;
  }

  setConnected(): void {
    this.state = ConnectionState.Authenticated;
  }

  setDisconnected(): void {
    this.state = ConnectionState.Disconnected;
    this.optionalResponses.clear();
    this.cancelAll();
  }

  /**
   * Register a handler for pushed frames.
   * @param handler Handler function to call when notification arrives
   */
  registerNotificationHandler(
    messageType: number,
    handler: NotificationHandler,
  ): void {
    this.notificationHandlers.set(messageType, handler);
  }

  /**
   * Unregister notification handler
   */
  unregisterNotificationHandler(messageType: number): void {
    this.notificationHandlers.delete(messageType);
  }

  expectOptionalResponse(messageType: number): () => void {
    const nextCount = (this.optionalResponses.get(messageType) ?? 0) + 1;
    this.optionalResponses.set(messageType, nextCount);

    return () => {
      const currentCount = this.optionalResponses.get(messageType) ?? 0;
      if (currentCount <= 1) {
        this.optionalResponses.delete(messageType);
        return;
      }
      this.optionalResponses.set(messageType, currentCount - 1);
    };
  }

  /**
   * Send a request and wait for the response (FIFO matching)
   */
  async request(
    messageType: number,
    frameData: Uint8Array,
    send: (data: Uint8Array) => Promise<void>,
    timeoutMs: number,
  ): Promise<Uint8Array> {
    const deferred = new Deferred<Uint8Array>();

    const timeout = setTimeout(() => {
      this.unregisterRequest(messageType, deferred);
      deferred.reject(
        new TimeoutError(
          `Request timeout for message type ${messageType} after ${timeoutMs}ms`,
        ),
      );
    }, timeoutMs);

    const request: PendingRequest = {
      deferred,
      timeout,
      sentAt: new Date(),
    };

    // Add to FIFO queue for this MessageType
    this.getOrCreatePendingQueue(messageType).push(request);

    this.requestsInFlight++;
    this.requestsTotal++;

    try {
      await send(frameData);
      return await deferred.promise;
    } catch (err) {
      clearTimeout(timeout);
      this.unregisterRequest(messageType, deferred);
      throw err;
    }
  }

  /**
   * Unregister a pending request (on cancel/timeout)
   */
  private unregisterRequest(
    messageType: number,
    deferred: Deferred<Uint8Array>,
  ): void {
    const queue = this.pending.get(messageType);
    if (!queue) return;

    const index = queue.findIndex((r) => r.deferred === deferred);
    if (index >= 0) {
      queue.splice(index, 1);
      this.requestsInFlight--;
      if (queue.length === 0) {
        this.pending.delete(messageType);
      }
    }
  }

  /**
   * Dispatch incoming frame to appropriate handler
   */
  dispatch(messageType: number, payload: Uint8Array): void {
    const queue = this.pending.get(messageType);
    if (queue && queue.length > 0) {
      // Match to oldest (FIFO) pending request.
      const request = queue.shift();
      if (!request) {
        return;
      }
      if (queue.length === 0) {
        this.pending.delete(messageType);
      }

      clearTimeout(request.timeout);
      this.requestsInFlight--;
      this.responsesTotal++;

      request.deferred.resolve(payload);
      return;
    }

    const handler = this.notificationHandlers.get(messageType);
    if (handler) {
      try {
        handler(payload);
      } catch {
        // Best-effort notification dispatch.
      }
      return;
    }

    const optionalResponses = this.optionalResponses.get(messageType) ?? 0;
    if (optionalResponses > 0) {
      if (optionalResponses === 1) {
        this.optionalResponses.delete(messageType);
      } else {
        this.optionalResponses.set(messageType, optionalResponses - 1);
      }
      this.responsesIgnored++;
      return;
    }

    if (this.state !== ConnectionState.Authenticated) {
      this.responsesIgnored++;
      return;
    }

    this.responsesDropped++;
  }

  /**
   * Cancel all in-flight requests
   */
  cancelAll(): void {
    for (const [, queue] of this.pending) {
      for (const request of queue) {
        clearTimeout(request.timeout);
        request.deferred.reject(new Error("Connection closed or reset"));
      }
    }
    this.pending.clear();
    this.requestsInFlight = 0;
  }

  /**
   * Get metrics
   */
  getMetrics(): {
    requestsInFlight: number;
    requestsTotal: number;
    responsesTotal: number;
    responsesDropped: number;
    responsesIgnored: number;
  } {
    return {
      requestsInFlight: this.requestsInFlight,
      requestsTotal: this.requestsTotal,
      responsesTotal: this.responsesTotal,
      responsesDropped: this.responsesDropped,
      responsesIgnored: this.responsesIgnored,
    };
  }

  /**
   * Get number of in-flight requests
   */
  getInFlightCount(): number {
    return this.requestsInFlight;
  }

  /**
   * Check if there are pending requests
   */
  hasPending(): boolean {
    return this.requestsInFlight > 0;
  }

  /**
   * Get current state
   */
  getState(): ConnectionState {
    return this.state;
  }
}
