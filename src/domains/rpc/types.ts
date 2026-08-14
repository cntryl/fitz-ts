/**
 * RPC domain types
 * Per fitz-go/internal/domains/rpc/rpc.go
 */

import { createSubscriptionHandle } from "../internal/subscription-handle";

/**
 * Single response frame from a streaming RPC call
 */
export interface ResponseFrame {
  /** Response payload for this frame. */
  body: Uint8Array;
  /** Monotonically increasing frame sequence assigned by the worker. */
  sequence: bigint;
}

/**
 * Inbound RPC request received by a worker
 */
export interface InboundRequest {
  /** Concrete RPC route on which the request arrived. */
  route: string;
  /** Caller-supplied request payload. */
  body: Uint8Array;
}

/**
 * Allows a worker to send responses back to the caller
 */
export interface ResponseWriter {
  /** Sends a non-terminal response frame. Writes for one request are ordered. */
  write(options: {
    /** Non-terminal response payload. */
    body: Uint8Array;
    /** Cancels sending this frame. */
    signal?: AbortSignal;
  }): Promise<void>;
  /** Sends the terminal frame, optionally with a final body; do not write afterward. */
  end(options?: {
    /** Optional terminal response payload. */
    body?: Uint8Array;
    /** Cancels sending the terminal frame; caller may then time out. */
    signal?: AbortSignal;
  }): Promise<void>;
}

/**
 * Handles one admitted RPC request. The handler must call
 * {@link ResponseWriter.end} exactly once to terminate the caller's iterator;
 * returning without it leaves the caller waiting until timeout. Throwing sends
 * a best-effort terminal error response. Handler concurrency is bounded by the
 * registration and the client's shared async-handler dispatcher.
 */
export type RpcHandler = (req: InboundRequest, writer: ResponseWriter) => Promise<void>;

/** Limits for an RPC worker registration. */
export interface RegisterWorkerOptions {
  /** Maximum requests handled concurrently by this registration. Must be at least 1. */
  maxConcurrency?: number;
}

/**
 * Active worker registration
 */
export interface RpcSubscription extends AsyncDisposable {
  /** Unregisters this worker and stops admitting new requests. */
  unsubscribe(): Promise<void>;
}

export function createRpcSubscription(
  unsubscribeFn: () => Promise<void>,
  signal?: AbortSignal,
): RpcSubscription {
  return createSubscriptionHandle<RpcSubscription>(unsubscribeFn, signal);
}

/**
 * RPC request options
 */
export interface RequestOptions {
  /** Request payload delivered to the selected worker. */
  body: Uint8Array;
  /** Broker RPC deadline in milliseconds. Defaults to the client request timeout. */
  timeoutMs?: number;
  /** Cancels local iteration/waiting; it cannot retract work already admitted by a worker. */
  signal?: AbortSignal;
}

/**
 * RPC status codes (from server responses)
 */
export enum RpcStatus {
  /** Operation succeeded. */
  Ok = 0,
  /** Request deadline elapsed. */
  Timeout = 1,
  /** No worker is registered for the route. */
  HandlerNotFound = 2,
  /** Worker handler failed. */
  HandlerError = 3,
  /** Request or response sequence is invalid. */
  InvalidRequest = 4,
}

/**
 * Response to RPC_SUBSCRIBE request
 */
export interface SubscribeResponse {
  status: number;
}

/**
 * Response to RPC_UNSUBSCRIBE request
 */
export interface UnsubscribeResponse {
  status: number;
}
