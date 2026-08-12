/**
 * RPC domain types
 * Per fitz-go/internal/domains/rpc/rpc.go
 */

import { createSubscriptionHandle } from "../internal/subscription-handle";

/**
 * Single response frame from a streaming RPC call
 */
export interface ResponseFrame {
  body: Uint8Array;
  sequence: bigint;
}

/**
 * Inbound RPC request received by a worker
 */
export interface InboundRequest {
  route: string;
  body: Uint8Array;
}

/**
 * Allows a worker to send responses back to the caller
 */
export interface ResponseWriter {
  write(options: { body: Uint8Array; signal?: AbortSignal }): Promise<void>;
  end(options?: { body?: Uint8Array; signal?: AbortSignal }): Promise<void>;
}

/**
 * Handler for incoming RPC requests (worker mode)
 */
export type RpcHandler = (req: InboundRequest, writer: ResponseWriter) => Promise<void>;

export interface RegisterWorkerOptions {
  maxConcurrency?: number;
}

/**
 * Active worker registration
 */
export interface RpcSubscription extends AsyncDisposable {
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
  body: Uint8Array;
  timeoutMs?: number;
  signal?: AbortSignal;
}

/**
 * RPC status codes (from server responses)
 */
export enum RpcStatus {
  Ok = 0,
  Timeout = 1,
  HandlerNotFound = 2,
  HandlerError = 3,
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
