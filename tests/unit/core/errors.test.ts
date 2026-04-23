import { describe, expect, it } from "vite-plus/test";

import {
  ErrCodeKvBackendError,
  ErrCodeKvIsolationConflict,
  ErrCodeLeaseHeld,
  ErrCodeQueueFull,
  ErrCodeRpcBackpressure,
  ErrCodeRpcRouteNotRegistered,
  ErrCodeRpcTimeout,
  ErrCodeRpcWorkerNotFound,
  ErrKvKeyNotFound,
  ErrLeaseHeld,
  ErrQueueFull,
  ErrRpcTimeout,
  ErrStreamNotFound,
  KvError,
  LeaseError,
  QueueError,
  RpcError,
  StreamError,
  TimeoutError,
  TransportError,
  isRetryable,
} from "../../../src/core/errors";

describe("core errors", () => {
  it("exports stable named error codes", () => {
    expect(ErrKvKeyNotFound).toBe(4);
    expect(ErrQueueFull).toBe(4);
    expect(ErrRpcTimeout).toBe(1);
    expect(ErrLeaseHeld).toBe(1);
    expect(ErrStreamNotFound).toBe(1);

    expect(ErrCodeKvIsolationConflict).toBe(1004);
    expect(ErrCodeKvBackendError).toBe(1009);
    expect(ErrCodeQueueFull).toBe(4005);
    expect(ErrCodeLeaseHeld).toBe(5001);
    expect(ErrCodeRpcTimeout).toBe(6001);
    expect(ErrCodeRpcWorkerNotFound).toBe(6002);
    expect(ErrCodeRpcBackpressure).toBe(6003);
    expect(ErrCodeRpcRouteNotRegistered).toBe(6004);
  });

  it("classifies timeout and transport failures as retryable", () => {
    expect(isRetryable(new TimeoutError("timeout"))).toBe(true);
    expect(isRetryable(new TransportError("connection reset"))).toBe(true);
  });

  it("classifies known domain retryable codes as retryable", () => {
    expect(isRetryable(new KvError("conflict", "CONFLICTING_WRITE", ErrCodeKvIsolationConflict))).toBe(true);
    expect(isRetryable(new KvError("backend", "BACKEND_ERROR", ErrCodeKvBackendError))).toBe(true);
    expect(isRetryable(new QueueError("full", "QUEUE_FULL", ErrCodeQueueFull))).toBe(true);
    expect(isRetryable(new LeaseError("held", "LEASE_HELD", ErrCodeLeaseHeld))).toBe(true);
    expect(isRetryable(new RpcError("timeout", "TIMEOUT", ErrCodeRpcTimeout))).toBe(true);
    expect(isRetryable(new RpcError("worker missing", "WORKER_NOT_FOUND", ErrCodeRpcWorkerNotFound))).toBe(true);
    expect(isRetryable(new RpcError("backpressure", "BACKPRESSURE", ErrCodeRpcBackpressure))).toBe(true);
    expect(isRetryable(new RpcError("route missing", "ROUTE_NOT_REGISTERED", ErrCodeRpcRouteNotRegistered))).toBe(true);
  });

  it("does not classify non-retryable domain errors as retryable", () => {
    expect(isRetryable(new KvError("missing", "KEY_NOT_FOUND", 4))).toBe(false);
    expect(isRetryable(new QueueError("invalid token", "INVALID_TOKEN", 3))).toBe(false);
    expect(isRetryable(new RpcError("handler error", "HANDLER_ERROR", 3))).toBe(false);
    expect(isRetryable(new StreamError("missing", "STREAM_NOT_FOUND", 1))).toBe(false);
    expect(isRetryable(new Error("plain error"))).toBe(false);
  });
});
