import { describe, expect, it } from "vitest";

import {
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
  });

  it("classifies timeout and transport failures as retryable", () => {
    expect(isRetryable(new TimeoutError("timeout"))).toBe(true);
    expect(isRetryable(new TransportError("connection reset"))).toBe(true);
  });

  it("classifies known domain retryable codes as retryable", () => {
    expect(isRetryable(new KvError("missing", "KEY_NOT_FOUND", 4))).toBe(true);
    expect(isRetryable(new QueueError("full", "QUEUE_FULL", 4))).toBe(true);
    expect(isRetryable(new LeaseError("held", "LEASE_HELD", 1))).toBe(true);
    expect(isRetryable(new RpcError("timeout", "TIMEOUT", 1))).toBe(true);
    expect(isRetryable(new StreamError("missing", "STREAM_NOT_FOUND", 1))).toBe(true);
  });

  it("does not classify non-retryable domain errors as retryable", () => {
    expect(isRetryable(new QueueError("invalid token", "INVALID_TOKEN", 3))).toBe(false);
    expect(isRetryable(new RpcError("handler missing", "HANDLER_NOT_FOUND", 2))).toBe(false);
    expect(isRetryable(new Error("plain error"))).toBe(false);
  });
});
