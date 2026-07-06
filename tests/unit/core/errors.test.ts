import { describe, expect, it } from "vite-plus/test";

import {
  ErrCodeKvBackendError,
  ErrCodeKvIsolationConflict,
  ErrCodeLeaseHeld,
  ErrCodeQueueFull,
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
  ErrKvConflictingWrite,
  ErrKvLeaseExpired,
  ErrKvOperationNotAllowed,
  ErrKvTransactionAborted,
  ErrLeaseHeld,
  ErrLeaseInvalidToken,
  ErrLeaseNotFound,
  ErrKvKeyNotFound,
  ErrNoticeGeneral,
  ErrQueueFull,
  ErrQueueInvalidDelay,
  ErrQueueInvalidToken,
  ErrQueueMessageNotFound,
  ErrQueueNotFound,
  ErrScheduleInvalidCron,
  ErrScheduleInvalidDelay,
  ErrScheduleInvalidTimestamp,
  ErrScheduleNotFound,
  ErrScheduleTaskNotFound,
  ErrStreamExpectedOffsetMismatch,
  ErrStreamFull,
  ErrStreamInvalidOffset,
  ErrStreamNotFound,
  ErrStreamOffsetOutOfRange,
  ErrStreamSessionClosed,
  ErrStreamSessionNotFound,
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
    expect({
      ErrKvTransactionAborted,
      ErrKvLeaseExpired,
      ErrKvConflictingWrite,
      ErrKvKeyNotFound,
      ErrKvOperationNotAllowed,
      ErrCodeKvIsolationConflict,
      ErrCodeKvBackendError,
      ErrQueueNotFound,
      ErrQueueMessageNotFound,
      ErrQueueInvalidToken,
      ErrQueueFull,
      ErrQueueInvalidDelay,
      ErrCodeQueueFull,
      ErrCodeRpcTimeout,
      ErrCodeRpcWorkerNotFound,
      ErrCodeRpcBackpressure,
      ErrCodeRpcRouteNotRegistered,
      ErrCodeRpcCorrelationNotFound,
      ErrCodeRpcInvalidSequence,
      ErrCodeRpcDuplicateCorrelation,
      ErrCodeRpcWrongWorker,
      ErrCodeRpcUnauthorized,
      ErrCodeRpcBackendError,
      ErrLeaseHeld,
      ErrLeaseNotFound,
      ErrLeaseInvalidToken,
      ErrCodeLeaseHeld,
      ErrNoticeGeneral,
      ErrStreamNotFound,
      ErrStreamOffsetOutOfRange,
      ErrStreamInvalidOffset,
      ErrStreamFull,
      ErrStreamSessionNotFound,
      ErrStreamSessionClosed,
      ErrStreamExpectedOffsetMismatch,
      ErrScheduleNotFound,
      ErrScheduleTaskNotFound,
      ErrScheduleInvalidCron,
      ErrScheduleInvalidDelay,
      ErrScheduleInvalidTimestamp,
    }).toEqual({
      ErrKvTransactionAborted: 1,
      ErrKvLeaseExpired: 2,
      ErrKvConflictingWrite: 3,
      ErrKvKeyNotFound: 4,
      ErrKvOperationNotAllowed: 5,
      ErrCodeKvIsolationConflict: 1004,
      ErrCodeKvBackendError: 1009,
      ErrQueueNotFound: 1,
      ErrQueueMessageNotFound: 2,
      ErrQueueInvalidToken: 3,
      ErrQueueFull: 4,
      ErrQueueInvalidDelay: 5,
      ErrCodeQueueFull: 4005,
      ErrCodeRpcTimeout: 6001,
      ErrCodeRpcWorkerNotFound: 6002,
      ErrCodeRpcBackpressure: 6003,
      ErrCodeRpcRouteNotRegistered: 6004,
      ErrCodeRpcCorrelationNotFound: 6005,
      ErrCodeRpcInvalidSequence: 6006,
      ErrCodeRpcDuplicateCorrelation: 6007,
      ErrCodeRpcWrongWorker: 6008,
      ErrCodeRpcUnauthorized: 6009,
      ErrCodeRpcBackendError: 6010,
      ErrLeaseHeld: 1,
      ErrLeaseNotFound: 2,
      ErrLeaseInvalidToken: 3,
      ErrCodeLeaseHeld: 5001,
      ErrNoticeGeneral: 1,
      ErrStreamNotFound: 1,
      ErrStreamOffsetOutOfRange: 2,
      ErrStreamInvalidOffset: 3,
      ErrStreamFull: 4,
      ErrStreamSessionNotFound: 5,
      ErrStreamSessionClosed: 6,
      ErrStreamExpectedOffsetMismatch: 7,
      ErrScheduleNotFound: 1,
      ErrScheduleTaskNotFound: 2,
      ErrScheduleInvalidCron: 3,
      ErrScheduleInvalidDelay: 4,
      ErrScheduleInvalidTimestamp: 5,
    });
  });

  it("classifies timeout and transport failures as retryable", () => {
    expect(isRetryable(new TimeoutError("timeout"))).toBe(true);
    expect(isRetryable(new TransportError("connection reset"))).toBe(true);
  });

  it("classifies known domain retryable codes as retryable", () => {
    expect(
      isRetryable(new KvError("conflict", "CONFLICTING_WRITE", ErrCodeKvIsolationConflict)),
    ).toBe(true);
    expect(isRetryable(new KvError("backend", "BACKEND_ERROR", ErrCodeKvBackendError))).toBe(true);
    expect(isRetryable(new QueueError("full", "QUEUE_FULL", ErrCodeQueueFull))).toBe(true);
    expect(isRetryable(new LeaseError("held", "LEASE_HELD", ErrCodeLeaseHeld))).toBe(true);
    expect(isRetryable(new RpcError("timeout", "TIMEOUT", ErrCodeRpcTimeout))).toBe(true);
    expect(
      isRetryable(new RpcError("worker missing", "WORKER_NOT_FOUND", ErrCodeRpcWorkerNotFound)),
    ).toBe(true);
    expect(isRetryable(new RpcError("backpressure", "BACKPRESSURE", ErrCodeRpcBackpressure))).toBe(
      true,
    );
    expect(
      isRetryable(
        new RpcError("route missing", "ROUTE_NOT_REGISTERED", ErrCodeRpcRouteNotRegistered),
      ),
    ).toBe(true);
    expect(
      isRetryable(
        new QueueError(
          'ENQUEUE failed: Failed to commit transaction: WriteStall("Memory budget exceeded")',
          "ERROR",
        ),
      ),
    ).toBe(true);
  });

  it("does not classify non-retryable domain errors as retryable", () => {
    expect(isRetryable(new KvError("missing", "KEY_NOT_FOUND", 4))).toBe(false);
    expect(isRetryable(new QueueError("invalid token", "INVALID_TOKEN", 3))).toBe(false);
    expect(
      isRetryable(
        new QueueError("ENQUEUE failed: Failed to commit transaction: InvalidRoute", "ERROR"),
      ),
    ).toBe(false);
    expect(isRetryable(new RpcError("unauthorized", "UNAUTHORIZED", ErrCodeRpcUnauthorized))).toBe(
      false,
    );
    expect(isRetryable(new StreamError("missing", "STREAM_NOT_FOUND", 1))).toBe(false);
    expect(isRetryable(new Error("plain error"))).toBe(false);
  });
});
