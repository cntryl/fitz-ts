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
  ErrCodeScheduleBackendError,
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
  ScheduleError,
  TimeoutError,
  TransportError,
  isRetryable,
  attachErrorDisposition,
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
      ErrCodeScheduleBackendError,
    }).toEqual({
      ErrKvTransactionAborted: 1,
      ErrKvLeaseExpired: 2,
      ErrKvConflictingWrite: 3,
      ErrKvKeyNotFound: 4,
      ErrKvOperationNotAllowed: 5,
      ErrCodeKvIsolationConflict: 1053,
      ErrCodeKvBackendError: 1006,
      ErrQueueNotFound: 1,
      ErrQueueMessageNotFound: 2,
      ErrQueueInvalidToken: 3,
      ErrQueueFull: 4,
      ErrQueueInvalidDelay: 5,
      ErrCodeQueueFull: 4005,
      ErrCodeRpcTimeout: 6007,
      ErrCodeRpcWorkerNotFound: 6050,
      ErrCodeRpcBackpressure: 6005,
      ErrCodeRpcRouteNotRegistered: 6051,
      ErrCodeRpcCorrelationNotFound: 6052,
      ErrCodeRpcInvalidSequence: 6053,
      ErrCodeRpcDuplicateCorrelation: 6054,
      ErrCodeRpcWrongWorker: 6055,
      ErrCodeRpcUnauthorized: 6001,
      ErrCodeRpcBackendError: 6006,
      ErrLeaseHeld: 1,
      ErrLeaseNotFound: 2,
      ErrLeaseInvalidToken: 3,
      ErrCodeLeaseHeld: 5050,
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
      ErrCodeScheduleBackendError: 7006,
    });
  });

  it("should classify retryable errors given timeout and transport failures when policy runs", () => {
    expect(isRetryable(new TimeoutError("timeout"))).toBe(true);
    expect(isRetryable(new TransportError("connection reset"))).toBe(true);
  });

  it("should classify broker rejection given explicit disposition when retry policy runs", () => {
    const cases = [
      [new KvError("conflict", "CONFLICT", 1053), 0, 0, true],
      [new QueueError("busy", "BUSY", 4005), 5, 0, true],
      [new LeaseError("held", "HELD", 5050), 0, 0, true],
      [new RpcError("missing", "ROUTE_NOT_REGISTERED", 6051), 0, 0, true],
      [new RpcError("timeout", "TIMEOUT", 6007), 7, 1, false],
      [new ScheduleError("backend", "BACKEND_ERROR", 7006), 6, 1, false],
      [new QueueError("busy", "BUSY", 4005), 5, 1, false],
    ] as const;
    for (const [error, category, effect, expected] of cases) {
      expect(isRetryable(attachErrorDisposition(error, category, effect))).toBe(expected);
    }
  });
  it("should reject retry given missing disposition despite transient Queue wording", () => {
    const error = new QueueError(
      'Failed to commit transaction: WriteStall("Memory budget exceeded")',
      "ERROR",
    );
    expect(isRetryable(error)).toBe(false);
  });

  it("should not retry fatal failures given an unauthorized error when a call is classified", () => {
    expect(isRetryable(new RpcError("unauthorized", "UNAUTHORIZED", ErrCodeRpcUnauthorized))).toBe(
      false,
    );
  });
});

it("should treat coded Queue unknown outcomes as terminal regardless of message text", () => {
  // Arrange
  const error = new QueueError(
    'Failed to commit transaction: WriteStall("Memory budget exceeded")',
    "ERROR",
    4007,
  );
  // Act
  const retryable = isRetryable(error);
  // Assert
  expect(retryable).toBe(false);
});
