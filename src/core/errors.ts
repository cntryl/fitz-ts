export const ErrKvTransactionAborted = 1;
export const ErrKvLeaseExpired = 2;
export const ErrKvConflictingWrite = 3;
export const ErrKvKeyNotFound = 4;
export const ErrKvOperationNotAllowed = 5;

export const ErrCodeKvIsolationConflict = 1004;
export const ErrCodeKvBackendError = 1009;

export const ErrQueueNotFound = 1;
export const ErrQueueMessageNotFound = 2;
export const ErrQueueInvalidToken = 3;
export const ErrQueueFull = 4;
export const ErrQueueInvalidDelay = 5;

export const ErrCodeQueueFull = 4005;

export const ErrRpcTimeout = 1;
export const ErrRpcHandlerNotFound = 2;
export const ErrRpcHandlerError = 3;
export const ErrRpcInvalidRequest = 4;

export const ErrCodeRpcTimeout = 6001;
export const ErrCodeRpcWorkerNotFound = 6002;
export const ErrCodeRpcBackpressure = 6003;
export const ErrCodeRpcRouteNotRegistered = 6004;
export const ErrCodeRpcCorrelationNotFound = 6005;
export const ErrCodeRpcUnauthorized = 6009;

export const ErrLeaseHeld = 1;
export const ErrLeaseNotFound = 2;
export const ErrLeaseInvalidToken = 3;

export const ErrCodeLeaseHeld = 5001;

export const ErrNoticeGeneral = 1;

export const ErrStreamNotFound = 1;
export const ErrStreamOffsetOutOfRange = 2;
export const ErrStreamInvalidOffset = 3;
export const ErrStreamFull = 4;
export const ErrStreamSessionNotFound = 5;
export const ErrStreamSessionClosed = 6;
export const ErrStreamExpectedOffsetMismatch = 7;

export const ErrScheduleNotFound = 1;
export const ErrScheduleTaskNotFound = 2;
export const ErrScheduleInvalidCron = 3;
export const ErrScheduleInvalidDelay = 4;
export const ErrScheduleInvalidTimestamp = 5;

const retryableErrorCodes = new Set([
  "KV_3",
  `KV_${ErrCodeKvIsolationConflict}`,
  `KV_${ErrCodeKvBackendError}`,
  "QUEUE_4",
  `QUEUE_${ErrCodeQueueFull}`,
  "LEASE_1",
  `LEASE_${ErrCodeLeaseHeld}`,
  "RPC_1",
  "RPC_2",
  `RPC_${ErrCodeRpcTimeout}`,
  `RPC_${ErrCodeRpcWorkerNotFound}`,
  `RPC_${ErrCodeRpcBackpressure}`,
  `RPC_${ErrCodeRpcRouteNotRegistered}`,
]);

function retryableKey(error: FitzError): string | null {
  const prefix = error.code.split("_")[0];
  if (error.domainCode === undefined) {
    return null;
  }
  return `${prefix}_${error.domainCode}`;
}

export function isRetryable(error: unknown): boolean {
  if (!(error instanceof FitzError)) {
    return false;
  }

  if (error instanceof TimeoutError || error instanceof TransportError) {
    return true;
  }

  const key = retryableKey(error);
  return key !== null && retryableErrorCodes.has(key);
}

/**
 * Error types for Fitz client
 */

export class FitzError extends Error {
  code: string;
  domainCode?: number;
  context?: Record<string, unknown>;

  constructor(
    message: string,
    code: string,
    domainCode?: number,
    context?: Record<string, unknown>,
  ) {
    super(message);
    this.name = "FitzError";
    this.code = code;
    this.domainCode = domainCode;
    this.context = context;
    Object.setPrototypeOf(this, FitzError.prototype);
  }

  getContext(): Record<string, unknown> | undefined {
    return this.context;
  }
}

export class TransportError extends FitzError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "TRANSPORT_ERROR", undefined, context);
    this.name = "TransportError";
    Object.setPrototypeOf(this, TransportError.prototype);
  }
}

export class ConnectionError extends FitzError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "CONNECTION_ERROR", undefined, context);
    this.name = "ConnectionError";
    Object.setPrototypeOf(this, ConnectionError.prototype);
  }
}

export class RequestQueueFullError extends FitzError {
  constructor(message = "Request queue is full", context?: Record<string, unknown>) {
    super(message, "REQUEST_QUEUE_FULL", undefined, context);
    this.name = "RequestQueueFullError";
    Object.setPrototypeOf(this, RequestQueueFullError.prototype);
  }
}

export class AuthenticationError extends FitzError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "AUTH_ERROR", undefined, context);
    this.name = "AuthenticationError";
    Object.setPrototypeOf(this, AuthenticationError.prototype);
  }
}

export class TimeoutError extends FitzError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "TIMEOUT", undefined, context);
    this.name = "TimeoutError";
    Object.setPrototypeOf(this, TimeoutError.prototype);
  }
}

export class ProtocolError extends FitzError {
  constructor(message: string, domainCode?: number, context?: Record<string, unknown>) {
    super(message, "PROTOCOL_ERROR", domainCode, context);
    this.name = "ProtocolError";
    Object.setPrototypeOf(this, ProtocolError.prototype);
  }
}

export class CodecError extends FitzError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "CODEC_ERROR", undefined, context);
    this.name = "CodecError";
    Object.setPrototypeOf(this, CodecError.prototype);
  }
}

// Domain-specific errors
export class KvError extends FitzError {
  constructor(
    message: string,
    code: string,
    domainCode?: number,
    context?: Record<string, unknown>,
  ) {
    super(message, `KV_${code}`, domainCode, context);
    this.name = "KvError";
    Object.setPrototypeOf(this, KvError.prototype);
  }
}

export class QueueError extends FitzError {
  constructor(
    message: string,
    code: string,
    domainCode?: number,
    context?: Record<string, unknown>,
  ) {
    super(message, `QUEUE_${code}`, domainCode, context);
    this.name = "QueueError";
    Object.setPrototypeOf(this, QueueError.prototype);
  }
}

export class NoticeError extends FitzError {
  constructor(
    message: string,
    code: string,
    domainCode?: number,
    context?: Record<string, unknown>,
  ) {
    super(message, `NOTICE_${code}`, domainCode, context);
    this.name = "NoticeError";
    Object.setPrototypeOf(this, NoticeError.prototype);
  }
}

export class RpcError extends FitzError {
  constructor(
    message: string,
    code: string,
    domainCode?: number,
    context?: Record<string, unknown>,
  ) {
    super(message, `RPC_${code}`, domainCode, context);
    this.name = "RpcError";
    Object.setPrototypeOf(this, RpcError.prototype);
  }
}

export class LeaseError extends FitzError {
  constructor(
    message: string,
    code: string,
    domainCode?: number,
    context?: Record<string, unknown>,
  ) {
    super(message, `LEASE_${code}`, domainCode, context);
    this.name = "LeaseError";
    Object.setPrototypeOf(this, LeaseError.prototype);
  }
}

export class StreamError extends FitzError {
  constructor(
    message: string,
    code: string,
    domainCode?: number,
    context?: Record<string, unknown>,
  ) {
    super(message, `STREAM_${code}`, domainCode, context);
    this.name = "StreamError";
    Object.setPrototypeOf(this, StreamError.prototype);
  }
}

export class ScheduleError extends FitzError {
  constructor(
    message: string,
    code: string,
    domainCode?: number,
    context?: Record<string, unknown>,
  ) {
    super(message, `SCHEDULE_${code}`, domainCode, context);
    this.name = "ScheduleError";
    Object.setPrototypeOf(this, ScheduleError.prototype);
  }
}
