export const ErrKvTransactionAborted = 1;
export const ErrKvLeaseExpired = 2;
export const ErrKvConflictingWrite = 3;
export const ErrKvKeyNotFound = 4;
export const ErrKvOperationNotAllowed = 5;

export const ErrQueueNotFound = 1;
export const ErrQueueMessageNotFound = 2;
export const ErrQueueInvalidToken = 3;
export const ErrQueueFull = 4;
export const ErrQueueInvalidDelay = 5;

export const ErrRpcTimeout = 1;
export const ErrRpcHandlerNotFound = 2;
export const ErrRpcHandlerError = 3;
export const ErrRpcInvalidRequest = 4;

export const ErrLeaseHeld = 1;
export const ErrLeaseNotFound = 2;
export const ErrLeaseInvalidToken = 3;

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
  "KV_4",
  "QUEUE_4",
  "LEASE_1",
  "NOTICE_1",
  "STREAM_1",
  "STREAM_2",
  "STREAM_3",
  "STREAM_4",
  "RPC_1",
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
  constructor(
    message: string,
    domainCode?: number,
    context?: Record<string, unknown>,
  ) {
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
