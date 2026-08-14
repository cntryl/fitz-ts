/** KV status: the transaction was aborted. */
export const ErrKvTransactionAborted = 1;
/** KV status: a lease required by the transaction expired. */
export const ErrKvLeaseExpired = 2;
/** KV status: a concurrent write conflicted with this transaction. */
export const ErrKvConflictingWrite = 3;
/** KV status: the requested key does not exist. */
export const ErrKvKeyNotFound = 4;
/** KV status: the operation is invalid for the transaction mode or state. */
export const ErrKvOperationNotAllowed = 5;

/** KV error code: transaction isolation conflict; callers may retry a new transaction. */
export const ErrCodeKvIsolationConflict = 1004;
/** KV error code: backend failure; retry only according to the configured policy. */
export const ErrCodeKvBackendError = 1009;
/** KV error code: invalid subscription selector or request. */
export const ErrCodeKvInvalidSubscription = 1012;
/** KV error code: broker subscription limit reached. */
export const ErrCodeKvSubscriptionLimit = 1013;

/** Queue status: route does not identify an existing queue. */
export const ErrQueueNotFound = 1;
/** Queue status: reserved message no longer exists. */
export const ErrQueueMessageNotFound = 2;
/** Queue status: reservation token is stale or invalid. */
export const ErrQueueInvalidToken = 3;
/** Queue status: queue capacity is exhausted. */
export const ErrQueueFull = 4;
/** Queue status: requested delay is outside the supported range. */
export const ErrQueueInvalidDelay = 5;

/** Queue error code: capacity is exhausted; classified as retryable. */
export const ErrCodeQueueFull = 4005;
/** Queue error code: invalid availability subscription. */
export const ErrCodeQueueInvalidSubscription = 4010;
/** Queue error code: broker subscription limit reached. */
export const ErrCodeQueueSubscriptionLimit = 4011;

/** RPC error code: request exceeded its broker deadline. */
export const ErrCodeRpcTimeout = 6001;
/** RPC error code: no worker is currently available for the route. */
export const ErrCodeRpcWorkerNotFound = 6002;
/** RPC error code: worker capacity is exhausted. */
export const ErrCodeRpcBackpressure = 6003;
/** RPC error code: route has no registered worker. */
export const ErrCodeRpcRouteNotRegistered = 6004;
/** RPC error code: correlation is unknown or already complete. */
export const ErrCodeRpcCorrelationNotFound = 6005;
/** RPC error code: response frames arrived with an invalid sequence. */
export const ErrCodeRpcInvalidSequence = 6006;
/** RPC error code: correlation identifier was reused. */
export const ErrCodeRpcDuplicateCorrelation = 6007;
/** RPC error code: response came from a worker that does not own the request. */
export const ErrCodeRpcWrongWorker = 6008;
/** RPC error code: caller or worker is not authorized for the route. */
export const ErrCodeRpcUnauthorized = 6009;
/** RPC error code: broker backend failed. */
export const ErrCodeRpcBackendError = 6010;
/** RPC error code: invalid worker subscription. */
export const ErrCodeRpcInvalidSubscription = 6012;
/** RPC error code: broker worker-subscription limit reached. */
export const ErrCodeRpcSubscriptionLimit = 6013;

/** Lease status: another owner currently holds the lease. */
export const ErrLeaseHeld = 1;
/** Lease status: route has no active lease. */
export const ErrLeaseNotFound = 2;
/** Lease status: fencing/renewal token is stale or invalid. */
export const ErrLeaseInvalidToken = 3;

/** Lease error code: another owner holds the lease; acquisition may wait or retry. */
export const ErrCodeLeaseHeld = 5001;
/** Lease error code: malformed acquisition or lifecycle request. */
export const ErrCodeLeaseBadRequest = 5008;
/** Lease error code: invalid change-subscription route. */
export const ErrCodeLeaseInvalidSubscriptionRoute = 5010;

/** Notice status: publish or subscription failed without a more specific status. */
export const ErrNoticeGeneral = 1;

/** Stream status: route does not identify an existing stream. */
export const ErrStreamNotFound = 1;
/** Stream status: requested offset is outside retained history. */
export const ErrStreamOffsetOutOfRange = 2;
/** Stream status: offset is invalid for this operation. */
export const ErrStreamInvalidOffset = 3;
/** Stream status: stream capacity is exhausted. */
export const ErrStreamFull = 4;
/** Stream status: write session does not exist. */
export const ErrStreamSessionNotFound = 5;
/** Stream status: write session is already finalized. */
export const ErrStreamSessionClosed = 6;
/** Stream status: optimistic expected offset did not match. */
export const ErrStreamExpectedOffsetMismatch = 7;

/** Schedule status: schedule route does not exist. */
export const ErrScheduleNotFound = 1;
/** Schedule status: task no longer exists. */
export const ErrScheduleTaskNotFound = 2;
/** Schedule status: cron expression is invalid. */
export const ErrScheduleInvalidCron = 3;
/** Schedule status: delay is outside the supported range. */
export const ErrScheduleInvalidDelay = 4;
/** Schedule status: timestamp is invalid. */
export const ErrScheduleInvalidTimestamp = 5;
/** Schedule error code: delivery mode is not `Broadcast` or `Single`. */
export const ErrCodeScheduleInvalidDeliveryMode = 7008;
/** Stream error code: invalid commit subscription selector. */
export const ErrCodeStreamInvalidSubscription = 2010;
/** Stream error code: broker subscription limit reached. */
export const ErrCodeStreamSubscriptionLimit = 2011;
/** Notice error code: subscription pattern is invalid. */
export const ErrCodeNoticeInvalidPattern = 3002;
/** Notice error code: broker subscription limit reached. */
export const ErrCodeNoticeSubscriptionLimit = 3003;
/** Schedule error code: invalid firing-subscription pattern. */
export const ErrCodeScheduleInvalidSubscription = 7006;
/** Schedule error code: broker subscription limit reached. */
export const ErrCodeScheduleSubscriptionLimit = 7007;

const retryableErrorCodes = new Set([
  "KV_3",
  `KV_${ErrCodeKvIsolationConflict}`,
  `KV_${ErrCodeKvBackendError}`,
  "QUEUE_4",
  `QUEUE_${ErrCodeQueueFull}`,
  "LEASE_1",
  `LEASE_${ErrCodeLeaseHeld}`,
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

function isTransientQueueCommitFailure(error: FitzError): boolean {
  if (!error.code.startsWith("QUEUE_")) {
    return false;
  }

  const message = error.message.toLowerCase();
  if (!message.includes("failed to commit transaction:")) {
    return false;
  }

  return (
    message.includes("writestall(") ||
    message.includes("memory budget exceeded") ||
    message.includes("lease heartbeat reports unhealthy") ||
    message.includes("refusing writes")
  );
}

/** Returns whether Fitz classifies `error` as safe for an automatic operation retry. */
export function isRetryable(error: unknown): boolean {
  if (!(error instanceof FitzError)) {
    return false;
  }

  if (error instanceof TimeoutError || error instanceof TransportError) {
    return true;
  }

  const key = retryableKey(error);
  if (key !== null && retryableErrorCodes.has(key)) {
    return true;
  }

  return isTransientQueueCommitFailure(error);
}

/**
 * Base class for failures produced by Fitz. Branch on the concrete subclass,
 * `code`, or `domainCode`; messages are diagnostic text and are not a stable
 * programmatic contract. Transport and timeout errors can have ambiguous
 * server outcomes, so consult operation retry semantics before replaying work.
 */
export class FitzError extends Error {
  /** Stable machine-readable client or domain code. */
  code: string;
  /** Numeric broker domain code when the wire response supplied one. */
  domainCode?: number;
  /** Safe structured diagnostic context associated with the failure. */
  context?: Record<string, unknown>;

  /** Creates a Fitz error. Application code usually receives a more specific subclass. */
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

  /** Returns structured diagnostic context, if present. */
  getContext(): Record<string, unknown> | undefined {
    return this.context;
  }
}

/** Network transport failed before a valid Fitz response was received. */
export class TransportError extends FitzError {
  /** Creates a transport failure with optional diagnostic context. */
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "TRANSPORT_ERROR", undefined, context);
    this.name = "TransportError";
    Object.setPrototypeOf(this, TransportError.prototype);
  }
}

/** Client connection state prevents the requested operation. */
export class ConnectionError extends FitzError {
  /** Creates a connection-state failure. */
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "CONNECTION_ERROR", undefined, context);
    this.name = "ConnectionError";
    Object.setPrototypeOf(this, ConnectionError.prototype);
  }
}

/** Local pending-request queue reached `maxRequestQueueSize`. */
export class RequestQueueFullError extends FitzError {
  /** Creates a local request-queue capacity failure. */
  constructor(message = "Request queue is full", context?: Record<string, unknown>) {
    super(message, "REQUEST_QUEUE_FULL", undefined, context);
    this.name = "RequestQueueFullError";
    Object.setPrototypeOf(this, RequestQueueFullError.prototype);
  }
}

/** Fitz rejected authentication or token acquisition failed. */
export class AuthenticationError extends FitzError {
  /** Creates an authentication failure. */
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "AUTH_ERROR", undefined, context);
    this.name = "AuthenticationError";
    Object.setPrototypeOf(this, AuthenticationError.prototype);
  }
}

/** A client-side operation or readiness deadline elapsed. */
export class TimeoutError extends FitzError {
  /** Creates a timeout failure. */
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "TIMEOUT", undefined, context);
    this.name = "TimeoutError";
    Object.setPrototypeOf(this, TimeoutError.prototype);
  }
}

/** Received bytes violate the Fitz framing or operation protocol. */
export class ProtocolError extends FitzError {
  /** Creates a protocol failure, optionally retaining the broker code. */
  constructor(message: string, domainCode?: number, context?: Record<string, unknown>) {
    super(message, "PROTOCOL_ERROR", domainCode, context);
    this.name = "ProtocolError";
    Object.setPrototypeOf(this, ProtocolError.prototype);
  }
}

/** A payload could not be encoded or decoded safely. */
export class CodecError extends FitzError {
  /** Creates a codec failure. */
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, "CODEC_ERROR", undefined, context);
    this.name = "CodecError";
    Object.setPrototypeOf(this, CodecError.prototype);
  }
}

// Domain-specific errors
/** KV domain operation failed. Inspect `domainCode` rather than parsing the message. */
export class KvError extends FitzError {
  /** Creates a KV domain failure. */
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

/** Queue domain operation failed. */
export class QueueError extends FitzError {
  /** Creates a Queue domain failure. */
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

/** Notice domain operation failed. */
export class NoticeError extends FitzError {
  /** Creates a Notice domain failure. */
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

/** RPC call or worker registration failed. */
export class RpcError extends FitzError {
  /** Creates an RPC domain failure. */
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

/** Lease domain operation failed. */
export class LeaseError extends FitzError {
  /** Creates a Lease domain failure. */
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

/** Stream domain operation failed. */
export class StreamError extends FitzError {
  /** Creates a Stream domain failure. */
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

/** A stream read page made no cursor progress; stop rather than loop indefinitely. */
export class StreamReadStalledError extends StreamError {
  /** Creates a stalled-read failure for `selector` at `fromOffset`. */
  constructor(selector: string, fromOffset: bigint) {
    super(
      `Stream read made no logical progress for selector ${selector} from offset ${fromOffset}`,
      "READ_STALLED",
      undefined,
      { selector, fromOffset: fromOffset.toString(), retryable: false },
    );
    this.name = "StreamReadStalledError";
    Object.setPrototypeOf(this, StreamReadStalledError.prototype);
  }
}

/** Schedule domain operation failed. */
export class ScheduleError extends FitzError {
  /** Creates a Schedule domain failure. */
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
