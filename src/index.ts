/**
 * Fitz TypeScript Client - Main Entry Point
 */

// Core exports
export { Client, createClient } from "./client/client";

// Types and errors
export type {
  AsyncHandlerOptions,
  ClientConfig,
  ClientConnectOptions,
  ConnectWhenReadyOptions,
  HeartbeatOptions,
  WebSocketOptions,
  FitzLifecycleEvent,
  FitzLogger,
  FitzLogLevel,
  FitzMeter,
  FitzObservability,
  FitzSpan,
  FitzTracer,
  TransportType,
  TokenProvider,
  ReconnectOptions,
  RetryOptions,
} from "./core/types";
export { ConnectionState } from "./core/types";
export { createWakeGate } from "./core/wake-gate";
export type { WakeGate, WakeWaitOptions } from "./core/wake-gate";
export {
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
  FitzError,
  TransportError,
  ConnectionError,
  RequestQueueFullError,
  AuthenticationError,
  TimeoutError,
  ProtocolError,
  CodecError,
  KvError,
  QueueError,
  NoticeError,
  RpcError,
  LeaseError,
  StreamError,
  ScheduleError,
  isRetryable,
} from "./core/errors";
export { createTaskGroup } from "./core/task-group";
export type {
  TaskGroup,
  TaskGroupStatus,
  TaskGroupErrorPolicy,
  TaskContext,
  TaskGroupOptions,
} from "./core/task-group";

// Domain clients
export { KvClient } from "./domains/kv/client";
export type { KvTransaction } from "./domains/kv/client";
export type {
  TxMode,
  DurabilityMode,
  KvBeginOptions,
  KvGetResult,
  KvScanPage,
  KvScanOptions,
} from "./domains/kv/types";

export { QueueClient } from "./domains/queue/client";
export type {
  EnqueueOptions,
  AvailabilityHandler,
  AvailabilityNotification,
  QueueItem,
  QueueSubscription,
  QueueStatus,
} from "./domains/queue/types";
export { RpcClient } from "./domains/rpc/client";
export type {
  RequestOptions as RpcRequestOptions,
  ResponseFrame,
  InboundRequest,
  ResponseWriter,
  RpcHandler,
  RegisterWorkerOptions,
  RpcSubscription,
  RpcStatus,
} from "./domains/rpc/types";
export { LeaseClient } from "./domains/lease/client";
export type {
  LeaseInfo,
  Lease,
  ChangeHandler,
  ChangeNotification,
  LeaseSubscription,
  LeaseStatus,
} from "./domains/lease/types";
export { NoticeClient } from "./domains/notice/client";
export type {
  NoticeMsg,
  NoticeHandler,
  NoticeSubscription,
  NoticeStatus,
} from "./domains/notice/types";
export { StreamClient } from "./domains/stream/client";
export type {
  StreamRecord,
  StreamMetadata,
  StreamDiscriminator,
  StreamFilteredReason,
  StreamFilterClause,
  StreamFilterSet,
  StreamAppendOptions,
  StreamReadOptions,
  StreamReadCursor,
  StreamReadEvent,
  StreamReadFiltered,
  StreamReadFilteredRange,
  StreamReadItem,
  StreamReadPage,
  StreamSession,
  StreamCommitNotification,
  StreamCommitHandler,
  StreamSubscription,
  StreamCommitMode,
  StreamStatus,
} from "./domains/stream/types";
export { ScheduleClient } from "./domains/schedule/client";
export type {
  ScheduleEntry,
  ScheduleNotification,
  ScheduleHandler,
  ScheduleSubscription,
  ScheduleStatus,
} from "./domains/schedule/types";
