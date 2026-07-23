/**
 * Fitz TypeScript Client - Browser Runtime Entry Point
 */

// Core exports
export { createClient } from "./client/browser-client";
export type {
  BrowserClient,
  BrowserClient as Client,
  BrowserClientConfig,
  BrowserClientConfig as ClientConfig,
  BrowserTransportType,
  BrowserTransportType as TransportType,
  BrowserWebSocketOptions,
  BrowserWebSocketOptions as WebSocketOptions,
} from "./client/browser-client";

// Types and errors
export type {
  AsyncHandlerOptions,
  ClientConnectOptions,
  ConnectWhenReadyOptions,
  HeartbeatOptions,
  FitzLifecycleEvent,
  FitzLogger,
  FitzLogLevel,
  FitzMeter,
  FitzObservability,
  FitzSpan,
  FitzTracer,
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
  ErrCodeScheduleInvalidDeliveryMode,
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
  LeaseLifecycleError,
  WithLeaseOptions,
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
  ScheduleDeliveryMode,
  ScheduleNotification,
  ScheduleHandler,
  ScheduleSubscription,
  ScheduleStatus,
} from "./domains/schedule/types";
