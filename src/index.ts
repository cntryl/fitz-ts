/**
 * Fitz TypeScript Client - Main Entry Point
 */

// Core exports
export { Client } from "./client/client";

// Types and errors
export type {
  ClientConfig,
  TransportType,
  TokenProvider,
  ReconnectOptions,
} from "./core/types";
export { ConnectionState } from "./core/types";
export {
  FitzError,
  TransportError,
  ConnectionError,
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
} from "./core/errors";

// Domain clients
export { KvClient, KvTransaction } from "./domains/kv/client";
export type {
  TxMode,
  DurabilityMode,
  KvBeginOptions,
  KvGetResult,
  KvScanOptions,
} from "./domains/kv/types";

export { QueueClient } from "./domains/queue/client";
export type {
  EnqueueOptions,
  AvailabilityHandler,
  AvailabilityNotification,
  QueueItem,
  QueueSubscription,
} from "./domains/queue/types";
export { RpcClient } from "./domains/rpc/client";
export type {
  RequestOptions as RpcRequestOptions,
  ResponseFrame,
  InboundRequest,
  ResponseWriter,
  RpcHandler,
  RpcSubscription,
} from "./domains/rpc/types";
export { LeaseClient } from "./domains/lease/client";
export type {
  LeaseInfo,
  Lease,
  ChangeHandler,
  ChangeNotification,
  LeaseSubscription,
} from "./domains/lease/types";
export { NoticeClient } from "./domains/notice/client";
export type {
  NoticeMsg,
  NoticeHandler,
  NoticeSubscription,
} from "./domains/notice/types";
export { StreamClient } from "./domains/stream/client";
export type {
  StreamRecord,
  StreamMetadata,
  StreamSession,
  StreamCommitNotification,
  StreamCommitHandler,
  StreamSubscription,
} from "./domains/stream/types";
export { ScheduleClient } from "./domains/schedule/client";
export type {
  ScheduleEntry,
  ScheduleNotification,
  ScheduleHandler,
  ScheduleSubscription,
} from "./domains/schedule/types";
