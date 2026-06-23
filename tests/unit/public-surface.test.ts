/// <reference types="node" />

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vite-plus/test";

function collectExportNames(source: string): string[] {
  const names: string[] = [];
  const re = /export\s+(?:type\s+)?\{([\s\S]*?)\}\s+from\s+["'][^"']+["'];/g;

  for (const match of source.matchAll(re)) {
    const block = match[1];
    for (const rawPart of block.split(",")) {
      const part = rawPart.trim();
      if (!part) {
        continue;
      }

      const aliasMatch = part.match(/^(?:type\s+)?([A-Za-z0-9_]+)\s+as\s+([A-Za-z0-9_]+)$/);
      if (aliasMatch) {
        names.push(aliasMatch[2]);
        continue;
      }

      names.push(part.replace(/^type\s+/, "").trim());
    }
  }

  return names;
}

function readSource(relativePath: string): string {
  const filePath = fileURLToPath(new URL(relativePath, import.meta.url));
  return readFileSync(filePath, "utf8");
}

describe("public surface", () => {
  it("keeps the root export inventory stable", () => {
    const source = readSource("../../src/index.ts");
    expect(collectExportNames(source)).toEqual([
      "Client",
      "createClient",
      "AsyncHandlerOptions",
      "ClientConfig",
      "ClientConnectOptions",
      "HeartbeatOptions",
      "WebSocketOptions",
      "FitzLifecycleEvent",
      "FitzLogger",
      "FitzLogLevel",
      "FitzMeter",
      "FitzObservability",
      "FitzSpan",
      "FitzTracer",
      "TransportType",
      "TokenProvider",
      "ReconnectOptions",
      "RetryOptions",
      "ConnectionState",
      "createWakeGate",
      "WakeGate",
      "WakeWaitOptions",
      "ErrKvTransactionAborted",
      "ErrKvLeaseExpired",
      "ErrKvConflictingWrite",
      "ErrKvKeyNotFound",
      "ErrKvOperationNotAllowed",
      "ErrCodeKvIsolationConflict",
      "ErrCodeKvBackendError",
      "ErrQueueNotFound",
      "ErrQueueMessageNotFound",
      "ErrQueueInvalidToken",
      "ErrQueueFull",
      "ErrQueueInvalidDelay",
      "ErrCodeQueueFull",
      "ErrRpcTimeout",
      "ErrRpcHandlerNotFound",
      "ErrRpcHandlerError",
      "ErrRpcInvalidRequest",
      "ErrCodeRpcTimeout",
      "ErrCodeRpcWorkerNotFound",
      "ErrCodeRpcBackpressure",
      "ErrCodeRpcRouteNotRegistered",
      "ErrCodeRpcCorrelationNotFound",
      "ErrCodeRpcUnauthorized",
      "ErrLeaseHeld",
      "ErrLeaseNotFound",
      "ErrLeaseInvalidToken",
      "ErrCodeLeaseHeld",
      "ErrNoticeGeneral",
      "ErrStreamNotFound",
      "ErrStreamOffsetOutOfRange",
      "ErrStreamInvalidOffset",
      "ErrStreamFull",
      "ErrStreamSessionNotFound",
      "ErrStreamSessionClosed",
      "ErrStreamExpectedOffsetMismatch",
      "ErrScheduleNotFound",
      "ErrScheduleTaskNotFound",
      "ErrScheduleInvalidCron",
      "ErrScheduleInvalidDelay",
      "ErrScheduleInvalidTimestamp",
      "FitzError",
      "TransportError",
      "ConnectionError",
      "RequestQueueFullError",
      "AuthenticationError",
      "TimeoutError",
      "ProtocolError",
      "CodecError",
      "KvError",
      "QueueError",
      "NoticeError",
      "RpcError",
      "LeaseError",
      "StreamError",
      "ScheduleError",
      "isRetryable",
      "createTaskGroup",
      "TaskGroup",
      "TaskGroupStatus",
      "TaskGroupErrorPolicy",
      "TaskContext",
      "TaskGroupOptions",
      "KvClient",
      "KvTransaction",
      "TxMode",
      "DurabilityMode",
      "KvBeginOptions",
      "KvGetResult",
      "KvScanOptions",
      "QueueClient",
      "EnqueueOptions",
      "AvailabilityHandler",
      "AvailabilityNotification",
      "QueueItem",
      "QueueSubscription",
      "QueueStatus",
      "RpcClient",
      "RpcRequestOptions",
      "ResponseFrame",
      "InboundRequest",
      "ResponseWriter",
      "RpcHandler",
      "RpcSubscription",
      "RpcStatus",
      "LeaseClient",
      "LeaseInfo",
      "Lease",
      "ChangeHandler",
      "ChangeNotification",
      "LeaseSubscription",
      "LeaseStatus",
      "NoticeClient",
      "NoticeMsg",
      "NoticeHandler",
      "NoticeSubscription",
      "NoticeStatus",
      "StreamClient",
      "StreamRecord",
      "StreamMetadata",
      "StreamDiscriminator",
      "StreamFilteredReason",
      "StreamFilterClause",
      "StreamFilterSet",
      "StreamAppendOptions",
      "StreamReadOptions",
      "StreamReadCursor",
      "StreamReadEvent",
      "StreamReadFiltered",
      "StreamReadFilteredRange",
      "StreamReadItem",
      "StreamReadPage",
      "StreamSession",
      "StreamCommitNotification",
      "StreamCommitHandler",
      "StreamSubscription",
      "StreamCommitMode",
      "StreamStatus",
      "ScheduleClient",
      "ScheduleEntry",
      "ScheduleNotification",
      "ScheduleHandler",
      "ScheduleSubscription",
      "ScheduleStatus",
    ]);
  });

  it("keeps rpc worker request correlation ids private", () => {
    const source = readSource("../../src/domains/rpc/types.ts");
    expect(source).not.toContain("correlationId");
  });

  it("keeps queue item ids private and exposes subscription factories", () => {
    const source = readSource("../../src/domains/queue/types.ts");
    expect(source).not.toContain("private id: bigint;");
    expect(source).not.toContain("private token: bigint;");
    expect(source).toContain("export type QueueItem = ReturnType<typeof createQueueItem>");
    expect(source).toContain("export function createQueueItem(");
    expect(source).toContain(
      "export type QueueSubscription = ReturnType<typeof createQueueSubscription>",
    );
    expect(source).toContain("export function createQueueSubscription(");
  });

  it("keeps lease tokens private and exposes lease factories", () => {
    const source = readSource("../../src/domains/lease/types.ts");
    expect(source).not.toContain("private token: bigint;");
    expect(source).toContain("export type Lease = ReturnType<typeof createLease>");
    expect(source).toContain("export function createLease(");
    expect(source).toContain(
      "export type LeaseSubscription = ReturnType<typeof createLeaseSubscription>",
    );
    expect(source).toContain("export function createLeaseSubscription(");
  });

  it("exposes notice, schedule, and stream subscription factories", () => {
    const noticeSource = readSource("../../src/domains/notice/types.ts");
    expect(noticeSource).toContain("export function createNoticeSubscription(");

    const scheduleSource = readSource("../../src/domains/schedule/types.ts");
    expect(scheduleSource).toContain("export function createScheduleSubscription(");

    const streamSource = readSource("../../src/domains/stream/types.ts");
    expect(streamSource).toContain("export function createStreamSubscription(");
  });
});
