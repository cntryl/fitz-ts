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
      "AsyncHandlerOptions",
      "ClientConfig",
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
      "ConnectionState",
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
      "RpcClient",
      "RpcRequestOptions",
      "ResponseFrame",
      "InboundRequest",
      "ResponseWriter",
      "RpcHandler",
      "RpcSubscription",
      "LeaseClient",
      "LeaseInfo",
      "Lease",
      "ChangeHandler",
      "ChangeNotification",
      "LeaseSubscription",
      "NoticeClient",
      "NoticeMsg",
      "NoticeHandler",
      "NoticeSubscription",
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
      "ScheduleClient",
      "ScheduleEntry",
      "ScheduleNotification",
      "ScheduleHandler",
      "ScheduleSubscription",
    ]);
  });

  it("keeps rpc worker request correlation ids private", () => {
    const source = readSource("../../src/domains/rpc/types.ts");
    expect(source).not.toContain("correlationId");
  });

  it("keeps queue item ids private and exposes subscription ids readonly", () => {
    const source = readSource("../../src/domains/queue/types.ts");
    expect(source).toContain("private id: bigint;");
    expect(source).toContain("private token: bigint;");
    expect(source).toContain("public readonly subId: bigint");
  });

  it("keeps lease tokens private and exposes subscription ids readonly", () => {
    const source = readSource("../../src/domains/lease/types.ts");
    expect(source).toContain("private token: bigint;");
    expect(source).toContain("public readonly subId: bigint");
  });

  it("exposes notice, schedule, and stream subscription ids as readonly", () => {
    expect(readSource("../../src/domains/notice/types.ts")).toContain(
      "public readonly subId: bigint",
    );
    const scheduleSource = readSource("../../src/domains/schedule/types.ts");
    expect(scheduleSource).toContain("public readonly subId: bigint");
    expect(scheduleSource).toContain("private readonly handler: ScheduleHandler");
    expect(readSource("../../src/domains/stream/types.ts")).toContain(
      "public readonly subId: bigint",
    );
  });
});
