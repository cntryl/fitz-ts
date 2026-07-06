/// <reference types="node" />

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import { Client } from "../../src/client/client";
import type {
  BrowserClient,
  BrowserTransportType,
  BrowserWebSocketOptions,
} from "../../src/client/browser-client";
import { Client as BrowserClientAlias } from "../../src/client/browser-client";
import { Connection } from "../../src/client/connection";
import { Multiplexer } from "../../src/client/multiplexer";
import { BufferReader, BufferWriter } from "../../src/core/buffer";
import { Deferred } from "../../src/core/types";
import { FrameParser } from "../../src/frame/codec";
import { KvClient } from "../../src/domains/kv/client";
import { LeaseClient } from "../../src/domains/lease/client";
import { NoticeClient } from "../../src/domains/notice/client";
import { QueueClient } from "../../src/domains/queue/client";
import { RpcClient } from "../../src/domains/rpc/client";
import { ScheduleClient } from "../../src/domains/schedule/client";
import { StreamClient } from "../../src/domains/stream/client";
import type { Client as BrowserFacadeClient } from "../../src/index.browser";

type IsCallable<T> = T extends (...args: never[]) => unknown ? true : false;
type IsNewable<T> = T extends abstract new (...args: never[]) => unknown ? true : false;
type IsCallableFactoryAlias<T> =
  IsCallable<T> extends true ? (IsNewable<T> extends false ? true : false) : false;

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
      "ErrCodeRpcTimeout",
      "ErrCodeRpcWorkerNotFound",
      "ErrCodeRpcBackpressure",
      "ErrCodeRpcRouteNotRegistered",
      "ErrCodeRpcCorrelationNotFound",
      "ErrCodeRpcInvalidSequence",
      "ErrCodeRpcDuplicateCorrelation",
      "ErrCodeRpcWrongWorker",
      "ErrCodeRpcUnauthorized",
      "ErrCodeRpcBackendError",
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
      "KvScanPage",
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
      "RegisterWorkerOptions",
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

  it("keeps browser client config browser-safe", () => {
    expectTypeOf<BrowserFacadeClient>().toEqualTypeOf<BrowserClient>();
    expectTypeOf<BrowserClient["config"]["transport"]>().toEqualTypeOf<BrowserTransportType>();
    expectTypeOf<BrowserClient["config"]["webSocket"]>().toEqualTypeOf<BrowserWebSocketOptions>();
  });

  it("exports callable factory aliases without constructor signatures", () => {
    expectTypeOf<IsCallableFactoryAlias<typeof Client>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof BrowserClientAlias>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof Connection>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof Multiplexer>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof FrameParser>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof Deferred>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof BufferWriter>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof BufferReader>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof KvClient>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof QueueClient>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof RpcClient>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof LeaseClient>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof NoticeClient>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof StreamClient>>().toEqualTypeOf<true>();
    expectTypeOf<IsCallableFactoryAlias<typeof ScheduleClient>>().toEqualTypeOf<true>();
  });
});
