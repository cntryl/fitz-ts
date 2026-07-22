import { describe, expect, it } from "vite-plus/test";

import type { Connection } from "../../../src/client/connection";
import {
  LeaseError,
  NoticeError,
  QueueError,
  RpcError,
  StreamError,
} from "../../../src/core/errors";
import { ConnectionState } from "../../../src/core/types";
import {
  MSG_NOTICE_SUBSCRIBE,
  MSG_QUEUE_SUBSCRIBE,
  MSG_STREAM_READ,
} from "../../../src/frame/types";
import { createLeaseClient } from "../../../src/domains/lease/client";
import { createNoticeClient } from "../../../src/domains/notice/client";
import { createQueueClient } from "../../../src/domains/queue/client";
import { createRpcClient } from "../../../src/domains/rpc/client";
import { createScheduleClient } from "../../../src/domains/schedule/client";
import { ScheduleError } from "../../../src/core/errors";
import { createStreamClient } from "../../../src/domains/stream/client";

class FakeConnection {
  public lastRequest: { messageType: number; payload: Uint8Array } | null = null;
  public readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  private readonly reconnectListeners = new Set<() => void | Promise<void>>();
  private readonly disconnectListeners = new Set<() => void>();
  private readonly multiplexer = {
    expectOptionalResponse: (_messageType: number) => () => undefined,
  };

  constructor(private readonly response: Uint8Array) {}

  async request(
    messageType: number,
    payload: Uint8Array,
    _signal?: AbortSignal,
  ): Promise<Uint8Array> {
    this.lastRequest = { messageType, payload };
    return this.response;
  }

  onReconnect(listener: () => void | Promise<void>): () => void {
    this.reconnectListeners.add(listener);
    return () => {
      this.reconnectListeners.delete(listener);
    };
  }

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => {
      this.disconnectListeners.delete(listener);
    };
  }

  registerNotificationHandler(messageType: number, handler: (payload: Uint8Array) => void): void {
    this.notificationHandlers.set(messageType, handler);
  }

  async sendFireAndForget(messageType: number, payload: Uint8Array): Promise<void> {
    this.lastRequest = { messageType, payload };
  }

  async send(messageType: number, payload: Uint8Array): Promise<void> {
    this.lastRequest = { messageType, payload };
  }

  getMultiplexer(): { expectOptionalResponse: (messageType: number) => () => void } {
    return this.multiplexer;
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }

  getState(): ConnectionState {
    return ConnectionState.Authenticated;
  }
}

function u64Bytes(value: bigint): Uint8Array {
  const buffer = new ArrayBuffer(8);
  new DataView(buffer).setBigUint64(0, value);
  return new Uint8Array(buffer);
}

type DomainErrorCtor = new (
  message: string,
  code: string,
  domainCode?: number,
  context?: Record<string, unknown>,
) => Error;

async function expectRouteValidationFailure(
  action: Promise<unknown>,
  expectedError: DomainErrorCtor,
  expectedCode: string,
  messageFragment: string,
): Promise<void> {
  let caught: unknown;

  try {
    await action;
  } catch (error) {
    caught = error;
  }

  expect(caught).toBeInstanceOf(expectedError);
  expect(caught).toMatchObject({ code: expectedCode });
  expect((caught as Error).message).toContain(messageFragment);
}

describe("route validation", () => {
  it("rejects invalid lease acquire routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(42n)]));
    const client = createLeaseClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.acquire("lease://example/*", 30),
      LeaseError,
      "LEASE_INVALID_ROUTE",
      "expected lease://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid queue enqueue routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0, ...u64Bytes(5n)]));
    const client = createQueueClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.enqueue("queue://example/app/*", new Uint8Array([1])),
      QueueError,
      "QUEUE_INVALID_ROUTE",
      "expected queue://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid queue reserve selectors before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = createQueueClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.reserve("queue://example/area/**", 30),
      QueueError,
      "QUEUE_INVALID_ROUTE",
      "expected queue://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("accepts queue subscription realm wildcards", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = createQueueClient(connection as unknown as Connection);

    const subscription = await client.subscribe("queue://example/**", async () => undefined);

    expect(subscription.pattern).toBe("queue://example/**");
    expect(connection.lastRequest?.messageType).toBe(MSG_QUEUE_SUBSCRIBE);
  });

  it("rejects invalid notice publish routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = createNoticeClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.publish("notice://example/**", new Uint8Array([1])),
      NoticeError,
      "NOTICE_INVALID_ROUTE",
      "expected notice://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("accepts notice subscription realm wildcards", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = createNoticeClient(connection as unknown as Connection);

    const subscription = await client.subscribe("notice://example/**", async () => undefined);

    expect(subscription.pattern).toBe("notice://example/**");
    expect(connection.lastRequest?.messageType).toBe(MSG_NOTICE_SUBSCRIBE);
  });

  it("rejects invalid rpc call routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = createRpcClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.call("rpc://example/*", new Uint8Array([1])),
      RpcError,
      "RPC_INVALID_ROUTE",
      "expected rpc://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid rpc worker routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = createRpcClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.registerWorker("rpc://example/**", async () => undefined),
      RpcError,
      "RPC_INVALID_ROUTE",
      "expected rpc://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid stream begin routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(11n)]));
    const client = createStreamClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.begin("stream://example/app/*"),
      StreamError,
      "STREAM_INVALID_ROUTE",
      "expected stream://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid stream subscription patterns before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = createStreamClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.subscribe("stream://example/area/**", async () => undefined),
      StreamError,
      "STREAM_INVALID_ROUTE",
      "expected stream://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("accepts stream realm wildcard selectors", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = createStreamClient(connection as unknown as Connection);

    const records = await client.read("stream://example/**", 0n);

    expect(records).toEqual([]);
    expect(connection.lastRequest?.messageType).toBe(MSG_STREAM_READ);
  });

  it("rejects invalid lease subscription patterns before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0, ...u64Bytes(7n)]));
    const client = createLeaseClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.subscribe("lease://example/**", async () => undefined),
      LeaseError,
      "LEASE_INVALID_ROUTE",
      "expected lease://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid schedule create routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = createScheduleClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.create("queue://example/app/jobs/run", "0 0 * * *", "broadcast"),
      ScheduleError,
      "SCHEDULE_INVALID_ROUTE",
      "expected schedule://",
    );
    expect(connection.lastRequest).toBeNull();
  });
});
