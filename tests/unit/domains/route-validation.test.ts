import { describe, expect, it } from "vite-plus/test";

import type { Connection } from "../../../src/client/connection";
import { ConnectionState } from "../../../src/core/types";
import { LeaseError } from "../../../src/core/errors";
import { NoticeError } from "../../../src/core/errors";
import { QueueError } from "../../../src/core/errors";
import { RpcError } from "../../../src/core/errors";
import { StreamError } from "../../../src/core/errors";
import { MSG_NOTICE_SUBSCRIBE } from "../../../src/frame/types";
import { MSG_QUEUE_SUBSCRIBE } from "../../../src/frame/types";
import { MSG_STREAM_READ } from "../../../src/frame/types";
import { LeaseClient } from "../../../src/domains/lease/client";
import { NoticeClient } from "../../../src/domains/notice/client";
import { QueueClient } from "../../../src/domains/queue/client";
import { RpcClient } from "../../../src/domains/rpc/client";
import { StreamClient } from "../../../src/domains/stream/client";

class FakeConnection {
  public lastRequest: { messageType: number; payload: Uint8Array } | null = null;
  public readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  private readonly reconnectListeners = new Set<() => void | Promise<void>>();

  constructor(private readonly response: Uint8Array) {}

  async request(messageType: number, payload: Uint8Array, _signal?: AbortSignal): Promise<Uint8Array> {
    this.lastRequest = { messageType, payload };
    return this.response;
  }

  onReconnect(listener: () => void | Promise<void>): () => void {
    this.reconnectListeners.add(listener);
    return () => {
      this.reconnectListeners.delete(listener);
    };
  }

  registerNotificationHandler(messageType: number, handler: (payload: Uint8Array) => void): void {
    this.notificationHandlers.set(messageType, handler);
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

async function expectRouteValidationFailure(
  action: Promise<unknown>,
  expectedCode: string,
  messageFragment: string,
): Promise<void> {
  let resolved = false;
  let error: unknown = null;

  try {
    await action;
    resolved = true;
  } catch (caught) {
    error = caught;
  }

  expect(resolved).toBe(false);
  expect(error).not.toBeNull();
  expect(error).toMatchObject({ code: expectedCode });
  expect((error as Error).message).toContain(messageFragment);
}

describe("route validation", () => {
  it("rejects invalid lease routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new LeaseClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.acquire("lease://example/*", 30),
      "LEASE_INVALID_ROUTE",
      "expected lease://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid queue routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new QueueClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.enqueue("queue://example/app/*", new Uint8Array([1])),
      "QUEUE_INVALID_ROUTE",
      "expected queue://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid queue reserve patterns before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new QueueClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.reserve("queue://example/area/**", 30),
      "QUEUE_INVALID_ROUTE",
      "expected queue://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("accepts queue subscription realm wildcards", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = new QueueClient(connection as unknown as Connection);

    const subscription = await client.subscribe("queue://example/**", async () => undefined);

    expect(subscription.pattern).toBe("queue://example/**");
    expect(connection.lastRequest?.messageType).toBe(MSG_QUEUE_SUBSCRIBE);
  });

  it("rejects invalid notice routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new NoticeClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.publish("notice://example/**", new Uint8Array([1])),
      "NOTICE_INVALID_ROUTE",
      "expected notice://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("accepts notice subscription realm wildcards", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = new NoticeClient(connection as unknown as Connection);

    const subscription = await client.subscribe("notice://example/**", async () => undefined);

    expect(subscription.pattern).toBe("notice://example/**");
    expect(connection.lastRequest?.messageType).toBe(MSG_NOTICE_SUBSCRIBE);
  });

  it("rejects invalid rpc routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new RpcClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.call("rpc://example/*", new Uint8Array([1])),
      "RPC_INVALID_ROUTE",
      "expected rpc://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid rpc worker routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new RpcClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.registerWorker("rpc://example/**", async () => undefined),
      "RPC_INVALID_ROUTE",
      "expected rpc://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid stream routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new StreamClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.begin("stream://example/app/*"),
      "STREAM_INVALID_ROUTE",
      "expected stream://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid stream subscription patterns before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = new StreamClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.subscribe("stream://example/area/**", async () => undefined),
      "STREAM_INVALID_ROUTE",
      "expected stream://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("accepts stream realm wildcard selectors", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new StreamClient(connection as unknown as Connection);

    const records = await client.read("stream://example/**", 0n);

    expect(records).toEqual([]);
    expect(connection.lastRequest?.messageType).toBe(MSG_STREAM_READ);
  });

  it("rejects invalid stream begin routes before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = new StreamClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.begin("stream://example/area/**"),
      "STREAM_INVALID_ROUTE",
      "expected stream://",
    );
    expect(connection.lastRequest).toBeNull();
  });

  it("rejects invalid lease subscriptions before sending", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = new LeaseClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.subscribe("lease://example/**", async () => undefined),
      "LEASE_INVALID_ROUTE",
      "expected lease://",
    );
    expect(connection.lastRequest).toBeNull();
  });
});
