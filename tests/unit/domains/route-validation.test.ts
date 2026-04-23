import { describe, expect, it } from "vite-plus/test";

import type { Connection } from "../../../src/client/connection";
import { ConnectionState } from "../../../src/core/types";
import {
  MSG_LEASE_ACQUIRE,
  MSG_LEASE_SUBSCRIBE,
  MSG_NOTICE_PUBLISH,
  MSG_NOTICE_SUBSCRIBE,
  MSG_QUEUE_ENQUEUE,
  MSG_QUEUE_RESERVE,
  MSG_QUEUE_SUBSCRIBE,
  MSG_RPC_REQUEST,
  MSG_RPC_SUBSCRIBE_WORKER,
  MSG_SCHEDULE_CREATE,
  MSG_STREAM_BEGIN,
  MSG_STREAM_READ,
  MSG_STREAM_SUBSCRIBE,
} from "../../../src/frame/types";
import { LeaseClient } from "../../../src/domains/lease/client";
import { NoticeClient } from "../../../src/domains/notice/client";
import { QueueClient } from "../../../src/domains/queue/client";
import { RpcClient } from "../../../src/domains/rpc/client";
import { ScheduleClient } from "../../../src/domains/schedule/client";
import { StreamClient } from "../../../src/domains/stream/client";

class FakeConnection {
  public lastRequest: { messageType: number; payload: Uint8Array } | null = null;
  public readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  private readonly reconnectListeners = new Set<() => void | Promise<void>>();
  private readonly disconnectListeners = new Set<() => void>();
  private readonly multiplexer = {
    expectOptionalResponse: (_messageType: number) => () => undefined,
  };

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

describe("route forwarding", () => {
  it("forwards lease acquire routes without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(42n)]));
    const client = new LeaseClient(connection as unknown as Connection);

    const lease = await client.acquire("lease://example/*", 30);

    expect(lease.route).toBe("lease://example/*");
    expect(lease.token).toBe(42n);
    expect(connection.lastRequest?.messageType).toBe(MSG_LEASE_ACQUIRE);
  });

  it("forwards queue enqueue routes without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0, ...u64Bytes(5n)]));
    const client = new QueueClient(connection as unknown as Connection);

    const messageId = await client.enqueue("queue://example/app/*", new Uint8Array([1]));

    expect(messageId).toBe(5n);
    expect(connection.lastRequest?.messageType).toBe(MSG_QUEUE_ENQUEUE);
  });

  it("forwards queue reserve selectors without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new QueueClient(connection as unknown as Connection);

    const items = await client.reserve("queue://example/area/**", 30);

    expect(items).toEqual([]);
    expect(connection.lastRequest?.messageType).toBe(MSG_QUEUE_RESERVE);
  });

  it("accepts queue subscription realm wildcards", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = new QueueClient(connection as unknown as Connection);

    const subscription = await client.subscribe("queue://example/**", async () => undefined);

    expect(subscription.pattern).toBe("queue://example/**");
    expect(connection.lastRequest?.messageType).toBe(MSG_QUEUE_SUBSCRIBE);
  });

  it("forwards notice publish routes without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new NoticeClient(connection as unknown as Connection);

    await client.publish("notice://example/**", new Uint8Array([1]));

    expect(connection.lastRequest?.messageType).toBe(MSG_NOTICE_PUBLISH);
  });

  it("accepts notice subscription realm wildcards", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = new NoticeClient(connection as unknown as Connection);

    const subscription = await client.subscribe("notice://example/**", async () => undefined);

    expect(subscription.pattern).toBe("notice://example/**");
    expect(connection.lastRequest?.messageType).toBe(MSG_NOTICE_SUBSCRIBE);
  });

  it("forwards rpc call routes without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new RpcClient(connection as unknown as Connection);

    const iterator = await client.call("rpc://example/*", new Uint8Array([1]));

    expect(iterator).toBeDefined();
    expect(connection.lastRequest?.messageType).toBe(MSG_RPC_REQUEST);
  });

  it("forwards rpc worker routes without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new RpcClient(connection as unknown as Connection);

    const subscription = await client.registerWorker("rpc://example/**", async () => undefined);

    expect(subscription).toBeDefined();
    expect(connection.lastRequest?.messageType).toBe(MSG_RPC_SUBSCRIBE_WORKER);
  });

  it("forwards stream begin routes without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(11n)]));
    const client = new StreamClient(connection as unknown as Connection);

    const session = await client.begin("stream://example/app/*");

    expect(session).toBeDefined();
    expect(connection.lastRequest?.messageType).toBe(MSG_STREAM_BEGIN);
  });

  it("forwards stream subscription patterns without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0, 1, ...u64Bytes(7n)]));
    const client = new StreamClient(connection as unknown as Connection);

    const subscription = await client.subscribe("stream://example/area/**", async () => undefined);

    expect(subscription.pattern).toBe("stream://example/area/**");
    expect(connection.lastRequest?.messageType).toBe(MSG_STREAM_SUBSCRIBE);
  });

  it("accepts stream realm wildcard selectors", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new StreamClient(connection as unknown as Connection);

    const records = await client.read("stream://example/**", 0n);

    expect(records).toEqual([]);
    expect(connection.lastRequest?.messageType).toBe(MSG_STREAM_READ);
  });

  it("forwards lease subscription patterns without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0, ...u64Bytes(7n)]));
    const client = new LeaseClient(connection as unknown as Connection);

    const subscription = await client.subscribe("lease://example/**", async () => undefined);

    expect(subscription).toBeDefined();
    expect(connection.lastRequest?.messageType).toBe(MSG_LEASE_SUBSCRIBE);
  });

  it("forwards schedule routes without local validation", async () => {
    const connection = new FakeConnection(new Uint8Array([0]));
    const client = new ScheduleClient(connection as unknown as Connection);

    const route = await client.create("queue://example/app/jobs/run", "0 0 * * *");

    expect(route).toBe("queue://example/app/jobs/run");
    expect(connection.lastRequest?.messageType).toBe(MSG_SCHEDULE_CREATE);
  });
});
