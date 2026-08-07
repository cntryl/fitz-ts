import { describe, expect, it } from "vite-plus/test";

import { createBufferReader, createBufferWriter } from "../../../src/core/buffer";
import { ConnectionError, KvError } from "../../../src/core/errors";
import {
  MSG_KV_BEGIN,
  MSG_KV_COMMIT,
  MSG_KV_GET,
  MSG_KV_NOTIFY,
  MSG_KV_ROLLBACK,
  MSG_KV_SCAN,
  MSG_KV_SUBSCRIBE,
  MSG_KV_UNSUBSCRIBE,
} from "../../../src/frame/types";
import { createKvClient } from "../../../src/domains/kv/client";

class FakeKvConnection {
  public lastRequest: { messageType: number; payload: Uint8Array } | null = null;
  public lastSignal: AbortSignal | undefined;
  public responses = new Map<number, Uint8Array[]>();
  private disconnectListeners = new Set<() => void>();
  private reconnectListeners = new Set<() => void | Promise<void>>();
  private notificationHandlers = new Map<number, (payload: Uint8Array) => void>();

  async request(
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    this.lastRequest = { messageType, payload };
    this.lastSignal = signal;

    if (signal?.aborted) {
      const error = new Error("The operation was aborted");
      error.name = "AbortError";
      throw error;
    }

    if (messageType === MSG_KV_BEGIN) {
      return new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    const queuedResponse = this.responses.get(messageType)?.shift();
    if (queuedResponse) {
      return queuedResponse;
    }

    if (messageType === MSG_KV_GET) {
      return await new Promise<Uint8Array>((_resolve, reject) => {
        const abort = () => {
          const error = new Error("The operation was aborted");
          error.name = "AbortError";
          reject(error);
        };

        signal?.addEventListener("abort", abort, { once: true });
      });
    }

    throw new ConnectionError("disconnected");
  }

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => {
      this.disconnectListeners.delete(listener);
    };
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

  disconnect(): void {
    for (const listener of this.disconnectListeners) {
      listener();
    }
  }

  respond(messageType: number, response: Uint8Array): void {
    const responses = this.responses.get(messageType);
    if (responses) {
      responses.push(response);
      return;
    }

    this.responses.set(messageType, [response]);
  }

  emitNotification(messageType: number, payload: Uint8Array): void {
    const handler = this.notificationHandlers.get(messageType);
    if (!handler) throw new Error(`No notification handler registered for ${messageType}`);
    handler(payload);
  }
}

function encodeSubscriptionResponse(subscriptionId: bigint): Uint8Array {
  const writer = createBufferWriter();
  writer.writeU8(0);
  writer.writeU64BE(subscriptionId);
  return writer.getBuffer();
}

function encodeKvNotification(
  subscriptionId: bigint,
  route: string,
  mutationCount: bigint,
): Uint8Array {
  const writer = createBufferWriter();
  writer.writeU64BE(subscriptionId);
  writer.writeString(route);
  writer.writeU64BE(mutationCount);
  return writer.getBuffer();
}

function encodeScanResponse(keys: Uint8Array[], hasMore: boolean): Uint8Array {
  const writer = createBufferWriter();
  writer.writeU8(0);
  writer.writeU32BE(keys.length);

  for (const key of keys) {
    writer.writeU32BE(key.length);
    writer.writeBytes(key);
    writer.writeU32BE(0);
  }

  writer.writeU8(hasMore ? 1 : 0);
  return writer.getBuffer();
}

async function expectKvRouteFailure(action: Promise<unknown>): Promise<void> {
  let caught: unknown;

  try {
    await action;
  } catch (error) {
    caught = error;
  }

  expect(caught).toBeInstanceOf(KvError);
  expect(caught).toMatchObject({ code: "KV_INVALID_ROUTE" });
}

describe("KvClient", () => {
  it("subscribes with a wildcard and delivers the exact concrete KV route", async () => {
    const connection = new FakeKvConnection();
    connection.respond(MSG_KV_SUBSCRIBE, encodeSubscriptionResponse(42n));
    connection.respond(MSG_KV_UNSUBSCRIBE, new Uint8Array([0]));
    const client = createKvClient(connection);
    let received: { route: string; mutationCount: bigint } | undefined;

    const subscription = await client.subscribe("kv://*/area/**", async (notification) => {
      received = notification;
    });
    connection.emitNotification(
      MSG_KV_NOTIFY,
      encodeKvNotification(42n, "kv://realm/area/resource", 3n),
    );
    await Promise.resolve();

    expect(subscription.subId).toBe(42n);
    expect(received).toEqual({ route: "kv://realm/area/resource", mutationCount: 3n });
    await subscription.unsubscribe();
    expect(connection.lastRequest?.messageType).toBe(MSG_KV_UNSUBSCRIBE);
  });

  it("preserves the broker KV subscription error code", async () => {
    const connection = new FakeKvConnection();
    const writer = createBufferWriter();
    writer.writeU8(1);
    writer.writeU32BE(1012);
    writer.writeString("invalid pattern");
    connection.respond(MSG_KV_SUBSCRIBE, writer.getBuffer());
    const client = createKvClient(connection);

    await expect(
      client.subscribe("kv://realm/area/resource", async () => undefined),
    ).rejects.toMatchObject({ domainCode: 1012 });
  });

  it("encodes the explicitly requested Sync durability in BEGIN payload", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    await client.begin("kv://realm/area/resource", { durability: "Sync" });

    expect(connection.lastRequest).not.toBeNull();
    expect(connection.lastRequest?.messageType).toBe(MSG_KV_BEGIN);

    const lastRequest = connection.lastRequest;
    if (!lastRequest) {
      throw new Error("Expected BEGIN request to be recorded");
    }

    const reader = createBufferReader(lastRequest.payload);
    reader.readString();
    reader.readU8();
    expect(reader.readU8()).toBe(1);
  });

  it("accepts bare server-legal KV routes", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    await client.begin("realm/area/resource", { durability: "Buffered" });

    const lastRequest = connection.lastRequest;
    if (!lastRequest) {
      throw new Error("Expected BEGIN request to be recorded");
    }

    const reader = createBufferReader(lastRequest.payload);
    expect(reader.readString()).toBe("realm/area/resource");
    reader.readU8();
    expect(reader.readU8()).toBe(0);
  });

  it("rejects wrong-scheme KV routes before sending", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    await expectKvRouteFailure(
      client.begin("queue://realm/area/resource", { durability: "Buffered" }),
    );

    expect(connection.lastRequest).toBeNull();
  });

  it("rejects empty-segment KV routes before sending", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    await expectKvRouteFailure(client.begin("kv://realm//resource", { durability: "Buffered" }));

    expect(connection.lastRequest).toBeNull();
  });

  it("rejects wildcard KV routes before sending", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    await expectKvRouteFailure(client.begin("kv://realm/area/*", { durability: "Buffered" }));

    expect(connection.lastRequest).toBeNull();
  });

  it("rejects BEGIN when durability is omitted", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    await expect(client.begin("kv://realm/area/resource", {} as never)).rejects.toMatchObject({
      code: "KV_MISSING_DURABILITY",
    });
  });

  it("should invalidate a KV handle given disconnect when the old handle is reused", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    const tx = await client.begin("kv://realm/area/resource", {
      durability: "Sync",
    });
    connection.disconnect();

    await expect(tx.get(new Uint8Array([1]))).rejects.toMatchObject({
      code: "KV_TX_CLOSED",
    });
  });

  it("cancels an in-flight kv transaction request", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    const tx = await client.begin("kv://realm/area/resource", {
      durability: "Sync",
    });
    const controller = new AbortController();
    const pending = tx.get(new Uint8Array([1]), controller.signal);

    await Promise.resolve();
    controller.abort();

    await expect(pending).rejects.toHaveProperty("name", "AbortError");
    expect(connection.lastSignal).toBe(controller.signal);
  });

  it("exposes scanPage hasMore and returns an explicitly limited scan", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    const tx = await client.begin("kv://realm/area/resource", {
      durability: "Sync",
    });
    connection.respond(MSG_KV_SCAN, encodeScanResponse([new Uint8Array([1])], true));

    await expect(tx.scanPage()).resolves.toEqual({
      entries: [{ key: new Uint8Array([1]), value: new Uint8Array() }],
      hasMore: true,
    });

    connection.respond(MSG_KV_SCAN, encodeScanResponse([new Uint8Array([2])], true));
    const limited: Uint8Array[] = [];
    for await (const entry of await tx.scan({ limit: 1 })) limited.push(entry.key);
    expect(limited).toEqual([new Uint8Array([2])]);

    connection.respond(MSG_KV_SCAN, encodeScanResponse([new Uint8Array([3])], true));
    await expect(tx.scan()).rejects.toMatchObject({
      code: "KV_SCAN_TRUNCATED",
    });
  });

  it("closes a transaction after a failed commit without rolling back during disposal", async () => {
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);

    const tx = await client.begin("kv://realm/area/resource", {
      durability: "Sync",
    });
    connection.respond(MSG_KV_COMMIT, new Uint8Array([3]));

    await expect(tx.commit()).rejects.toMatchObject({
      code: "KV_COMMIT",
    });
    expect(tx.isOpen()).toBe(false);
    await expect(tx.rollback()).resolves.toBeUndefined();
    await expect(tx[Symbol.asyncDispose]()).resolves.toBeUndefined();
    expect(tx.isOpen()).toBe(false);
    expect(connection.lastRequest?.messageType).toBe(MSG_KV_COMMIT);
  });

  it("should roll back an open transaction when asynchronously disposed", async () => {
    // Arrange
    const connection = new FakeKvConnection();
    const client = createKvClient(connection);
    const tx = await client.begin("kv://realm/area/resource", { durability: "Sync" });
    connection.respond(MSG_KV_ROLLBACK, new Uint8Array([0]));

    // Act
    await tx[Symbol.asyncDispose]();

    // Assert
    expect(tx.isOpen()).toBe(false);
    expect(connection.lastRequest?.messageType).toBe(MSG_KV_ROLLBACK);
  });
});
