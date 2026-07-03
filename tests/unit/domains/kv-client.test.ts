import { describe, expect, it } from "vite-plus/test";

import { BufferReader } from "../../../src/core/buffer";
import { ConnectionError } from "../../../src/core/errors";
import { MSG_KV_BEGIN, MSG_KV_GET } from "../../../src/frame/types";
import { KvClient } from "../../../src/domains/kv/client";

class FakeKvConnection {
  public lastRequest: { messageType: number; payload: Uint8Array } | null = null;
  public lastSignal: AbortSignal | undefined;
  private disconnectListeners = new Set<() => void>();

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

  disconnect(): void {
    for (const listener of this.disconnectListeners) {
      listener();
    }
  }
}

function expectBeginRoute(connection: FakeKvConnection, route: string): void {
  const lastRequest = connection.lastRequest;
  if (!lastRequest) {
    throw new Error("Expected BEGIN request to be recorded");
  }

  const reader = new BufferReader(lastRequest.payload);
  expect(reader.readString()).toBe(route);
}

describe("KvClient", () => {
  it("encodes the explicitly requested Sync durability in BEGIN payload", async () => {
    const connection = new FakeKvConnection();
    const client = new KvClient(connection);

    await client.begin("kv://realm/area/resource", { durability: "Sync" });

    expect(connection.lastRequest).not.toBeNull();
    expect(connection.lastRequest?.messageType).toBe(MSG_KV_BEGIN);

    const lastRequest = connection.lastRequest;
    if (!lastRequest) {
      throw new Error("Expected BEGIN request to be recorded");
    }

    const reader = new BufferReader(lastRequest.payload);
    reader.readString();
    reader.readU8();
    expect(reader.readU8()).toBe(1);
  });

  it("accepts bare server-legal KV routes", async () => {
    const connection = new FakeKvConnection();
    const client = new KvClient(connection);

    await client.begin("realm/area/resource", { durability: "Buffered" });

    const lastRequest = connection.lastRequest;
    if (!lastRequest) {
      throw new Error("Expected BEGIN request to be recorded");
    }

    const reader = new BufferReader(lastRequest.payload);
    expect(reader.readString()).toBe("realm/area/resource");
    reader.readU8();
    expect(reader.readU8()).toBe(0);
  });

  it("forwards wrong-scheme KV routes without local validation", async () => {
    const connection = new FakeKvConnection();
    const client = new KvClient(connection);

    await client.begin("queue://realm/area/resource", { durability: "Buffered" });

    expect(connection.lastRequest?.messageType).toBe(MSG_KV_BEGIN);
    expectBeginRoute(connection, "queue://realm/area/resource");
  });

  it("forwards empty-segment KV routes without local validation", async () => {
    const connection = new FakeKvConnection();
    const client = new KvClient(connection);

    await client.begin("kv://realm//resource", { durability: "Buffered" });

    expect(connection.lastRequest?.messageType).toBe(MSG_KV_BEGIN);
    expectBeginRoute(connection, "kv://realm//resource");
  });

  it("forwards wildcard KV routes without local validation", async () => {
    const connection = new FakeKvConnection();
    const client = new KvClient(connection);

    await client.begin("kv://realm/area/*", { durability: "Buffered" });

    expect(connection.lastRequest?.messageType).toBe(MSG_KV_BEGIN);
    expectBeginRoute(connection, "kv://realm/area/*");
  });

  it("rejects BEGIN when durability is omitted", async () => {
    const connection = new FakeKvConnection();
    const client = new KvClient(connection);

    await expect(client.begin("kv://realm/area/resource", {} as never)).rejects.toMatchObject({
      code: "KV_MISSING_DURABILITY",
    });
  });

  it("invalidates open transactions on disconnect", async () => {
    const connection = new FakeKvConnection();
    const client = new KvClient(connection);

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
    const client = new KvClient(connection);

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
});
