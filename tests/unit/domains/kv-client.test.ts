import { describe, expect, it } from "vitest";

import { BufferReader } from "../../../src/core/buffer";
import { ConnectionError } from "../../../src/core/errors";
import { MSG_KV_BEGIN } from "../../../src/frame/types";
import { KvClient } from "../../../src/domains/kv/client";
import type { Connection } from "../../../src/client/connection";

class FakeKvConnection {
  public lastRequest: { messageType: number; payload: Uint8Array } | null =
    null;
  private disconnectListeners = new Set<() => void>();

  async request(messageType: number, payload: Uint8Array): Promise<Uint8Array> {
    this.lastRequest = { messageType, payload };

    if (messageType === MSG_KV_BEGIN) {
      return new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 1]);
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

describe("KvClient", () => {
  it("uses Sync durability by default in BEGIN payload", async () => {
    const connection = new FakeKvConnection();
    const client = new KvClient(connection as unknown as Connection);

    await client.begin("kv://realm/area/resource");

    expect(connection.lastRequest).not.toBeNull();
    expect(connection.lastRequest?.messageType).toBe(MSG_KV_BEGIN);

    const reader = new BufferReader(connection.lastRequest!.payload);
    reader.readString();
    reader.readU8();
    expect(reader.readU8()).toBe(1);
  });

  it("invalidates open transactions on disconnect", async () => {
    const connection = new FakeKvConnection();
    const client = new KvClient(connection as unknown as Connection);

    const tx = await client.begin("kv://realm/area/resource");
    connection.disconnect();

    await expect(tx.get(new Uint8Array([1]))).rejects.toMatchObject({
      code: "KV_TX_CLOSED",
    });
  });
});
