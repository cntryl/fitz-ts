import { describe, expect, it } from "vite-plus/test";

import { createLease } from "../../../src/domains/lease/types";
import { createQueueItem } from "../../../src/domains/queue/types";

class DisconnectableConnection {
  private readonly disconnectListeners = new Set<() => void>();

  async request(): Promise<Uint8Array> {
    throw new Error("request should not be reached after disconnect");
  }

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => {
      this.disconnectListeners.delete(listener);
    };
  }

  emitDisconnect(): void {
    for (const listener of this.disconnectListeners) {
      listener();
    }
  }
}

class LeaseConnection extends DisconnectableConnection {
  readonly tokens: bigint[] = [];
  private nextToken = 2n;

  override async request(_type?: number, payload?: Uint8Array): Promise<Uint8Array> {
    if (payload) {
      this.tokens.push(
        new DataView(payload.buffer, payload.byteOffset + payload.byteLength - 16, 8).getBigUint64(
          0,
        ),
      );
    }
    const result = new Uint8Array(9);
    new DataView(result.buffer).setBigUint64(1, this.nextToken++);
    return result;
  }
}

describe("stale handles", () => {
  it("fails queue item operations after disconnect", async () => {
    const connection = new DisconnectableConnection();
    const item = createQueueItem(
      1n,
      2n,
      new Uint8Array([1]),
      "queue://realm/area/resource",
      connection,
    );

    connection.emitDisconnect();

    await expect(item.extend(30)).rejects.toMatchObject({
      code: "QUEUE_ITEM_CLOSED",
    });
    await expect(item.complete()).rejects.toMatchObject({
      code: "QUEUE_ITEM_CLOSED",
    });
  });

  it("fails lease operations after disconnect", async () => {
    const connection = new DisconnectableConnection();
    const lease = createLease(1n, 2n, "lease://realm/area/resource", connection);

    connection.emitDisconnect();

    await expect(lease.extend(30)).rejects.toMatchObject({
      code: "LEASE_CLOSED",
    });
    await expect(lease.release()).rejects.toMatchObject({
      code: "LEASE_CLOSED",
    });
  });

  it("serializes lease extensions and rotates the fencing token", async () => {
    const connection = new LeaseConnection();
    const lease = createLease(1n, 2n, "lease://realm/area/resource", connection);

    await Promise.all([lease.extend(30), lease.extend(30)]);

    expect(connection.tokens).toEqual([1n, 2n]);
  });

  it("closes a lease before release transport completes", async () => {
    const connection = new LeaseConnection();
    const lease = createLease(1n, 2n, "lease://realm/area/resource", connection);

    await lease.release();

    await expect(lease.release()).rejects.toMatchObject({ code: "LEASE_CLOSED" });
  });
});
