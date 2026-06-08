import { describe, expect, it } from "vite-plus/test";

import type { Connection } from "../../../src/client/connection";
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

describe("stale handles", () => {
  it("fails queue item operations after disconnect", async () => {
    const connection = new DisconnectableConnection();
    const item = createQueueItem(
      1n,
      2n,
      new Uint8Array([1]),
      "queue://realm/area/resource",
      connection as unknown as Connection,
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
    const lease = createLease(
      1n,
      2n,
      "lease://realm/area/resource",
      connection as unknown as Connection,
    );

    connection.emitDisconnect();

    await expect(lease.extend(30)).rejects.toMatchObject({
      code: "LEASE_CLOSED",
    });
    await expect(lease.release()).rejects.toMatchObject({
      code: "LEASE_CLOSED",
    });
  });
});
