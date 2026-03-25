import { describe, expect, it } from "vitest";

import { ConnectionError } from "../../../src/core/errors";
import { MSG_STREAM_BEGIN } from "../../../src/frame/types";
import { StreamClient } from "../../../src/domains/stream/client";
import type { Connection } from "../../../src/client/connection";

class FakeStreamConnection {
  private disconnectListeners = new Set<() => void>();

  async request(messageType: number): Promise<Uint8Array> {
    if (messageType === MSG_STREAM_BEGIN) {
      return new Uint8Array([0, 1, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    throw new ConnectionError("disconnected");
  }

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => {
      this.disconnectListeners.delete(listener);
    };
  }

  onReconnect(): () => void {
    return () => undefined;
  }

  disconnect(): void {
    for (const listener of this.disconnectListeners) {
      listener();
    }
  }
}

describe("StreamClient", () => {
  it("invalidates open stream sessions on disconnect", async () => {
    const connection = new FakeStreamConnection();
    const client = new StreamClient(connection as unknown as Connection);

    const session = await client.begin("stream://realm/area/resource", 0n);
    connection.disconnect();

    await expect(session.append(new Uint8Array([1]))).rejects.toMatchObject({
      code: "STREAM_SESSION_CLOSED",
    });
  });
});
