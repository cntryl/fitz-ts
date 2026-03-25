import { describe, expect, it } from "vite-plus/test";

import { ConnectionError } from "../../../src/core/errors";
import { MSG_STREAM_APPEND, MSG_STREAM_BEGIN } from "../../../src/frame/types";
import { StreamClient } from "../../../src/domains/stream/client";
import type { Connection } from "../../../src/client/connection";

class FakeStreamConnection {
  public lastSignal: AbortSignal | undefined;
  private disconnectListeners = new Set<() => void>();

  async request(
    messageType: number,
    _payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    this.lastSignal = signal;

    if (signal?.aborted) {
      const error = new Error("The operation was aborted");
      error.name = "AbortError";
      throw error;
    }

    if (messageType === MSG_STREAM_BEGIN) {
      return new Uint8Array([0, 1, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    if (messageType === MSG_STREAM_APPEND) {
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

  it("cancels an in-flight stream append request", async () => {
    const connection = new FakeStreamConnection();
    const client = new StreamClient(connection as unknown as Connection);

    const session = await client.begin("stream://realm/area/resource", 0n);
    const controller = new AbortController();
    const pending = session.append(new Uint8Array([1]), controller.signal);

    await Promise.resolve();
    controller.abort();

    await expect(pending).rejects.toHaveProperty("name", "AbortError");
    expect(connection.lastSignal).toBe(controller.signal);
  });
});
