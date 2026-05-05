import { describe, expect, it } from "vite-plus/test";

import { BufferReader } from "../../../src/core/buffer";
import { ConnectionError } from "../../../src/core/errors";
import { MSG_STREAM_APPEND, MSG_STREAM_BEGIN, MSG_STREAM_READ } from "../../../src/frame/types";
import { StreamClient } from "../../../src/domains/stream/client";
import type { Connection } from "../../../src/client/connection";
import type { StreamFilterSet } from "../../../src/domains/stream/types";

class FakeStreamConnection {
  public lastSignal: AbortSignal | undefined;
  public lastPayload: Uint8Array | undefined;
  private disconnectListeners = new Set<() => void>();

  constructor(private readonly appendMode: "pending" | "success" = "pending") {}

  async request(
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    this.lastPayload = payload;
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
      if (this.appendMode === "success") {
        return new Uint8Array([0]);
      }

      return await new Promise<Uint8Array>((_resolve, reject) => {
        const abort = () => {
          const error = new Error("The operation was aborted");
          error.name = "AbortError";
          reject(error);
        };

        signal?.addEventListener("abort", abort, { once: true });
      });
    }

    if (messageType === MSG_STREAM_READ) {
      return new Uint8Array([0]);
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

    const session = await client.begin("stream://realm/area/resource");
    connection.disconnect();

    await expect(session.append(0n, new Uint8Array([1]))).rejects.toMatchObject({
      code: "STREAM_SESSION_CLOSED",
    });
  });

  it("cancels an in-flight stream append request", async () => {
    const connection = new FakeStreamConnection();
    const client = new StreamClient(connection as unknown as Connection);

    const session = await client.begin("stream://realm/area/resource");
    const controller = new AbortController();
    const pending = session.append(0n, new Uint8Array([1]), controller.signal);

    await Promise.resolve();
    controller.abort();

    await expect(pending).rejects.toHaveProperty("name", "AbortError");
    expect(connection.lastSignal).toBe(controller.signal);
  });

  it("encodes append discriminator options", async () => {
    const connection = new FakeStreamConnection("success");
    const client = new StreamClient(connection as unknown as Connection);

    const session = await client.begin("stream://realm/area/resource");
    const offset = await session.append(0n, new Uint8Array([1, 2]), {
      discriminator: "proj.alpha",
    });

    expect(offset).toBe(0n);
    const reader = new BufferReader(connection.lastPayload ?? new Uint8Array());
    expect(reader.readU64BE()).toBe(1n);
    expect(reader.readU64BE()).toBe(0n);
    expect(reader.readU32BE()).toBe(2);
    expect(Array.from(reader.readBytes(2))).toEqual([1, 2]);
    expect(reader.readU8()).toBe(0);
    expect(reader.readU8()).toBe(1);
    expect(reader.readString()).toBe("proj.alpha");
    expect(reader.isEOF()).toBe(true);
  });

  it("encodes read filter options", async () => {
    const connection = new FakeStreamConnection("success");
    const client = new StreamClient(connection as unknown as Connection);

    const filter: StreamFilterSet = {
      clauses: [{ kind: "Equals", value: "proj.alpha" }],
    };

    const records = await client.read("stream://realm/area/resource", 5n, 10, { filter });

    expect(records).toEqual([]);
    const reader = new BufferReader(connection.lastPayload ?? new Uint8Array());
    expect(reader.readRoute()).toBe("stream://realm/area/resource");
    expect(reader.readU64BE()).toBe(5n);
    expect(reader.readU64BE()).toBe(10n);
    expect(reader.readU8()).toBe(1);
    const filterLength = reader.readU32BE();
    expect(filterLength).toBeGreaterThan(0);
    expect(reader.readBytes(filterLength)).toBeInstanceOf(Uint8Array);
    expect(reader.isEOF()).toBe(true);
  });
});
