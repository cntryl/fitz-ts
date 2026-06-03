import { describe, expect, it, vi } from "vite-plus/test";

import { Connection } from "../../../src/client/connection";
import type { Transport } from "../../../src/transport/types";

class FakeTransport implements Transport {
  public connected = false;
  public sent: Uint8Array[] = [];
  private resolveReceive: ((data: Uint8Array) => void) | null = null;
  private rejectReceive: ((error: Error) => void) | null = null;

  async connect(): Promise<void> {
    this.connected = true;
  }

  async send(data: Uint8Array): Promise<void> {
    this.sent.push(data);
  }

  async receive(): Promise<Uint8Array> {
    return await new Promise<Uint8Array>((resolve, reject) => {
      this.resolveReceive = resolve;
      this.rejectReceive = reject;
    });
  }

  async close(): Promise<void> {
    this.connected = false;
    if (this.rejectReceive) {
      this.rejectReceive(new Error("closed"));
    }
  }

  getUrl(): string {
    return "ws://example.test";
  }

  isConnected(): boolean {
    return this.connected;
  }

  completeReceive(data: Uint8Array): void {
    if (this.resolveReceive) {
      this.resolveReceive(data);
      this.resolveReceive = null;
      this.rejectReceive = null;
    }
  }
}

describe("Connection lifecycle", () => {
  it("should expose a connection scope signal that aborts on close", async () => {
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      { authSettleDelayMs: 0 },
    );

    await connection.connect();
    const scope = connection.getScope?.();
    expect(scope).toBeDefined();
    expect(scope?.signal.aborted).toBe(false);

    await connection.close();
    expect(scope?.signal.aborted).toBe(true);
  });

  it("should wait for active async handlers when closing connection", async () => {
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        asyncHandlers: {
          maxConcurrency: 1,
          timeoutMs: 10000,
        },
      },
    );

    await connection.connect();

    let releaseTask: () => void = () => undefined;
    const taskStarted = vi.fn();
    const taskBlocked = new Promise<void>((resolve) => {
      releaseTask = resolve;
    });

    connection.dispatchAsyncHandler(async () => {
      taskStarted();
      await taskBlocked;
    });

    await vi.waitFor(() => {
      expect(taskStarted).toHaveBeenCalled();
    });

    let closeResolved = false;
    const closePromise = connection.close().then(() => {
      closeResolved = true;
    });

    await Promise.resolve();
    expect(closeResolved).toBe(false);

    releaseTask();
    await closePromise;
    expect(closeResolved).toBe(true);
  });

  it("should reject in-flight requests when connection is closed", async () => {
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      { authSettleDelayMs: 0 },
    );

    await connection.connect();

    const requestPromise = connection.request(92, new Uint8Array([1, 2, 3]));
    const requestExpectation = expect(requestPromise).rejects.toMatchObject({
      name: "ConnectionError",
    });

    await connection.close();
    await requestExpectation;
  });
});
