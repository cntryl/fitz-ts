import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  AuthenticationError,
  ConnectionError,
  TimeoutError,
  TransportError,
} from "../../../src/core/errors";
import { ConnectionState } from "../../../src/core/types";

const { createConnectionMock } = vi.hoisted(() => ({
  createConnectionMock: vi.fn(),
}));

vi.mock("../../../src/client/connection", () => ({
  Connection: vi.fn(),
  createConnection: createConnectionMock,
}));

import { createClient } from "../../../src/client/client";

class FakeOwnedConnection {
  public state = ConnectionState.Disconnected;
  public connectCalls = 0;
  public closeCalls = 0;
  public waitCalls = 0;
  public shouldWaitForReconnectValue = false;
  private connectImpl: ((options?: { signal?: AbortSignal }) => Promise<void>) | null = null;
  private waitImpl: ((signal?: AbortSignal, waitTimeoutMs?: number) => Promise<void>) | null = null;

  setConnectImpl(impl: (options?: { signal?: AbortSignal }) => Promise<void>): void {
    this.connectImpl = impl;
  }

  setWaitImpl(impl: (signal?: AbortSignal, waitTimeoutMs?: number) => Promise<void>): void {
    this.waitImpl = impl;
  }

  async connect(options?: { signal?: AbortSignal }): Promise<void> {
    this.connectCalls += 1;
    if (this.connectImpl) {
      return await this.connectImpl(options);
    }

    this.state = ConnectionState.Authenticated;
  }

  async waitUntilReady(signal?: AbortSignal, waitTimeoutMs?: number): Promise<void> {
    this.waitCalls += 1;
    if (this.waitImpl) {
      return await this.waitImpl(signal, waitTimeoutMs);
    }

    if (signal?.aborted) {
      const error = new Error("The operation was aborted");
      error.name = "AbortError";
      throw error;
    }
  }

  shouldWaitForReconnect(): boolean {
    return this.shouldWaitForReconnectValue;
  }

  async close(): Promise<void> {
    this.closeCalls += 1;
    this.state = ConnectionState.Closed;
  }

  isConnected(): boolean {
    return this.state === ConnectionState.Authenticated;
  }

  getState(): ConnectionState {
    return this.state;
  }

  getUrl(): string {
    return "ws://example.test";
  }

  async request(): Promise<Uint8Array> {
    return new Uint8Array();
  }

  async send(): Promise<void> {
    return;
  }

  async sendFireAndForget(): Promise<void> {
    return;
  }

  registerNotificationHandler(): void {}

  unregisterNotificationHandler(): void {}

  onReconnect(): () => void {
    return () => undefined;
  }

  onDisconnect(): () => void {
    return () => undefined;
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }
}

describe("Client lifecycle ownership", () => {
  beforeEach(() => {
    createConnectionMock.mockReset();
  });

  it("coalesces concurrent initial connect calls onto one owned connection", async () => {
    const connection = new FakeOwnedConnection();
    let releaseConnect: () => void = () => undefined;
    connection.setConnectImpl(async () => {
      await new Promise<void>((resolve) => {
        releaseConnect = () => {
          connection.state = ConnectionState.Authenticated;
          resolve();
        };
      });
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });
    const firstConnect = client.connect();
    const secondConnect = client.connect();

    expect(createConnectionMock).toHaveBeenCalledTimes(1);
    expect(connection.connectCalls).toBe(1);

    releaseConnect();
    await Promise.all([firstConnect, secondConnect]);

    expect(client.isConnected()).toBe(true);
  });

  it("waits on the existing reconnect path without replacing the owned connection", async () => {
    const connection = new FakeOwnedConnection();
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });
    await client.connect();
    const kvClient = client.kv();

    connection.state = ConnectionState.Reconnecting;
    connection.shouldWaitForReconnectValue = true;
    let releaseReconnect: () => void = () => undefined;
    connection.setWaitImpl(async () => {
      await new Promise<void>((resolve) => {
        releaseReconnect = () => {
          connection.state = ConnectionState.Authenticated;
          resolve();
        };
      });
    });

    const firstWait = client.connect();
    const secondWait = client.connect();

    expect(createConnectionMock).toHaveBeenCalledTimes(1);
    expect(connection.connectCalls).toBe(1);
    expect(connection.waitCalls).toBe(1);

    releaseReconnect();
    await Promise.all([firstWait, secondWait]);

    expect(client.kv()).toBe(kvClient);
    expect(client.isConnected()).toBe(true);
  });

  it("closes the owned connection once and stays terminal after close", async () => {
    const connection = new FakeOwnedConnection();
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });
    await client.connect();

    connection.state = ConnectionState.Reconnecting;
    connection.shouldWaitForReconnectValue = true;

    await client.close();

    expect(connection.closeCalls).toBe(1);
    expect(client.getState()).toBe(ConnectionState.Closed);
    await expect(client.connect()).rejects.toBeInstanceOf(ConnectionError);
    expect(createConnectionMock).toHaveBeenCalledTimes(1);
  });

  it("rejects an initial connect that resolves after close", async () => {
    const connection = new FakeOwnedConnection();
    let releaseConnect: () => void = () => undefined;
    connection.setConnectImpl(async () => {
      await new Promise<void>((resolve) => {
        releaseConnect = () => {
          connection.state = ConnectionState.Authenticated;
          resolve();
        };
      });
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });
    const connect = client.connect();

    await client.close();
    releaseConnect();

    await expect(connect).rejects.toBeInstanceOf(ConnectionError);
    expect(client.getState()).toBe(ConnectionState.Closed);
    expect(client.isConnected()).toBe(false);
  });

  it("rejects a reconnect wait that resolves after close", async () => {
    const connection = new FakeOwnedConnection();
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });
    await client.connect();

    connection.state = ConnectionState.Reconnecting;
    connection.shouldWaitForReconnectValue = true;
    let releaseReconnect: () => void = () => undefined;
    connection.setWaitImpl(async () => {
      await new Promise<void>((resolve) => {
        releaseReconnect = () => {
          connection.state = ConnectionState.Authenticated;
          resolve();
        };
      });
    });

    const waitingConnect = client.connect();

    await client.close();
    releaseReconnect();

    await expect(waitingConnect).rejects.toBeInstanceOf(ConnectionError);
    expect(client.getState()).toBe(ConnectionState.Closed);
    expect(client.isConnected()).toBe(false);
  });

  it("reuses the same owned connection after a failed initial connect", async () => {
    const connection = new FakeOwnedConnection();
    let connectAttempts = 0;
    connection.setConnectImpl(async () => {
      connectAttempts += 1;
      if (connectAttempts === 1) {
        throw new Error("dial failed");
      }

      connection.state = ConnectionState.Authenticated;
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });

    await expect(client.connect()).rejects.toThrow("dial failed");
    await expect(client.connect()).resolves.toBeUndefined();

    expect(createConnectionMock).toHaveBeenCalledTimes(1);
    expect(connection.connectCalls).toBe(2);
    expect(client.isConnected()).toBe(true);
  });

  it("does not retry a failed one-shot connect call", async () => {
    const connection = new FakeOwnedConnection();
    connection.setConnectImpl(async () => {
      throw new TransportError("dial failed");
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });

    await expect(client.connect()).rejects.toBeInstanceOf(TransportError);
    expect(connection.connectCalls).toBe(1);
    expect(client.isConnected()).toBe(false);
  });

  it("retries initial startup readiness failures until a later attempt connects", async () => {
    const connection = new FakeOwnedConnection();
    let connectAttempts = 0;
    connection.setConnectImpl(async () => {
      connectAttempts += 1;
      if (connectAttempts < 3) {
        throw new TransportError("dial failed");
      }

      connection.state = ConnectionState.Authenticated;
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });

    await expect(
      client.connectWhenReady({
        timeoutMs: 100,
        backoffMs: 1,
        maxBackoffMs: 1,
      }),
    ).resolves.toBeUndefined();

    expect(createConnectionMock).toHaveBeenCalledTimes(1);
    expect(connection.connectCalls).toBe(3);
    expect(client.isConnected()).toBe(true);
  });

  it("retries initial startup transport timeouts until a later attempt connects", async () => {
    const connection = new FakeOwnedConnection();
    let connectAttempts = 0;
    connection.setConnectImpl(async () => {
      connectAttempts += 1;
      if (connectAttempts < 3) {
        throw new TimeoutError("dial timed out");
      }

      connection.state = ConnectionState.Authenticated;
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });

    await expect(
      client.connectWhenReady({
        timeoutMs: 100,
        backoffMs: 1,
        maxBackoffMs: 1,
      }),
    ).resolves.toBeUndefined();

    expect(connection.connectCalls).toBe(3);
    expect(client.isConnected()).toBe(true);
  });

  it("enforces connectWhenReady timeout against an in-flight startup attempt", async () => {
    const connection = new FakeOwnedConnection();
    let receivedSignal: AbortSignal | undefined;
    connection.setConnectImpl(async (options) => {
      receivedSignal = options?.signal;
      await new Promise<void>((_resolve, reject) => {
        options?.signal?.addEventListener(
          "abort",
          () => {
            const error = new Error("The operation was aborted");
            error.name = "AbortError";
            reject(error);
          },
          { once: true },
        );
      });
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });

    await expect(
      client.connectWhenReady({
        timeoutMs: 10,
        backoffMs: 1,
        maxBackoffMs: 1,
      }),
    ).rejects.toBeInstanceOf(TimeoutError);

    expect(receivedSignal?.aborted).toBe(true);
    expect(connection.connectCalls).toBe(1);
    expect(client.isConnected()).toBe(false);
  });

  it("attempts once when connectWhenReady timeoutMs is zero", async () => {
    const connection = new FakeOwnedConnection();
    connection.setConnectImpl(async () => {
      connection.state = ConnectionState.Authenticated;
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });

    await expect(
      client.connectWhenReady({
        timeoutMs: 0,
        backoffMs: 1,
        maxBackoffMs: 1,
      }),
    ).resolves.toBeUndefined();

    expect(connection.connectCalls).toBe(1);
    expect(client.isConnected()).toBe(true);
  });

  it("rejects connectWhenReady with TimeoutError when total startup wait expires", async () => {
    const connection = new FakeOwnedConnection();
    connection.setConnectImpl(async () => {
      throw new TransportError("dial failed");
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });

    await expect(
      client.connectWhenReady({
        timeoutMs: 10,
        backoffMs: 1,
        maxBackoffMs: 1,
      }),
    ).rejects.toBeInstanceOf(TimeoutError);

    expect(connection.connectCalls).toBeGreaterThan(1);
    expect(client.isConnected()).toBe(false);
  });

  it("rejects connectWhenReady with AbortError when the caller aborts", async () => {
    const connection = new FakeOwnedConnection();
    connection.setConnectImpl(async () => {
      throw new TransportError("dial failed");
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });
    const controller = new AbortController();
    const abortTimer = setTimeout(() => {
      controller.abort();
    }, 10);

    try {
      await expect(
        client.connectWhenReady({
          signal: controller.signal,
          timeoutMs: 1000,
          backoffMs: 100,
          maxBackoffMs: 100,
        }),
      ).rejects.toHaveProperty("name", "AbortError");
    } finally {
      clearTimeout(abortTimer);
    }

    expect(connection.connectCalls).toBe(1);
    expect(client.isConnected()).toBe(false);
  });

  it("does not retry authentication failures in connectWhenReady", async () => {
    const connection = new FakeOwnedConnection();
    connection.setConnectImpl(async () => {
      throw new AuthenticationError("authentication rejected");
    });
    createConnectionMock.mockReturnValue(connection);

    const client = createClient({ url: "ws://example.test" });

    await expect(
      client.connectWhenReady({
        timeoutMs: 100,
        backoffMs: 1,
        maxBackoffMs: 1,
      }),
    ).rejects.toBeInstanceOf(AuthenticationError);

    expect(connection.connectCalls).toBe(1);
    expect(client.isConnected()).toBe(false);
  });
});
