import { describe, expect, it } from "vite-plus/test";

import { ConnectionState } from "../../../src/core/types";
import { ConnectionError } from "../../../src/core/errors";
import { MSG_RPC_REQUEST, MSG_RPC_RESPONSE } from "../../../src/frame/types";
import { RpcClient } from "../../../src/domains/rpc/client";
import { RpcCodec } from "../../../src/domains/rpc/codec";
import type { Connection } from "../../../src/client/connection";

class FakeRpcConnection {
  public readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  public lastRequest: { messageType: number; payload: Uint8Array } | undefined;
  public sendCalls: Array<{ messageType: number; payload: Uint8Array }> = [];
  public lastSignal: AbortSignal | undefined;
  private state = ConnectionState.Authenticated;
  private readonly disconnectListeners = new Set<() => void>();

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

    if (messageType === 300 || messageType === 301 || messageType === 302) {
      return new Uint8Array([0]);
    }
    throw new Error(`Unexpected request message type ${messageType}`);
  }

  async send(messageType: number, payload: Uint8Array): Promise<void> {
    this.sendCalls.push({ messageType, payload });
    if (this.state !== ConnectionState.Authenticated) {
      throw new ConnectionError(`Cannot use connection while state is ${this.state}`);
    }
  }

  registerNotificationHandler(messageType: number, handler: (payload: Uint8Array) => void): void {
    this.notificationHandlers.set(messageType, handler);
  }

  unregisterNotificationHandler(messageType: number): void {
    this.notificationHandlers.delete(messageType);
  }

  onReconnect(): () => void {
    return () => undefined;
  }

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => {
      this.disconnectListeners.delete(listener);
    };
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }

  getState(): ConnectionState {
    return this.state;
  }

  setState(state: ConnectionState): void {
    this.state = state;
  }

  emitDisconnect(): void {
    for (const listener of this.disconnectListeners) {
      listener();
    }
  }
}

describe("RpcClient", () => {
  it("swallows worker response sends after the connection closes", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection as unknown as Connection);
    const route = "rpc://realm/area/method";

    await client.registerWorker(route, async (_req, writer) => {
      await writer.send(new Uint8Array([1]), true);
    });

    connection.setState(ConnectionState.Closed);

    const handler = connection.notificationHandlers.get(MSG_RPC_REQUEST);
    expect(handler).toBeTypeOf("function");

    if (!handler) {
      throw new Error("Expected RPC request handler to be registered");
    }

    handler(RpcCodec.encodeRequest(new Uint8Array(16), route, "", new Uint8Array([9])));

    await Promise.resolve();
    await Promise.resolve();

    expect(connection.sendCalls).toHaveLength(1);
    expect(connection.sendCalls[0].messageType).toBe(303);
  });

  it("forwards request cancellation to the connection layer", async () => {
    const controller = new AbortController();
    controller.abort();
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection as unknown as Connection);

    await expect(
      client.call("rpc://realm/area/method", new Uint8Array([1]), {
        signal: controller.signal,
      }),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(connection.lastSignal).toBe(controller.signal);
  });

  it("delivers a terminal RPC response frame that also carries a body", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection as unknown as Connection);

    const iterator = await client.call("rpc://realm/area/method", new Uint8Array([1]));

    const request = connection.lastRequest;
    if (!request) {
      throw new Error("Expected RPC request payload to be recorded");
    }

    const decoded = RpcCodec.decodeInboundRequest(request.payload);
    const responseHandler = connection.notificationHandlers.get(MSG_RPC_RESPONSE);
    expect(responseHandler).toBeTypeOf("function");

    if (!responseHandler) {
      throw new Error("Expected RPC response handler to be registered");
    }

    responseHandler(
      RpcCodec.encodeResponse(decoded.correlationId, 0n, new Uint8Array([7, 8, 9]), true),
    );

    await expect(iterator.next()).resolves.toMatchObject({
      done: false,
      value: { body: new Uint8Array([7, 8, 9]), sequence: 0n },
    });
    await expect(iterator.next()).resolves.toMatchObject({ done: true });
  });

  it("fails a pending iterator when the connection disconnects", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection as unknown as Connection);

    const iterator = await client.call("rpc://realm/area/method", new Uint8Array([1]));

    const nextPromise = iterator.next();
    connection.emitDisconnect();

    await expect(nextPromise).rejects.toMatchObject({
      name: "ConnectionError",
    });
  });
});
