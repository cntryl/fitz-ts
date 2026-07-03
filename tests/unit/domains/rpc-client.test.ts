import { describe, expect, it, vi } from "vite-plus/test";

import { ConnectionState } from "../../../src/core/types";
import {
  ConnectionError,
  ErrCodeRpcBackpressure,
  ErrCodeRpcRouteNotRegistered,
  ErrCodeRpcWorkerNotFound,
  RpcError,
} from "../../../src/core/errors";
import { BufferReader, BufferWriter, utf8Decoder, utf8Encoder } from "../../../src/core/buffer";
import {
  MSG_RPC_REQUEST,
  MSG_RPC_RESPONSE,
  MSG_RPC_SUBSCRIBE_WORKER,
  MSG_RPC_UNSUBSCRIBE_WORKER,
} from "../../../src/frame/types";
import { RpcClient } from "../../../src/domains/rpc/client";
import { RpcCodec } from "../../../src/domains/rpc/codec";

class FakeRpcConnection {
  public readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  public lastRequest: { messageType: number; payload: Uint8Array } | undefined;
  public requestCalls: Array<{ messageType: number; payload: Uint8Array }> = [];
  public sendCalls: Array<{ messageType: number; payload: Uint8Array }> = [];
  public lastSignal: AbortSignal | undefined;
  public asyncDispatchAccepted = true;
  private state = ConnectionState.Authenticated;
  private readonly disconnectListeners = new Set<() => void>();
  private readonly reconnectListeners = new Set<() => void | Promise<void>>();

  async request(
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    this.lastRequest = { messageType, payload };
    this.requestCalls.push({ messageType, payload });
    this.lastSignal = signal;
    if (signal?.aborted) {
      const error = new Error("The operation was aborted");
      error.name = "AbortError";
      throw error;
    }

    if (messageType === 300 || messageType === 301) {
      return new Uint8Array([0]);
    }
    throw new Error(`Unexpected request message type ${messageType}`);
  }

  async send(messageType: number, payload: Uint8Array, signal?: AbortSignal): Promise<void> {
    this.lastRequest = { messageType, payload };
    this.sendCalls.push({ messageType, payload });
    this.lastSignal = signal;
    if (signal?.aborted) {
      const error = new Error("The operation was aborted");
      error.name = "AbortError";
      throw error;
    }
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

  onReconnect(listener: () => void | Promise<void>): () => void {
    this.reconnectListeners.add(listener);
    return () => {
      this.reconnectListeners.delete(listener);
    };
  }

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => {
      this.disconnectListeners.delete(listener);
    };
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): boolean {
    if (!this.asyncDispatchAccepted) {
      return false;
    }
    void Promise.resolve().then(task);
    return true;
  }

  tryDispatchAsyncHandler(task: () => void | Promise<void>): boolean {
    return this.dispatchAsyncHandler(task);
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

  async reconnect(): Promise<void> {
    for (const listener of this.reconnectListeners) {
      await listener();
    }
  }

  countRequests(messageType: number): number {
    return this.requestCalls.filter((call) => call.messageType === messageType).length;
  }
}

describe("RpcClient", () => {
  it("swallows worker response sends after the connection closes", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);
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

    handler(RpcCodec.encodeRequest(new Uint8Array(16), route, new Uint8Array([9])));

    await Promise.resolve();
    await Promise.resolve();

    expect(connection.sendCalls).toHaveLength(1);
    expect(connection.sendCalls[0].messageType).toBe(303);
  });

  it("does not send a stale worker response after disconnect and reconnect", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);
    const route = "rpc://realm/area/method";
    let releaseHandler: () => void = () => undefined;
    const handlerGate = new Promise<void>((resolve) => {
      releaseHandler = resolve;
    });

    await client.registerWorker(route, async (_req, writer) => {
      await handlerGate;
      await writer.send(new Uint8Array([1]), true);
    });

    const handler = connection.notificationHandlers.get(MSG_RPC_REQUEST);
    expect(handler).toBeTypeOf("function");

    if (!handler) {
      throw new Error("Expected RPC request handler to be registered");
    }

    handler(RpcCodec.encodeRequest(new Uint8Array(16), route, new Uint8Array([9])));

    await Promise.resolve();
    connection.emitDisconnect();
    connection.setState(ConnectionState.Authenticated);
    releaseHandler();

    await Promise.resolve();
    await Promise.resolve();

    expect(connection.sendCalls).toHaveLength(0);
  });

  it("re-subscribes workers on reconnect and handles requests with the restored handler", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);
    const route = "rpc://realm/area/method";
    const handledBodies: string[] = [];

    await client.registerWorker(
      route,
      async (req, writer) => {
        expect("replyRoute" in req).toBe(false);
        handledBodies.push(utf8Decoder.decode(req.body));
        await writer.send(new Uint8Array([5]), true);
      },
      { maxConcurrency: 7 },
    );
    expect(connection.countRequests(MSG_RPC_SUBSCRIBE_WORKER)).toBe(1);
    expect(readSubscribeMaxConcurrency(connection.requestCalls[0].payload)).toBe(7);

    await connection.reconnect();
    expect(connection.countRequests(MSG_RPC_SUBSCRIBE_WORKER)).toBe(2);
    expect(readSubscribeMaxConcurrency(connection.requestCalls[1].payload)).toBe(7);

    const handler = connection.notificationHandlers.get(MSG_RPC_REQUEST);
    expect(handler).toBeTypeOf("function");

    if (!handler) {
      throw new Error("Expected RPC request handler to be registered");
    }

    handler(RpcCodec.encodeRequest(new Uint8Array(16), route, utf8Encoder.encode("after")));

    await Promise.resolve();
    await Promise.resolve();

    expect(handledBodies).toEqual(["after"]);
    expect(connection.sendCalls).toHaveLength(1);
    expect(connection.sendCalls[0].messageType).toBe(MSG_RPC_RESPONSE);
    expect(RpcCodec.decodeResponseKey(connection.sendCalls[0].payload)).toMatchObject({
      body: new Uint8Array([5]),
      streamEnd: true,
    });
  });

  it("returns a terminal backpressure response when local worker dispatch is saturated", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);
    const route = "rpc://realm/area/method";
    const worker = vi.fn(async () => undefined);
    connection.asyncDispatchAccepted = false;

    await client.registerWorker(route, worker);

    const handler = connection.notificationHandlers.get(MSG_RPC_REQUEST);
    expect(handler).toBeTypeOf("function");

    if (!handler) {
      throw new Error("Expected RPC request handler to be registered");
    }

    const correlationId = new Uint8Array(16);
    correlationId[15] = 9;
    handler(RpcCodec.encodeRequest(correlationId, route, new Uint8Array([9])));

    await vi.waitFor(() => {
      expect(connection.sendCalls).toHaveLength(1);
    });

    expect(worker).not.toHaveBeenCalled();
    expect(connection.sendCalls[0].messageType).toBe(MSG_RPC_RESPONSE);

    const response = RpcCodec.decodeResponseKey(connection.sendCalls[0].payload);
    expect(response.sequence).toBe(0n);
    expect(response.streamEnd).toBe(true);
    expect(response.correlationKey).toBe(9n);
    expect(RpcCodec.decodeErrorBody(response.body)).toEqual({
      code: ErrCodeRpcBackpressure,
      message: "Local RPC worker is overloaded",
    });
  });

  it("does not re-subscribe workers after they unsubscribe", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);
    const subscription = await client.registerWorker(
      "rpc://realm/area/method",
      async () => undefined,
    );
    expect(connection.countRequests(MSG_RPC_SUBSCRIBE_WORKER)).toBe(1);

    await subscription.unsubscribe();
    expect(connection.countRequests(MSG_RPC_UNSUBSCRIBE_WORKER)).toBe(1);

    await connection.reconnect();
    expect(connection.countRequests(MSG_RPC_SUBSCRIBE_WORKER)).toBe(1);
  });

  it("encodes worker maxConcurrency and rejects invalid values", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);

    await client.registerWorker("rpc://realm/area/method", async () => undefined, {
      maxConcurrency: 32,
    });

    expect(readSubscribeMaxConcurrency(connection.requestCalls[0].payload)).toBe(32);
    await expect(
      client.registerWorker("rpc://realm/area/other", async () => undefined, {
        maxConcurrency: 0,
      }),
    ).rejects.toMatchObject({ code: "RPC_INVALID_OPTIONS" });
    await expect(
      client.registerWorker("rpc://realm/area/other", async () => undefined, {
        maxConcurrency: 1025,
      }),
    ).rejects.toMatchObject({ code: "RPC_INVALID_OPTIONS" });
    await expect(
      client.registerWorker("rpc://realm/area/other", async () => undefined, {
        maxConcurrency: 1.5,
      }),
    ).rejects.toMatchObject({ code: "RPC_INVALID_OPTIONS" });
  });

  it("does not register an RPC ACK notification handler", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);

    await client.registerWorker("rpc://realm/area/method", async () => undefined);

    expect(connection.notificationHandlers.has(304)).toBe(false);
  });

  it("forwards request cancellation to the connection layer", async () => {
    const controller = new AbortController();
    controller.abort();
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);

    await expect(
      client.call("rpc://realm/area/method", new Uint8Array([1]), {
        signal: controller.signal,
      }),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(connection.lastSignal).toBe(controller.signal);
    expect(connection.countRequests(MSG_RPC_REQUEST)).toBe(0);
  });

  it("delivers a terminal RPC response frame that also carries a body", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);

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
    const client = new RpcClient(connection);

    const iterator = await client.call("rpc://realm/area/method", new Uint8Array([1]));

    const nextPromise = iterator.next();
    connection.emitDisconnect();

    await expect(nextPromise).rejects.toMatchObject({
      name: "ConnectionError",
    });
  });

  it("fails immediately when an RPC stream reports worker not found", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);

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
      RpcCodec.encodeResponse(decoded.correlationId, 0n, encodeWorkerNotFoundBody(), true),
    );

    await expect(iterator.next()).rejects.toBeInstanceOf(RpcError);
    await expect(iterator.next()).rejects.toMatchObject({
      code: "RPC_WORKER_NOT_FOUND",
      domainCode: ErrCodeRpcWorkerNotFound,
    });
  });

  it("maps terminal RPC error frames by domain code", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection);

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
      RpcCodec.encodeResponse(
        decoded.correlationId,
        0n,
        encodeRpcErrorBody(ErrCodeRpcRouteNotRegistered, "No workers registered for route"),
        true,
      ),
    );

    await expect(iterator.next()).rejects.toMatchObject({
      code: "RPC_ROUTE_NOT_REGISTERED",
      domainCode: ErrCodeRpcRouteNotRegistered,
    });
  });

  it("clears the pending next timeout when a stream reports worker not found", async () => {
    vi.useFakeTimers();
    try {
      const connection = new FakeRpcConnection();
      const client = new RpcClient(connection);

      const iterator = await client.call("rpc://realm/area/method", new Uint8Array([1]), {
        timeoutMs: 10000,
      });
      const nextPromise = iterator.next();

      expect(vi.getTimerCount()).toBe(1);

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
        RpcCodec.encodeResponse(decoded.correlationId, 0n, encodeWorkerNotFoundBody(), true),
      );

      expect(vi.getTimerCount()).toBe(0);
      await expect(nextPromise).rejects.toBeInstanceOf(RpcError);

      await vi.advanceTimersByTimeAsync(10000);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("clears the pending next timeout when the connection disconnects", async () => {
    vi.useFakeTimers();
    try {
      const connection = new FakeRpcConnection();
      const client = new RpcClient(connection);

      const iterator = await client.call("rpc://realm/area/method", new Uint8Array([1]), {
        timeoutMs: 10000,
      });
      const nextPromise = iterator.next();

      expect(vi.getTimerCount()).toBe(1);

      connection.emitDisconnect();

      expect(vi.getTimerCount()).toBe(0);
      await expect(nextPromise).rejects.toMatchObject({
        name: "ConnectionError",
      });

      await vi.advanceTimersByTimeAsync(10000);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});

function encodeWorkerNotFoundBody(): Uint8Array {
  return encodeRpcErrorBody(ErrCodeRpcWorkerNotFound, "worker missing");
}

function encodeRpcErrorBody(code: number, message: string): Uint8Array {
  const writer = new BufferWriter();
  writer.writeU8(1);
  writer.writeU32BE(code);
  writer.writeString(message);
  return writer.getBuffer();
}

function readSubscribeMaxConcurrency(payload: Uint8Array): number {
  const reader = new BufferReader(payload);
  reader.readString();
  const maxConcurrency = reader.readU32BE();
  expect(reader.isEOF()).toBe(true);
  return maxConcurrency;
}
