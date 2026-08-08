import { describe, expect, it, vi } from "vite-plus/test";

import { createBufferWriter } from "../../../src/core/buffer";
import { ConnectionError } from "../../../src/core/errors";
import { createQueueClient } from "../../../src/domains/queue/client";
import { createQueueItem } from "../../../src/domains/queue/types";
import {
  MSG_QUEUE_EXTEND,
  MSG_QUEUE_NOTIFY,
  MSG_QUEUE_RESERVE,
  MSG_QUEUE_SUBSCRIBE,
  MSG_QUEUE_UNSUBSCRIBE,
} from "../../../src/frame/types";

type Handler = (payload: Uint8Array) => void;

class FakeQueueConnection {
  readonly requests: Array<{ messageType: number; payload: Uint8Array }> = [];
  readonly handlers = new Map<number, Handler>();
  readonly reconnectListeners = new Set<() => void | Promise<void>>();
  readonly disconnectListeners = new Set<() => void>();
  readonly gates = new Map<number, Promise<void>>();
  reserveResponses: Uint8Array[] = [];
  subscribeSubId = 7n;
  unsubscribeCount = 0;
  onReserve: (() => void) | null = null;
  connected = true;
  private reconnectGate: Promise<void> = Promise.resolve();
  private releaseReconnectGate: (() => void) | null = null;
  private failReconnectGate: ((error: Error) => void) | null = null;

  async request(
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    if (!this.connected) {
      // Mirrors the real connection layer: a request made while
      // disconnected waits out an in-progress reconnect (resolved by
      // reconnect()) rather than failing immediately, unless the reconnect
      // itself is given up on (failReconnect()).
      await this.reconnectGate;
    }
    const gate = this.gates.get(messageType);
    if (gate) {
      await new Promise<void>((resolve, reject) => {
        const onAbort = (): void => {
          const error = new Error("The operation was aborted");
          error.name = "AbortError";
          reject(error);
        };
        signal?.addEventListener("abort", onAbort, { once: true });
        void gate.then(() => {
          signal?.removeEventListener("abort", onAbort);
          resolve();
        });
      });
    }
    this.requests.push({ messageType, payload });
    if (messageType === MSG_QUEUE_SUBSCRIBE) {
      return encodeQueueSubscribeResponse(this.subscribeSubId);
    }
    if (messageType === MSG_QUEUE_UNSUBSCRIBE) {
      this.unsubscribeCount += 1;
      return new Uint8Array([0]);
    }
    if (messageType === MSG_QUEUE_RESERVE) {
      this.onReserve?.();
      return this.reserveResponses.shift() ?? encodeQueueReserveResponse([]);
    }
    throw new Error(`unexpected message type ${messageType}`);
  }

  registerNotificationHandler(messageType: number, handler: Handler): void {
    this.handlers.set(messageType, handler);
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }

  onReconnect(listener: () => void | Promise<void>): () => void {
    this.reconnectListeners.add(listener);
    return () => {
      this.reconnectListeners.delete(listener);
    };
  }

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => this.disconnectListeners.delete(listener);
  }

  gate(messageType: number): () => void {
    let release: () => void = () => undefined;
    const promise = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.gates.set(messageType, promise);
    return () => {
      this.gates.delete(messageType);
      release();
    };
  }

  notify(route: string = "queue://realm/area/resource", subId = 7n): void {
    this.handlers.get(MSG_QUEUE_NOTIFY)?.(encodeQueueNotification(subId, route));
  }

  async reconnect(): Promise<void> {
    this.connected = true;
    this.releaseReconnectGate?.();
    for (const listener of this.reconnectListeners) {
      await listener();
    }
  }

  disconnect(): void {
    this.connected = false;
    this.reconnectGate = new Promise<void>((resolve, reject) => {
      this.releaseReconnectGate = resolve;
      this.failReconnectGate = reject;
    });
    // A test that never calls reconnect()/failReconnect() shouldn't trip
    // Node's unhandled-rejection tracker for this gate.
    this.reconnectGate.catch(() => undefined);
    for (const listener of this.disconnectListeners) {
      listener();
    }
  }

  failReconnect(error: Error): void {
    this.failReconnectGate?.(error);
  }
}

describe("QueueClient reserveWhenAvailable", () => {
  it("returns each concrete queue route given a wildcard reserve selector", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(
      encodeQueueReserveResponse(
        [
          {
            route: "queue://acme/cats/cat",
            id: 1n,
            token: 2n,
            body: new Uint8Array([3]),
          },
        ],
        true,
      ),
    );
    const client = createQueueClient(connection);

    const items = await client.reserve("queue://*/cats/*", 30, 1);

    expect(items).toHaveLength(1);
    expect(items[0].route).toBe("queue://acme/cats/cat");
  });

  it("reserves before waiting and wakes after an empty reserve", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(
      encodeQueueReserveResponse([]),
      encodeQueueReserveResponse([
        { route: "queue://realm/area/resource", id: 1n, token: 2n, body: new Uint8Array([3]) },
      ]),
    );
    const client = createQueueClient(connection);
    const iterator = client
      .reserveWhenAvailable("queue://realm/area/resource", {
        leaseSeconds: 30,
        batchSize: 2,
      })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(
        connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
      ).toHaveLength(1);
    });

    connection.notify();

    const result = await pending;
    expect(result.done).toBe(false);
    expect(result.value).toHaveLength(1);
    expect(result.value?.[0].body).toEqual(new Uint8Array([3]));

    await iterator.return?.();
    expect(connection.unsubscribeCount).toBe(1);
  });

  it("does not lose a notification that arrives during an empty reserve", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(
      encodeQueueReserveResponse([]),
      encodeQueueReserveResponse([
        { route: "queue://realm/area/resource", id: 1n, token: 2n, body: new Uint8Array([4]) },
      ]),
    );
    connection.onReserve = () => {
      if (
        connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE).length === 1
      ) {
        connection.notify();
      }
    };
    const client = createQueueClient(connection);

    const result = await client
      .reserveWhenAvailable("queue://realm/area/resource", { leaseSeconds: 30 })
      [Symbol.asyncIterator]()
      .next();

    expect(result.done).toBe(false);
    expect(result.value).toHaveLength(1);
    expect(
      connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
    ).toHaveLength(2);
  });

  it("reserves again after reconnect even without a queue notification", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(
      encodeQueueReserveResponse([]),
      encodeQueueReserveResponse([
        { route: "queue://realm/area/resource", id: 1n, token: 2n, body: new Uint8Array([5]) },
      ]),
    );
    const client = createQueueClient(connection);
    const iterator = client
      .reserveWhenAvailable("queue://realm/area/resource", { leaseSeconds: 30, batchSize: 10 })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(
        connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
      ).toHaveLength(1);
    });

    await connection.reconnect();

    const result = await pending;
    expect(result.done).toBe(false);
    expect(result.value).toHaveLength(1);
    expect(result.value?.[0].body).toEqual(new Uint8Array([5]));
    expect(
      connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
    ).toHaveLength(2);

    await iterator.return?.();
    expect(connection.unsubscribeCount).toBe(1);
  });

  it("unsubscribes when the iterator is aborted", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(encodeQueueReserveResponse([]));
    const client = createQueueClient(connection);
    const controller = new AbortController();
    const iterator = client
      .reserveWhenAvailable("queue://realm/area/resource", {
        leaseSeconds: 30,
        signal: controller.signal,
      })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(
        connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
      ).toHaveLength(1);
    });
    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(connection.unsubscribeCount).toBe(1);
  });

  it("wakes on disconnect and unsubscribes once the connection gives up on reconnecting, instead of hanging forever while idle-parked", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(encodeQueueReserveResponse([]));
    const client = createQueueClient(connection);
    const iterator = client
      .reserveWhenAvailable("queue://realm/area/resource", { leaseSeconds: 30 })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(
        connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
      ).toHaveLength(1);
    });

    connection.disconnect();
    connection.failReconnect(new ConnectionError("reconnect exhausted"));

    // Pre-fix there is no onDisconnect wiring at all, so the generator has
    // no wake source and this would hang forever — race against a short
    // timer so a regression fails the test instead of hanging the suite.
    const hung = Symbol("hung");
    const outcome = await Promise.race([
      pending.then(
        () => "settled",
        () => "settled",
      ),
      new Promise((resolve) => setTimeout(() => resolve(hung), 500)),
    ]);

    expect(outcome).toBe("settled");
    // The wake let the generator retry, discover the connection is
    // genuinely gone (not just transiently reconnecting), and propagate
    // that failure — instead of parking forever with no way out. The wire
    // UNSUBSCRIBE the `finally` block attempts also fails the same way
    // (there's nothing left to unsubscribe from), which is expected here.
    await expect(pending).rejects.toBeInstanceOf(ConnectionError);
  });

  it("keeps iterating across a transient disconnect that reconnects, instead of terminating the iterator", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(
      encodeQueueReserveResponse([]),
      encodeQueueReserveResponse([
        { route: "queue://realm/area/resource", id: 1n, token: 2n, body: new Uint8Array([5]) },
      ]),
    );
    const client = createQueueClient(connection);
    const iterator = client
      .reserveWhenAvailable("queue://realm/area/resource", { leaseSeconds: 30 })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(
        connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
      ).toHaveLength(1);
    });

    // A disconnect immediately followed by a successful reconnect must not
    // terminate the iterator — a sticky "disconnected" flag would throw
    // here before the reconnect (and its restored subscription) ever gets
    // a chance to resume the iteration.
    connection.disconnect();
    await connection.reconnect();

    const result = await pending;
    expect(result.done).toBe(false);
    expect(result.value).toHaveLength(1);
    expect(result.value?.[0].body).toEqual(new Uint8Array([5]));

    await iterator.return?.();
    expect(connection.unsubscribeCount).toBe(1);
  });
});

describe("QueueItem.extend", () => {
  it("reports a generic EXTEND_FAILED code instead of mislabeling every failure as QueueNotFound", async () => {
    const writer = createBufferWriter();
    writer.writeU8(1); // status=1 (failure), no domain error code on the wire
    writer.writeString("lease token expired");
    const failureResponse = writer.getBuffer();

    const connection = {
      request: async (messageType: number) => {
        if (messageType === MSG_QUEUE_EXTEND) return failureResponse;
        throw new Error(`unexpected message type ${messageType}`);
      },
      onDisconnect: () => () => undefined,
    };

    const item = createQueueItem(
      1n,
      2n,
      new Uint8Array(),
      "queue://realm/area/resource",
      connection,
    );

    let caught: unknown;
    try {
      await item.extend(30);
    } catch (error) {
      caught = error;
    }

    expect(caught).toMatchObject({ code: "QUEUE_EXTEND_FAILED" });
    expect((caught as Error).message).not.toContain("QueueNotFound");
    expect((caught as Error).message).toContain("lease token expired");
  });
});

describe("QueueClient enqueue", () => {
  it("throws synchronously instead of silently dropping priority or ttlMs", async () => {
    const connection = new FakeQueueConnection();
    const client = createQueueClient(connection);

    await expect(
      client.enqueue("queue://realm/area/resource", new Uint8Array([1]), { priority: 5 }),
    ).rejects.toThrow(/not yet supported/i);
    await expect(
      client.enqueue("queue://realm/area/resource", new Uint8Array([1]), { ttlMs: 1000 }),
    ).rejects.toThrow(/not yet supported/i);

    // Neither call should have reached the wire.
    expect(connection.requests).toHaveLength(0);
  });
});

describe("QueueClient reserve", () => {
  it("accepts an AbortSignal, matching the sibling read methods on other domains", async () => {
    const connection = new FakeQueueConnection();
    const controller = new AbortController();
    const client = createQueueClient(connection);

    connection.gate(MSG_QUEUE_RESERVE);
    const pending = client.reserve("queue://realm/area/resource", 30, 1, 30, controller.signal);
    await Promise.resolve();

    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  });
});

describe("QueueClient subscribe/unsubscribe", () => {
  it("issues a fresh wire subscribe for a concurrent subscribe() that lands while an unsubscribe() is still in flight", async () => {
    const connection = new FakeQueueConnection();
    const client = createQueueClient(connection);

    let receivedA = false;
    const subA = await client.subscribe("queue://realm/area/**", async () => {
      receivedA = true;
    });

    const release = connection.gate(MSG_QUEUE_UNSUBSCRIBE);
    const unsubscribing = subA.unsubscribe();
    await Promise.resolve();

    let receivedB = false;
    // subscribe() must wait out the in-flight unsubscribe instead of
    // reusing the about-to-be-torn-down state — reusing it would register B
    // locally with no corresponding broker subscription.
    const subscribingB = client.subscribe("queue://realm/area/**", async () => {
      receivedB = true;
    });
    await Promise.resolve();
    expect(
      connection.requests.filter((call) => call.messageType === MSG_QUEUE_SUBSCRIBE),
    ).toHaveLength(1);

    release();
    await unsubscribing;
    const subB = await subscribingB;

    // B's subscribe() only resolved once the unsubscribe settled, and sent
    // its own wire SUBSCRIBE — it is genuinely registered at the broker,
    // not just piggybacking on A's torn-down local state.
    expect(
      connection.requests.filter((call) => call.messageType === MSG_QUEUE_SUBSCRIBE),
    ).toHaveLength(2);

    connection.notify("queue://realm/area/resource");
    await Promise.resolve();

    expect(receivedA).toBe(false);
    expect(receivedB).toBe(true);

    await subB.unsubscribe();
  });

  it("keeps a live subId across reconnect for a subscription that survives it, unlike a frozen value", async () => {
    const connection = new FakeQueueConnection();
    connection.subscribeSubId = 1n;
    const client = createQueueClient(connection);

    const subscription = await client.subscribe("queue://realm/area/**", async () => undefined);
    expect(subscription.subId).toBe(1n);

    connection.subscribeSubId = 2n;
    await connection.reconnect();

    expect(subscription.subId).toBe(2n);
  });

  it("exposes subscribeIterator() for symmetry with the other domains", async () => {
    const connection = new FakeQueueConnection();
    const client = createQueueClient(connection);

    const iterator = client.subscribeIterator("queue://realm/area/**")[Symbol.asyncIterator]();
    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(connection.requests.some((call) => call.messageType === MSG_QUEUE_SUBSCRIBE)).toBe(
        true,
      );
    });

    connection.notify("queue://realm/area/resource");
    const result = await pending;

    expect(result.done).toBe(false);
    expect(result.value).toMatchObject({ route: "queue://realm/area/resource" });

    await iterator.return?.();
  });
});

function encodeQueueSubscribeResponse(subId: bigint): Uint8Array {
  const payload = new Uint8Array(10);
  payload[0] = 0;
  payload[1] = 1;
  new DataView(payload.buffer).setBigUint64(2, subId, false);
  return payload;
}

function encodeQueueReserveResponse(
  items: Array<{ route: string; id: bigint; token: bigint; body: Uint8Array }>,
  includeRoute = false,
): Uint8Array {
  const writer = createBufferWriter(64);
  writer.writeU8(0);
  writer.writeU32BE(items.length);
  for (const item of items) {
    if (includeRoute) writer.writeRoute(item.route);
    writer.writeU64BE(item.id);
    writer.writeU64BE(item.token);
    writer.writeU32BE(item.body.length);
    writer.writeBytes(item.body);
  }
  return writer.getBuffer();
}

function encodeQueueNotification(subId: bigint, route: string): Uint8Array {
  const writer = createBufferWriter(64);
  writer.writeU64BE(subId);
  writer.writeRoute(route);
  writer.writeU64BE(3n);
  writer.writeU64BE(2n);
  writer.writeU64BE(1n);
  return writer.getBuffer();
}
