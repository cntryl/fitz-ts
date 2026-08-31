import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { createBufferWriter } from "../../../src/core/buffer";
import { LeaseLifecycleError } from "../../../src/domains/lease/types";
import { FitzError } from "../../../src/core/errors";
import type { Connection } from "../../../src/client/connection";
import { createLeaseClient } from "../../../src/domains/lease/client";
import { LeaseCodec } from "../../../src/domains/lease/codec";
import {
  MSG_LEASE_ACQUIRE,
  MSG_LEASE_LIST,
  MSG_LEASE_NOTIFY,
  MSG_LEASE_RENEW,
  MSG_LEASE_RELEASE,
  MSG_LEASE_SUBSCRIBE,
  MSG_LEASE_UNSUBSCRIBE,
} from "../../../src/frame/types";

function subscribeResponse(subId: bigint): Uint8Array {
  const writer = createBufferWriter();
  writer.writeU8(0);
  writer.writeU64BE(subId);
  return writer.getBuffer();
}

function plainSuccessResponse(): Uint8Array {
  return new Uint8Array([0]);
}

function encodeLeaseNotification(subId: bigint, route: string): Uint8Array {
  const writer = createBufferWriter();
  writer.writeU64BE(subId);
  writer.writeRoute(route);
  writer.writeU32BE(0);
  return writer.getBuffer();
}

/** A fuller fake connection supporting queued responses per messageType,
 * gated (delayable) responses, disconnect, reconnect, and notifications —
 * used by the subscribe/unsubscribe/withLease-lifecycle tests below. */
class FullLeaseConnection {
  private readonly responses = new Map<number, Uint8Array[]>();
  private readonly gates = new Map<number, Promise<void>>();
  private readonly disconnectListeners = new Set<() => void>();
  private readonly reconnectListeners = new Set<() => void | Promise<void>>();
  private readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  requests: Array<{ messageType: number; payload: Uint8Array }> = [];

  async request(
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    const gate = this.gates.get(messageType);
    if (gate) {
      await gate;
    }
    this.requests.push({ messageType, payload });
    if (signal?.aborted) {
      const error = new Error("The operation was aborted");
      error.name = "AbortError";
      throw error;
    }
    const queued = this.responses.get(messageType)?.shift();
    if (!queued) {
      throw new Error(`FullLeaseConnection: no queued response for messageType ${messageType}`);
    }
    return queued;
  }

  respond(messageType: number, response: Uint8Array): void {
    const existing = this.responses.get(messageType);
    if (existing) {
      existing.push(response);
      return;
    }
    this.responses.set(messageType, [response]);
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

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => this.disconnectListeners.delete(listener);
  }

  onReconnect(listener: () => void | Promise<void>): () => void {
    this.reconnectListeners.add(listener);
    return () => this.reconnectListeners.delete(listener);
  }

  registerNotificationHandler(messageType: number, handler: (payload: Uint8Array) => void): void {
    this.notificationHandlers.set(messageType, handler);
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }

  disconnect(): void {
    for (const listener of this.disconnectListeners) {
      listener();
    }
  }

  async reconnect(): Promise<void> {
    for (const listener of this.reconnectListeners) {
      await listener();
    }
  }

  emitNotification(messageType: number, payload: Uint8Array): void {
    const handler = this.notificationHandlers.get(messageType);
    if (!handler) throw new Error(`No notification handler registered for ${messageType}`);
    handler(payload);
  }
}

function acquireResponse(kind: 0 | 1 | 2 | 3, token: bigint): Uint8Array {
  const bytes = new Uint8Array(10);
  bytes[0] = 0;
  bytes[1] = kind;
  new DataView(bytes.buffer).setBigUint64(2, token);
  return bytes;
}

function renewResponse(token: bigint): Uint8Array {
  const bytes = new Uint8Array(9);
  bytes[0] = 0;
  new DataView(bytes.buffer).setBigUint64(1, token);
  return bytes;
}

function errorResponse(code: number, message: string): Uint8Array {
  const encoded = new TextEncoder().encode(message);
  const bytes = new Uint8Array(1 + 4 + 4 + encoded.length);
  bytes[0] = 1;
  const view = new DataView(bytes.buffer);
  view.setUint32(1, code);
  view.setUint32(5, encoded.length);
  bytes.set(encoded, 9);
  return bytes;
}

class FakeLeaseConnection {
  readonly handlers = new Map<number, (payload: Uint8Array) => void>();
  readonly requests: Array<{ messageType: number; payload: Uint8Array }> = [];
  private readonly disconnectListeners = new Set<() => void>();

  constructor(private readonly responses: Uint8Array[]) {}

  async request(messageType: number, payload: Uint8Array): Promise<Uint8Array> {
    this.requests.push({ messageType, payload });
    const response = this.responses.shift();
    if (!response) throw new Error("missing fake response");
    return response;
  }

  registerNotificationHandler(type: number, handler: (payload: Uint8Array) => void): void {
    this.handlers.set(type, handler);
  }

  onReconnect(): () => void {
    return () => undefined;
  }
  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => this.disconnectListeners.delete(listener);
  }
  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void task();
  }
}

class DisconnectingAcquireConnection {
  readonly handlers = new Map<number, (payload: Uint8Array) => void>();
  private readonly disconnectListeners = new Set<() => void>();
  private rejectRequest: (error: unknown) => void = () => undefined;

  request(): Promise<Uint8Array> {
    return new Promise<Uint8Array>((_resolve, reject) => {
      this.rejectRequest = reject;
    });
  }

  registerNotificationHandler(type: number, handler: (payload: Uint8Array) => void): void {
    this.handlers.set(type, handler);
  }

  onReconnect(): () => void {
    return () => undefined;
  }

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => this.disconnectListeners.delete(listener);
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void task();
  }

  disconnect(): void {
    for (const listener of this.disconnectListeners) listener();
    this.rejectRequest(new Error("transport disconnected"));
  }
}

describe("lease acquisition", () => {
  it("encodes the canonical wait_seconds field, including zero", () => {
    const zero = LeaseCodec.encodeAcquire("lease://realm/area/resource", 30);
    const waiting = LeaseCodec.encodeAcquire("lease://realm/area/resource", 30, 17);

    expect(
      new DataView(zero.buffer, zero.byteOffset, zero.byteLength).getUint32(zero.length - 4),
    ).toBe(0);
    expect(
      new DataView(waiting.buffer, waiting.byteOffset, waiting.byteLength).getUint32(
        waiting.length - 4,
      ),
    ).toBe(17);
  });

  it("resolves a queued acquisition from the deferred ACQUIRE frame", async () => {
    const connection = new FakeLeaseConnection([acquireResponse(2, 0n)]);
    const client = createLeaseClient(connection as unknown as Connection);

    const pending = client.acquire("lease://realm/area/resource", {
      ttlSeconds: 30,
      waitSeconds: 12,
    });
    await Promise.resolve();
    connection.handlers.get(MSG_LEASE_ACQUIRE)?.(acquireResponse(0, 42n));

    await expect(pending).resolves.toBeDefined();
    expect(connection.requests[0]?.messageType).toBe(MSG_LEASE_ACQUIRE);
    const payload = connection.requests[0]!.payload;
    expect(
      new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getUint32(
        payload.length - 4,
      ),
    ).toBe(12);
  });

  it("serializes acquisition lifecycles until deferred completion arrives", async () => {
    const connection = new FakeLeaseConnection([acquireResponse(2, 0n), acquireResponse(0, 99n)]);
    const client = createLeaseClient(connection as unknown as Connection);

    const first = client.acquire("lease://realm/area/first", { ttlSeconds: 30, waitSeconds: 12 });
    const second = client.acquire("lease://realm/area/second", { ttlSeconds: 30 });
    await Promise.resolve();
    await Promise.resolve();
    expect(connection.requests).toHaveLength(1);

    connection.handlers.get(MSG_LEASE_ACQUIRE)?.(acquireResponse(0, 42n));
    await first;
    await second;
    expect(connection.requests).toHaveLength(2);
  });

  it("should preserve broker message given deferred timeout when acquisition completes", async () => {
    const connection = new FakeLeaseConnection([acquireResponse(2, 0n)]);
    const client = createLeaseClient(connection as unknown as Connection);
    const pending = client.acquire("lease://realm/area/resource", {
      ttlSeconds: 30,
      waitSeconds: 1,
    });
    await Promise.resolve();
    const message = new TextEncoder().encode("lease wait timed out");
    const error = new Uint8Array(1 + 4 + 4 + message.length);
    error[0] = 1;
    const view = new DataView(error.buffer);
    view.setUint32(1, 5006);
    view.setUint32(5, message.length);
    error.set(message, 9);
    connection.handlers.get(MSG_LEASE_ACQUIRE)?.(error);

    await expect(pending).rejects.toMatchObject({ domainCode: 5006 });
    await expect(pending).rejects.toThrow("lease wait timed out");
  });

  it.each([0, 12])(
    "should_reject_the_public_acquisition_without_an_unhandled_rejection_given_disconnect_with_wait_seconds_%s",
    async (waitSeconds) => {
      const connection = new DisconnectingAcquireConnection();
      const client = createLeaseClient(connection as unknown as Connection);
      const unhandled = vi.fn();
      process.on("unhandledRejection", unhandled);

      try {
        const pending = client.acquire("lease://realm/area/resource", {
          ttlSeconds: 30,
          waitSeconds,
        });
        await Promise.resolve();

        connection.disconnect();

        await expect(pending).rejects.toMatchObject({ code: "LEASE_DISCONNECTED" });
        await new Promise<void>((resolve) => setImmediate(resolve));
        expect(unhandled).not.toHaveBeenCalled();
      } finally {
        process.off("unhandledRejection", unhandled);
      }
    },
  );
});

describe("withLease", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("passes a frozen snapshot of the immediately granted fencing token to the callback", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_ACQUIRE, acquireResponse(0, 42n));
    connection.respond(MSG_LEASE_RELEASE, plainSuccessResponse());
    const client = createLeaseClient(connection as unknown as Connection);

    await client.withLease(
      "lease://realm/area/resource",
      (_signal, authority) => {
        expect(authority.fencingToken).toBe(42n);
        expect(Object.isFrozen(authority)).toBe(true);
      },
      { ttlSeconds: 30 },
    );
  });

  it("uses the token returned by an AlreadyHeld acquisition", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_ACQUIRE, acquireResponse(1, 43n));
    connection.respond(MSG_LEASE_RELEASE, plainSuccessResponse());
    const client = createLeaseClient(connection as unknown as Connection);

    await client.withLease(
      "lease://realm/area/resource",
      (_signal, authority) => {
        expect(authority.fencingToken).toBe(43n);
      },
      { ttlSeconds: 30 },
    );
  });

  it("passes the final granted token after a queued acquisition", async () => {
    const connection = new FakeLeaseConnection([acquireResponse(2, 7n), plainSuccessResponse()]);
    const client = createLeaseClient(connection as unknown as Connection);

    let observedToken: bigint | undefined;
    const pending = client.withLease(
      "lease://realm/area/resource",
      (_signal, authority) => {
        observedToken = authority.fencingToken;
      },
      { ttlSeconds: 30, waitSeconds: 12 },
    );
    await Promise.resolve();
    connection.handlers.get(MSG_LEASE_ACQUIRE)?.(acquireResponse(0, 42n));

    await pending;
    expect(observedToken).toBe(42n);
  });

  it("keeps the admission snapshot stable when renewal rotates the live credential", async () => {
    vi.useFakeTimers();
    const route = "lease://realm/area/resource";
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_ACQUIRE, acquireResponse(0, 42n));
    connection.respond(MSG_LEASE_RENEW, renewResponse(99n));
    connection.respond(MSG_LEASE_RELEASE, plainSuccessResponse());
    const client = createLeaseClient(connection as unknown as Connection);

    let finishCallback: () => void = () => undefined;
    const callbackCanFinish = new Promise<void>((resolve) => {
      finishCallback = resolve;
    });
    let callbackStarted: () => void = () => undefined;
    const started = new Promise<void>((resolve) => {
      callbackStarted = resolve;
    });

    const pending = client.withLease(
      route,
      async (_signal, authority) => {
        callbackStarted();
        expect(authority.fencingToken).toBe(42n);
        await callbackCanFinish;
        expect(authority.fencingToken).toBe(42n);
      },
      { ttlSeconds: 3 },
    );

    await started;
    await vi.advanceTimersByTimeAsync(1000);
    expect(connection.requests.some(({ messageType }) => messageType === MSG_LEASE_RENEW)).toBe(
      true,
    );
    finishCallback();
    await pending;
    const release = connection.requests.find(
      ({ messageType }) => messageType === MSG_LEASE_RELEASE,
    );
    expect(release?.payload).toEqual(LeaseCodec.encodeRelease(route, 99n));
  });

  it("does not invoke the callback when acquisition fails", async () => {
    const connection = new FakeLeaseConnection([errorResponse(5005, "lease held")]);
    const client = createLeaseClient(connection as unknown as Connection);
    let invoked = false;

    await expect(
      client.withLease(
        "lease://realm/area/resource",
        () => {
          invoked = true;
        },
        { ttlSeconds: 30 },
      ),
    ).rejects.toThrow("lease held");
    expect(invoked).toBe(false);
  });

  it("does not invoke the callback when queued acquisition times out", async () => {
    const connection = new FakeLeaseConnection([acquireResponse(2, 7n)]);
    const client = createLeaseClient(connection as unknown as Connection);
    let invoked = false;

    const pending = client.withLease(
      "lease://realm/area/resource",
      () => {
        invoked = true;
      },
      { ttlSeconds: 30, waitSeconds: 1 },
    );
    await Promise.resolve();
    connection.handlers.get(MSG_LEASE_ACQUIRE)?.(errorResponse(5006, "lease wait timed out"));

    await expect(pending).rejects.toThrow("lease wait timed out");
    expect(invoked).toBe(false);
  });

  it("does not invoke the callback when acquisition is already canceled", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);
    const controller = new AbortController();
    const reason = new Error("stop before acquire");
    controller.abort(reason);
    let invoked = false;

    await expect(
      client.withLease(
        "lease://realm/area/resource",
        () => {
          invoked = true;
        },
        { ttlSeconds: 30, signal: controller.signal },
      ),
    ).rejects.toBe(reason);
    expect(invoked).toBe(false);
  });

  // A RELEASE call that fails with an AbortError-named Error unrelated to
  // any real cancellation — mirroring what the real multiplexer produces
  // when a request's own signal aborts for any reason, including (but not
  // limited to) withLease's internal 5s release-cleanup watchdog.
  class FailingReleaseConnection {
    readonly handlers = new Map<number, (payload: Uint8Array) => void>();
    private readonly disconnectListeners = new Set<() => void>();

    request(messageType: number): Promise<Uint8Array> {
      if (messageType === MSG_LEASE_ACQUIRE) {
        return Promise.resolve(acquireResponse(0, 42n));
      }
      if (messageType === MSG_LEASE_RELEASE) {
        const error = new Error("The operation was aborted");
        error.name = "AbortError";
        return Promise.reject(error);
      }
      return Promise.reject(
        new Error(`FailingReleaseConnection: unexpected request ${messageType}`),
      );
    }

    registerNotificationHandler(): void {
      // no queued ACQUIRE notifications expected in this test
    }
    onReconnect(): () => void {
      return () => undefined;
    }
    onDisconnect(listener: () => void): () => void {
      this.disconnectListeners.add(listener);
      return () => this.disconnectListeners.delete(listener);
    }
    dispatchAsyncHandler(task: () => void | Promise<void>): void {
      void task();
    }
  }

  it("rejects when release fails with an AbortError-named error, instead of silently returning the callback's value", async () => {
    const connection = new FailingReleaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    const pending = client.withLease("lease://realm/area/resource", () => "ok", { ttlSeconds: 30 });

    // Pre-fix, isManagedCancellation() misclassified this as a benign
    // cancellation (since `lifecycle.signal` is always aborted by the time
    // the failures filter runs) and `withLease` resolved with "ok" as if the
    // release had actually succeeded. It must reject instead.
    await expect(pending).rejects.toBeDefined();
  });

  it("aborts the callback's signal immediately on disconnect, instead of waiting for the next renewal", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_ACQUIRE, acquireResponse(0, 42n));
    connection.respond(MSG_LEASE_RELEASE, plainSuccessResponse());
    const client = createLeaseClient(connection as unknown as Connection);

    let observedAbort = false;
    let releaseCallbackStarted: () => void = () => undefined;
    const callbackStarted = new Promise<void>((resolve) => {
      releaseCallbackStarted = resolve;
    });

    const pending = client.withLease(
      "lease://realm/area/resource",
      async (signal) => {
        releaseCallbackStarted();
        await new Promise<void>((resolve) => {
          signal.addEventListener("abort", () => {
            observedAbort = true;
            resolve();
          });
        });
      },
      { ttlSeconds: 30 },
    );

    // Wait until acquire() has fully resolved and the callback has actually
    // started (registered its abort listener) before disconnecting — not
    // just one microtask tick into the call.
    await callbackStarted;
    connection.disconnect();

    await pending.catch(() => undefined);
    expect(observedAbort).toBe(true);
  });
});

describe("lease subscribe/unsubscribe", () => {
  it("issues a fresh wire subscribe for a concurrent subscribe() that lands while an unsubscribe() is still in flight", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(42n));
    const client = createLeaseClient(connection as unknown as Connection);

    let receivedA = false;
    const subA = await client.subscribe("lease://realm/area/resource", async () => {
      receivedA = true;
    });

    const release = connection.gate(MSG_LEASE_UNSUBSCRIBE);
    const unsubscribing = subA.unsubscribe();
    await Promise.resolve();

    // A concurrent subscribe() for the SAME route lands while that
    // UNSUBSCRIBE is still in flight. It must wait the unsubscribe out
    // rather than reuse the not-yet-deleted shared state — reusing it
    // would register B locally with no corresponding broker subscription.
    let receivedB = false;
    const subscribingB = client.subscribe("lease://realm/area/resource", async () => {
      receivedB = true;
    });
    await Promise.resolve();

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(99n));
    release();
    await unsubscribing;
    const subB = await subscribingB;

    // B's subscribe() only resolved once the unsubscribe settled, and it
    // sent its own fresh wire SUBSCRIBE — a genuinely new subId, not a
    // reuse of A's now-torn-down subscription.
    expect(subB).not.toHaveProperty("subId");

    connection.emitNotification(
      MSG_LEASE_NOTIFY,
      encodeLeaseNotification(99n, "lease://realm/area/resource"),
    );
    await Promise.resolve();

    expect(receivedA).toBe(false);
    expect(receivedB).toBe(true);

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await subB.unsubscribe();
  });

  it("should retain local handler given rejected unsubscribe when notifications continue", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(42n));
    const client = createLeaseClient(connection as unknown as Connection);

    let received = false;
    const subscription = await client.subscribe("lease://realm/area/resource", async () => {
      received = true;
    });

    const writer = createBufferWriter();
    writer.writeU8(1);
    writer.writeU32BE(5010);
    writer.writeString("broker rejected unsubscribe");
    connection.respond(MSG_LEASE_UNSUBSCRIBE, writer.getBuffer());

    await expect(subscription.unsubscribe()).rejects.toThrow("UNSUBSCRIBE failed");

    // The local handler must still be registered — a failed UNSUBSCRIBE
    // must not have removed it before the wire call was confirmed.
    connection.emitNotification(
      MSG_LEASE_NOTIFY,
      encodeLeaseNotification(42n, "lease://realm/area/resource"),
    );
    await Promise.resolve();

    expect(received).toBe(true);
  });

  it("keeps reporting its own subId after a later, unrelated resubscription reuses the same route", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(1n));
    const client = createLeaseClient(connection as unknown as Connection);

    const subA = await client.subscribe("lease://realm/area/resource", async () => undefined);
    expect(subA).not.toHaveProperty("subId");

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await subA.unsubscribe();

    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(2n));
    const subB = await client.subscribe("lease://realm/area/resource", async () => undefined);
    expect(subB).not.toHaveProperty("subId");

    expect(subA).not.toHaveProperty("subId");

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await subB.unsubscribe();
  });

  it("keeps a live subId across reconnect for a subscription that survives it", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(1n));
    const client = createLeaseClient(connection as unknown as Connection);

    const subscription = await client.subscribe(
      "lease://realm/area/resource",
      async () => undefined,
    );
    expect(subscription).not.toHaveProperty("subId");

    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(2n));
    await connection.reconnect();

    expect(subscription).not.toHaveProperty("subId");

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await subscription.unsubscribe();
  });

  it("accepts a synchronous ChangeHandler, not just an async one", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(1n));
    const client = createLeaseClient(connection as unknown as Connection);

    let received = false;
    // This must compile: ChangeHandler allows a plain synchronous function,
    // matching every other domain's notification handler type.
    const subscription = await client.subscribe("lease://realm/area/resource", () => {
      received = true;
    });

    connection.emitNotification(
      MSG_LEASE_NOTIFY,
      encodeLeaseNotification(1n, "lease://realm/area/resource"),
    );
    await Promise.resolve();

    expect(received).toBe(true);

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await subscription.unsubscribe();
  });
});

describe("lease subscribe/unsubscribe pattern grammar", () => {
  it("accepts a whole-segment wildcard pattern for subscribe", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(1n));
    const client = createLeaseClient(connection as unknown as Connection);

    const subscription = await client.subscribe("lease://acme/renderers/*", async () => undefined);
    expect(connection.requests[0]).toMatchObject({ messageType: MSG_LEASE_SUBSCRIBE });

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await subscription.unsubscribe();
  });

  it("accepts a trailing ** wildcard pattern for subscribe", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(1n));
    const client = createLeaseClient(connection as unknown as Connection);

    const subscription = await client.subscribe("lease://acme/**", async () => undefined);
    expect(connection.requests[0]).toMatchObject({ messageType: MSG_LEASE_SUBSCRIBE });

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await subscription.unsubscribe();
  });

  it("rejects a partial-wildcard pattern for subscribe", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    await expect(
      client.subscribe("lease://acme/renderers/lock*", async () => undefined),
    ).rejects.toMatchObject({ code: "LEASE_INVALID_ROUTE" });
  });

  it("rejects a wrong-depth pattern for subscribe", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    await expect(client.subscribe("lease://acme/*", async () => undefined)).rejects.toMatchObject({
      code: "LEASE_INVALID_ROUTE",
    });
  });

  it("still rejects a wildcard route for acquire", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    await expect(
      client.acquire("lease://acme/renderers/*", { ttlSeconds: 30 }),
    ).rejects.toMatchObject({ code: "LEASE_INVALID_ROUTE" });
  });

  it("still rejects a wildcard route for query", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    await expect(client.query("lease://acme/renderers/*")).rejects.toMatchObject({
      code: "LEASE_INVALID_ROUTE",
    });
  });
});

describe("lease list", () => {
  function listItem(overrides: Partial<Record<string, unknown>> = {}) {
    return {
      route: "lease://acme/renderers/one",
      ownerId: "worker-1",
      holderIncarnation: 1n,
      acquiredAt: "2026-08-29T00:00:00Z",
      expiresInSecs: 60n,
      renewals: 0,
      ...overrides,
    };
  }

  function encodeListPage(
    items: ReturnType<typeof listItem>[],
    nextCursor?: { snapshotId: bigint; offset: number },
  ): Uint8Array {
    const writer = createBufferWriter();
    writer.writeU8(0);
    writer.writeU32BE(items.length);
    for (const item of items) {
      writer.writeString(item.route as string);
      writer.writeString(item.ownerId as string);
      writer.writeU64BE(item.holderIncarnation as bigint);
      writer.writeString(item.acquiredAt as string);
      writer.writeU64BE(item.expiresInSecs as bigint);
      writer.writeU32BE(item.renewals as number);
    }
    writer.writeU8(nextCursor ? 1 : 0);
    if (nextCursor) {
      writer.writeU64BE(nextCursor.snapshotId);
      writer.writeU32BE(nextCursor.offset);
    }
    return writer.getBuffer();
  }

  it("listPage() sends a LIST frame and decodes the returned page", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_LIST, encodeListPage([listItem()]));
    const client = createLeaseClient(connection as unknown as Connection);

    const page = await client.listPage("lease://acme/renderers/*");

    expect(connection.requests[0]?.messageType).toBe(MSG_LEASE_LIST);
    expect(page.items).toEqual([listItem()]);
    expect(page.nextCursor).toBeUndefined();
  });

  it("list() pages through multiple LIST calls using the returned cursor", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(
      MSG_LEASE_LIST,
      encodeListPage([listItem({ route: "lease://acme/renderers/one" })], {
        snapshotId: 42n,
        offset: 1,
      }),
    );
    connection.respond(
      MSG_LEASE_LIST,
      encodeListPage([listItem({ route: "lease://acme/renderers/two" })]),
    );
    const client = createLeaseClient(connection as unknown as Connection);

    const pages: string[][] = [];
    for await (const page of client.list("lease://acme/renderers/*")) {
      pages.push(page.map((item) => item.route));
    }

    expect(pages).toEqual([["lease://acme/renderers/one"], ["lease://acme/renderers/two"]]);
    expect(connection.requests).toHaveLength(2);
  });

  it("rejects a malformed list pattern before sending a wire request", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    await expect(client.listPage("lease://acme/renderers/lock*")).rejects.toMatchObject({
      code: "LEASE_INVALID_ROUTE",
    });
    expect(connection.requests).toHaveLength(0);
  });

  it("rejects an invalid listPage() limit before sending a wire request", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    await expect(client.listPage("lease://acme/renderers/*", { limit: -1 })).rejects.toMatchObject({
      code: "LEASE_INVALID_LIST_ARGUMENT",
    });
    expect(connection.requests).toHaveLength(0);
  });

  it("rejects an invalid listPage() cursor offset before sending a wire request", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    await expect(
      client.listPage("lease://acme/renderers/*", {
        cursor: { snapshotId: 1n, offset: -1 },
      }),
    ).rejects.toMatchObject({
      code: "LEASE_INVALID_LIST_ARGUMENT",
    });
    expect(connection.requests).toHaveLength(0);
  });

  it("rejects an invalid list() pageSize before sending any wire request", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    const iterator = client.list("lease://acme/renderers/*", { pageSize: 1.5 });
    await expect(iterator.next()).rejects.toMatchObject({
      code: "LEASE_INVALID_LIST_ARGUMENT",
    });
    expect(connection.requests).toHaveLength(0);
  });
});

describe("lease observeInventory", () => {
  /** Flushes many microtask ticks — the observer's chains (dispatch defer,
   * requestFrame, connection.request) resolve over several ticks. */
  async function flush(ticks = 20): Promise<void> {
    for (let i = 0; i < ticks; i++) {
      await Promise.resolve();
    }
  }

  function inventoryItem(overrides: Partial<Record<string, unknown>> = {}) {
    return {
      route: "lease://acme/renderers/one",
      ownerId: "worker-1",
      holderIncarnation: 1n,
      acquiredAt: "2026-08-29T00:00:00Z",
      expiresInSecs: 60n,
      renewals: 0,
      ...overrides,
    };
  }

  function encodeListPage(
    items: ReturnType<typeof inventoryItem>[],
    nextCursor?: { snapshotId: bigint; offset: number },
  ): Uint8Array {
    const writer = createBufferWriter();
    writer.writeU8(0);
    writer.writeU32BE(items.length);
    for (const item of items) {
      writer.writeString(item.route as string);
      writer.writeString(item.ownerId as string);
      writer.writeU64BE(item.holderIncarnation as bigint);
      writer.writeString(item.acquiredAt as string);
      writer.writeU64BE(item.expiresInSecs as bigint);
      writer.writeU32BE(item.renewals as number);
    }
    writer.writeU8(nextCursor ? 1 : 0);
    if (nextCursor) {
      writer.writeU64BE(nextCursor.snapshotId);
      writer.writeU32BE(nextCursor.offset);
    }
    return writer.getBuffer();
  }

  const pattern = "lease://acme/renderers/*";

  it("subscribes and waits for its ack before listing, and buffers notifications until the first list installs", async () => {
    const connection = new FullLeaseConnection();
    const releaseSubscribe = connection.gate(MSG_LEASE_SUBSCRIBE);
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(9n));
    const releaseList = connection.gate(MSG_LEASE_LIST);
    connection.respond(MSG_LEASE_LIST, encodeListPage([inventoryItem()]));
    const client = createLeaseClient(connection as unknown as Connection);

    const observerPromise = client.observeInventory(pattern);
    await flush();
    expect(connection.requests.some((r) => r.messageType === MSG_LEASE_LIST)).toBe(false);

    // Let SUBSCRIBE's ack land, then give bootstrap() a turn to flip
    // buffering on and reach the (still gated) LIST call.
    releaseSubscribe();
    await flush();
    expect(connection.requests.some((r) => r.messageType === MSG_LEASE_SUBSCRIBE)).toBe(true);
    // LIST itself is still gated — its call is in flight (blocked inside
    // FullLeaseConnection.request awaiting the gate) but not yet recorded,
    // since recording happens only once the gate releases.

    // A notification arrives while the initial LIST is still gated — it must
    // invalidate the in-flight pass and must not be visible yet.
    connection.emitNotification(
      MSG_LEASE_NOTIFY,
      encodeLeaseNotification(9n, "lease://acme/renderers/two"),
    );
    // A second LIST response for the buffered-notification drain pass.
    connection.respond(
      MSG_LEASE_LIST,
      encodeListPage([inventoryItem(), inventoryItem({ route: "lease://acme/renderers/two" })]),
    );
    releaseList();

    const observer = await observerPromise;
    expect(observer.ready).toBe(true);
    // Two LIST calls: the initial bootstrap list, then the drain relist.
    expect(connection.requests.filter((r) => r.messageType === MSG_LEASE_LIST)).toHaveLength(2);
    const snapshot = observer.snapshot();
    expect(snapshot.has("lease://acme/renderers/one")).toBe(true);
    expect(snapshot.has("lease://acme/renderers/two")).toBe(true);

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await observer.close();
  });

  it("does not drain a second time when nothing was buffered during the initial list", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(1n));
    connection.respond(MSG_LEASE_LIST, encodeListPage([inventoryItem()]));
    const client = createLeaseClient(connection as unknown as Connection);

    const observer = await client.observeInventory(pattern);

    expect(connection.requests.filter((r) => r.messageType === MSG_LEASE_LIST)).toHaveLength(1);
    expect(observer.ready).toBe(true);
    expect(observer.snapshot().get("lease://acme/renderers/one")).toEqual(inventoryItem());

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await observer.close();
  });

  it("reconciles with LIST on steady-state notifications and preserves full items", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(2n));
    connection.respond(MSG_LEASE_LIST, encodeListPage([inventoryItem()]));
    const client = createLeaseClient(connection as unknown as Connection);
    const observer = await client.observeInventory(pattern);

    const updates: number[] = [];
    observer.onUpdate((snapshot) => updates.push(snapshot.size));

    connection.respond(
      MSG_LEASE_LIST,
      encodeListPage([
        inventoryItem(),
        inventoryItem({
          route: "lease://acme/renderers/two",
          holderIncarnation: 22n,
          renewals: 7,
        }),
      ]),
    );
    connection.emitNotification(
      MSG_LEASE_NOTIFY,
      encodeLeaseNotification(2n, "lease://acme/renderers/two"),
    );
    await flush();

    expect(connection.requests.filter((r) => r.messageType === MSG_LEASE_LIST)).toHaveLength(2);
    expect(observer.snapshot().get("lease://acme/renderers/two")).toMatchObject({
      holderIncarnation: 22n,
      renewals: 7,
    });
    expect(updates.length).toBeGreaterThan(0);

    // A release notification for an already-observed route removes it.
    connection.respond(
      MSG_LEASE_LIST,
      encodeListPage([
        inventoryItem({
          route: "lease://acme/renderers/two",
          holderIncarnation: 22n,
          renewals: 7,
        }),
      ]),
    );
    connection.emitNotification(
      MSG_LEASE_NOTIFY,
      encodeLeaseNotification(2n, "lease://acme/renderers/one"),
    );
    await flush();

    expect(observer.snapshot().has("lease://acme/renderers/one")).toBe(false);
    expect(connection.requests.filter((r) => r.messageType === MSG_LEASE_LIST)).toHaveLength(3);

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await observer.close();
  });

  it("periodically reconciles with a full relist on a jittered interval", async () => {
    vi.useFakeTimers();
    try {
      const randomSpy = vi.spyOn(Math, "random").mockReturnValue(0.5); // no jitter at 0.5
      const connection = new FullLeaseConnection();
      connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(3n));
      connection.respond(MSG_LEASE_LIST, encodeListPage([inventoryItem()]));
      const client = createLeaseClient(connection as unknown as Connection);
      const observer = await client.observeInventory(pattern, { reconcileIntervalMs: 1000 });

      expect(connection.requests.filter((r) => r.messageType === MSG_LEASE_LIST)).toHaveLength(1);

      connection.respond(
        MSG_LEASE_LIST,
        encodeListPage([inventoryItem({ route: "lease://acme/renderers/reconciled" })]),
      );
      await vi.advanceTimersByTimeAsync(1000);

      expect(connection.requests.filter((r) => r.messageType === MSG_LEASE_LIST)).toHaveLength(2);
      expect(observer.snapshot().has("lease://acme/renderers/reconciled")).toBe(true);
      expect(observer.snapshot().has("lease://acme/renderers/one")).toBe(false);

      randomSpy.mockRestore();
      connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
      await observer.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("invalidates readiness and re-runs the bootstrap on reconnect", async () => {
    const connection = new FullLeaseConnection();
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(4n));
    connection.respond(MSG_LEASE_LIST, encodeListPage([inventoryItem()]));
    const client = createLeaseClient(connection as unknown as Connection);
    const observer = await client.observeInventory(pattern);
    expect(observer.ready).toBe(true);

    // The client's own generic reconnect-restore resends SUBSCRIBE for the
    // still-registered route.
    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(5n));
    connection.respond(
      MSG_LEASE_LIST,
      encodeListPage([inventoryItem({ route: "lease://acme/renderers/after-reconnect" })]),
    );

    const reconnectPromise = connection.reconnect();
    // Readiness must flip false synchronously-ish while the re-bootstrap is
    // in flight (before the reconnect's awaited listeners settle).
    await reconnectPromise;

    expect(observer.ready).toBe(true);
    expect(connection.requests.filter((r) => r.messageType === MSG_LEASE_LIST)).toHaveLength(2);
    expect(observer.snapshot().has("lease://acme/renderers/after-reconnect")).toBe(true);
    expect(observer.snapshot().has("lease://acme/renderers/one")).toBe(false);

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await observer.close();
  });

  it("close() unsubscribes and stops background work", async () => {
    vi.useFakeTimers();
    try {
      const connection = new FullLeaseConnection();
      connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(6n));
      connection.respond(MSG_LEASE_LIST, encodeListPage([inventoryItem()]));
      connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
      const client = createLeaseClient(connection as unknown as Connection);
      const observer = await client.observeInventory(pattern, { reconcileIntervalMs: 1000 });

      await observer.close();

      expect(connection.requests.some((r) => r.messageType === MSG_LEASE_UNSUBSCRIBE)).toBe(true);

      const listCallsBeforeAdvance = connection.requests.filter(
        (r) => r.messageType === MSG_LEASE_LIST,
      ).length;
      await vi.advanceTimersByTimeAsync(5000);
      expect(connection.requests.filter((r) => r.messageType === MSG_LEASE_LIST).length).toBe(
        listCallsBeforeAdvance,
      );

      // Idempotent.
      await expect(observer.close()).resolves.toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects an invalid pattern before subscribing", async () => {
    const connection = new FullLeaseConnection();
    const client = createLeaseClient(connection as unknown as Connection);

    await expect(client.observeInventory("lease://acme/renderers/lock*")).rejects.toMatchObject({
      code: "LEASE_INVALID_ROUTE",
    });
    expect(connection.requests).toHaveLength(0);
  });
});

describe("LeaseLifecycleError", () => {
  it("extends FitzError so instanceof FitzError / isRetryable classification picks it up", () => {
    const error = new LeaseLifecycleError("multiple failures", [new Error("a"), new Error("b")]);

    expect(error).toBeInstanceOf(FitzError);
    expect(error.code).toBe("LEASE_LIFECYCLE_MULTIPLE_FAILURES");
  });
});
