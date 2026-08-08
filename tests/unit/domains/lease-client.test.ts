import { describe, expect, it } from "vite-plus/test";

import { createBufferWriter } from "../../../src/core/buffer";
import { LeaseLifecycleError } from "../../../src/domains/lease/types";
import { FitzError } from "../../../src/core/errors";
import type { Connection } from "../../../src/client/connection";
import { createLeaseClient } from "../../../src/domains/lease/client";
import { LeaseCodec } from "../../../src/domains/lease/codec";
import {
  MSG_LEASE_ACQUIRE,
  MSG_LEASE_NOTIFY,
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

    const pending = client.acquire("lease://realm/area/resource", 30, { waitSeconds: 12 });
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

    const first = client.acquire("lease://realm/area/first", 30, { waitSeconds: 12 });
    const second = client.acquire("lease://realm/area/second", 30);
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
    const pending = client.acquire("lease://realm/area/resource", 30, { waitSeconds: 1 });
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
});

describe("withLease", () => {
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

    const pending = client.withLease("lease://realm/area/resource", 30, () => "ok");

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

    const pending = client.withLease("lease://realm/area/resource", 30, async (signal) => {
      releaseCallbackStarted();
      await new Promise<void>((resolve) => {
        signal.addEventListener("abort", () => {
          observedAbort = true;
          resolve();
        });
      });
    });

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
    expect(subB.subId).toBe(99n);

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
    expect(subA.subId).toBe(1n);

    connection.respond(MSG_LEASE_UNSUBSCRIBE, plainSuccessResponse());
    await subA.unsubscribe();

    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(2n));
    const subB = await client.subscribe("lease://realm/area/resource", async () => undefined);
    expect(subB.subId).toBe(2n);

    expect(subA.subId).toBe(1n);

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
    expect(subscription.subId).toBe(1n);

    connection.respond(MSG_LEASE_SUBSCRIBE, subscribeResponse(2n));
    await connection.reconnect();

    expect(subscription.subId).toBe(2n);

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

describe("LeaseLifecycleError", () => {
  it("extends FitzError so instanceof FitzError / isRetryable classification picks it up", () => {
    const error = new LeaseLifecycleError("multiple failures", [new Error("a"), new Error("b")]);

    expect(error).toBeInstanceOf(FitzError);
    expect(error.code).toBe("LEASE_LIFECYCLE_MULTIPLE_FAILURES");
  });
});
