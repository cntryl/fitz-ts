import { describe, expect, it } from "vite-plus/test";

import { createBufferWriter } from "../../../src/core/buffer";
import type { Connection } from "../../../src/client/connection";
import { createNoticeClient } from "../../../src/domains/notice/client";
import {
  MSG_NOTICE_NOTIFY,
  MSG_NOTICE_PUBLISH,
  MSG_NOTICE_SUBSCRIBE,
  MSG_NOTICE_UNSUBSCRIBE,
} from "../../../src/frame/types";

function subscribeResponse(subId: bigint): Uint8Array {
  const writer = createBufferWriter();
  writer.writeU8(0);
  writer.writeU8(1);
  writer.writeU64BE(subId);
  return writer.getBuffer();
}

function plainSuccessResponse(): Uint8Array {
  return new Uint8Array([0]);
}

function errorResponse(message: string): Uint8Array {
  const writer = createBufferWriter();
  writer.writeU8(1);
  writer.writeU32BE(0);
  writer.writeString(message);
  return writer.getBuffer();
}

function encodeNoticeNotification(subId: bigint, route: string, body: Uint8Array): Uint8Array {
  const writer = createBufferWriter();
  writer.writeU64BE(subId);
  writer.writeRoute(route);
  writer.writeU32BE(body.length);
  writer.writeBytes(body);
  return writer.getBuffer();
}

class FakeNoticeConnection {
  private readonly responses = new Map<number, Uint8Array[]>();
  private readonly gates = new Map<number, Promise<void>>();
  private readonly reconnectListeners = new Set<() => void | Promise<void>>();
  private readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  private readonly optionalResponseCounts = new Map<number, number>();
  sent: Array<{ messageType: number; payload: Uint8Array }> = [];
  requests: Array<{ messageType: number; payload: Uint8Array }> = [];

  async request(messageType: number, payload: Uint8Array): Promise<Uint8Array> {
    const gate = this.gates.get(messageType);
    if (gate) await gate;
    this.requests.push({ messageType, payload });
    const queued = this.responses.get(messageType)?.shift();
    if (!queued) {
      throw new Error(`FakeNoticeConnection: no queued response for messageType ${messageType}`);
    }
    return queued;
  }

  async sendFireAndForget(messageType: number, payload: Uint8Array): Promise<void> {
    this.sent.push({ messageType, payload });
  }

  expectOptionalResponse(messageType: number): () => void {
    const next = (this.optionalResponseCounts.get(messageType) ?? 0) + 1;
    this.optionalResponseCounts.set(messageType, next);
    return () => {
      const current = this.optionalResponseCounts.get(messageType) ?? 0;
      if (current <= 1) {
        this.optionalResponseCounts.delete(messageType);
        return;
      }
      this.optionalResponseCounts.set(messageType, current - 1);
    };
  }

  optionalResponseCount(messageType: number): number {
    return this.optionalResponseCounts.get(messageType) ?? 0;
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

  onReconnect(listener: () => void | Promise<void>): () => void {
    this.reconnectListeners.add(listener);
    return () => this.reconnectListeners.delete(listener);
  }

  async reconnect(): Promise<void> {
    for (const listener of this.reconnectListeners) {
      await listener();
    }
  }

  registerNotificationHandler(messageType: number, handler: (payload: Uint8Array) => void): void {
    this.notificationHandlers.set(messageType, handler);
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }

  emitNotification(messageType: number, payload: Uint8Array): void {
    const handler = this.notificationHandlers.get(messageType);
    if (!handler) throw new Error(`No notification handler registered for ${messageType}`);
    handler(payload);
  }
}

describe("NoticeClient", () => {
  it("subscribes and delivers a notification to the registered handler", async () => {
    const connection = new FakeNoticeConnection();
    connection.respond(MSG_NOTICE_SUBSCRIBE, subscribeResponse(7n));
    const client = createNoticeClient(connection as unknown as Connection);

    let received: unknown;
    const subscription = await client.subscribe("notice://realm/area/**", async (msg) => {
      received = msg;
    });
    expect(subscription.subId).toBe(7n);

    connection.emitNotification(
      MSG_NOTICE_NOTIFY,
      encodeNoticeNotification(7n, "notice://realm/area/resource", new Uint8Array([1, 2])),
    );
    await Promise.resolve();

    expect(received).toEqual({
      route: "notice://realm/area/resource",
      body: new Uint8Array([1, 2]),
    });

    connection.respond(MSG_NOTICE_UNSUBSCRIBE, plainSuccessResponse());
    await subscription.unsubscribe();
  });

  it("cancels the optional-response registration on a successful publish, not just on failure", async () => {
    const connection = new FakeNoticeConnection();
    const client = createNoticeClient(connection as unknown as Connection);

    await client.publish("notice://realm/area/resource", new Uint8Array([1]));

    // A fire-and-forget PUBLISH never gets a real response — leaving the
    // registration in place after a successful send would leak one
    // optional-response slot per publish() call for the connection's
    // lifetime.
    expect(connection.optionalResponseCount(MSG_NOTICE_PUBLISH)).toBe(0);
  });

  it("issues a fresh wire subscribe for a concurrent subscribe() that lands while an unsubscribe() is still in flight", async () => {
    const connection = new FakeNoticeConnection();
    connection.respond(MSG_NOTICE_SUBSCRIBE, subscribeResponse(7n));
    const client = createNoticeClient(connection as unknown as Connection);

    let receivedA = false;
    const subA = await client.subscribe("notice://realm/area/**", async () => {
      receivedA = true;
    });

    const release = connection.gate(MSG_NOTICE_UNSUBSCRIBE);
    const unsubscribing = subA.unsubscribe();
    await Promise.resolve();

    // A concurrent subscribe() for the SAME pattern lands while that
    // UNSUBSCRIBE is still in flight. It must wait the unsubscribe out
    // rather than reuse the not-yet-deleted shared state — reusing it
    // would register B locally with no corresponding broker subscription.
    let receivedB = false;
    const subscribingB = client.subscribe("notice://realm/area/**", async () => {
      receivedB = true;
    });
    await Promise.resolve();

    connection.respond(MSG_NOTICE_UNSUBSCRIBE, plainSuccessResponse());
    connection.respond(MSG_NOTICE_SUBSCRIBE, subscribeResponse(9n));
    release();
    await unsubscribing;
    const subB = await subscribingB;

    // B's subscribe() only resolved once the unsubscribe settled, and it
    // sent its own fresh wire SUBSCRIBE — a genuinely new subId, not a
    // reuse of A's now-torn-down subscription.
    expect(subB.subId).toBe(9n);

    connection.emitNotification(
      MSG_NOTICE_NOTIFY,
      encodeNoticeNotification(9n, "notice://realm/area/resource", new Uint8Array()),
    );
    await Promise.resolve();

    expect(receivedA).toBe(false);
    expect(receivedB).toBe(true);

    connection.respond(MSG_NOTICE_UNSUBSCRIBE, plainSuccessResponse());
    await subB.unsubscribe();
  });

  it("keeps the local handler registered when the wire UNSUBSCRIBE fails, instead of dropping notifications silently", async () => {
    const connection = new FakeNoticeConnection();
    connection.respond(MSG_NOTICE_SUBSCRIBE, subscribeResponse(7n));
    const client = createNoticeClient(connection as unknown as Connection);

    let received = false;
    const subscription = await client.subscribe("notice://realm/area/**", async () => {
      received = true;
    });

    connection.respond(MSG_NOTICE_UNSUBSCRIBE, errorResponse("broker rejected unsubscribe"));
    await expect(subscription.unsubscribe()).rejects.toThrow("UNSUBSCRIBE failed");

    connection.emitNotification(
      MSG_NOTICE_NOTIFY,
      encodeNoticeNotification(7n, "notice://realm/area/resource", new Uint8Array()),
    );
    await Promise.resolve();

    expect(received).toBe(true);
  });

  it("keeps reporting its own subId after a later, unrelated resubscription reuses the same pattern", async () => {
    const connection = new FakeNoticeConnection();
    connection.respond(MSG_NOTICE_SUBSCRIBE, subscribeResponse(1n));
    const client = createNoticeClient(connection as unknown as Connection);

    const subA = await client.subscribe("notice://realm/area/**", async () => undefined);
    expect(subA.subId).toBe(1n);

    connection.respond(MSG_NOTICE_UNSUBSCRIBE, plainSuccessResponse());
    await subA.unsubscribe();

    connection.respond(MSG_NOTICE_SUBSCRIBE, subscribeResponse(2n));
    const subB = await client.subscribe("notice://realm/area/**", async () => undefined);
    expect(subB.subId).toBe(2n);

    expect(subA.subId).toBe(1n);

    connection.respond(MSG_NOTICE_UNSUBSCRIBE, plainSuccessResponse());
    await subB.unsubscribe();
  });

  it("keeps a live subId across reconnect for a subscription that survives it", async () => {
    const connection = new FakeNoticeConnection();
    connection.respond(MSG_NOTICE_SUBSCRIBE, subscribeResponse(1n));
    const client = createNoticeClient(connection as unknown as Connection);

    const subscription = await client.subscribe("notice://realm/area/**", async () => undefined);
    expect(subscription.subId).toBe(1n);

    connection.respond(MSG_NOTICE_SUBSCRIBE, subscribeResponse(2n));
    await connection.reconnect();

    expect(subscription.subId).toBe(2n);

    connection.respond(MSG_NOTICE_UNSUBSCRIBE, plainSuccessResponse());
    await subscription.unsubscribe();
  });

  it("buffers a notification that arrives before the subscribing call resolves, and flushes it once it does", async () => {
    const connection = new FakeNoticeConnection();
    connection.respond(MSG_NOTICE_SUBSCRIBE, subscribeResponse(9n));
    const client = createNoticeClient(connection as unknown as Connection);

    // initNotifyHandler() registers the MSG_NOTICE_NOTIFY handler
    // synchronously at the start of subscribe(), before the wire SUBSCRIBE
    // round-trip (gated here) resolves and populates patternsBySubId — so a
    // notification for this subId can genuinely arrive with no known
    // pattern yet.
    const releaseSubscribe = connection.gate(MSG_NOTICE_SUBSCRIBE);
    let received: unknown;
    const subscribing = client.subscribe("notice://realm/area/**", async (msg) => {
      received = msg;
    });
    await Promise.resolve();

    connection.emitNotification(
      MSG_NOTICE_NOTIFY,
      encodeNoticeNotification(9n, "notice://realm/area/resource", new Uint8Array([5])),
    );

    releaseSubscribe();
    const subscription = await subscribing;
    await Promise.resolve();

    // The buffered notification must have been flushed to the handler
    // supplied to subscribe() once its subId became known.
    expect(received).toEqual({
      route: "notice://realm/area/resource",
      body: new Uint8Array([5]),
    });

    connection.respond(MSG_NOTICE_UNSUBSCRIBE, plainSuccessResponse());
    await subscription.unsubscribe();
  });
});
