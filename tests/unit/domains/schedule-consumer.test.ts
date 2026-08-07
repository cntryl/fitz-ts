import { describe, expect, it, vi } from "vite-plus/test";

import { createBufferWriter } from "../../../src/core/buffer";
import { createScheduleClient } from "../../../src/domains/schedule/client";
import {
  MSG_SCHEDULE_NOTIFY,
  MSG_SCHEDULE_SUBSCRIBE,
  MSG_SCHEDULE_UNSUBSCRIBE,
} from "../../../src/frame/types";

type Handler = (payload: Uint8Array) => void;

class FakeScheduleConsumerConnection {
  readonly handlers = new Map<number, Handler>();
  readonly disconnectListeners = new Set<() => void>();
  readonly reconnectListeners = new Set<() => void | Promise<void>>();
  readonly gates = new Map<number, Promise<void>>();
  subscribeSubId = 11n;
  unsubscribeCount = 0;

  async request(messageType: number): Promise<Uint8Array> {
    const gate = this.gates.get(messageType);
    if (gate) await gate;
    if (messageType === MSG_SCHEDULE_SUBSCRIBE) {
      const bytes = new Uint8Array(10);
      bytes[0] = 0;
      bytes[1] = 1;
      new DataView(bytes.buffer).setBigUint64(2, this.subscribeSubId, false);
      return bytes;
    }
    if (messageType === MSG_SCHEDULE_UNSUBSCRIBE) {
      this.unsubscribeCount += 1;
      return new Uint8Array([0]);
    }
    return new Uint8Array([0]);
  }

  registerNotificationHandler(messageType: number, handler: Handler): void {
    this.handlers.set(messageType, handler);
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }

  onReconnect(listener: () => void | Promise<void>): () => void {
    this.reconnectListeners.add(listener);
    return () => this.reconnectListeners.delete(listener);
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

  async reconnect(): Promise<void> {
    for (const listener of this.reconnectListeners) {
      await listener();
    }
  }

  disconnect(): void {
    for (const listener of this.disconnectListeners) {
      listener();
    }
  }

  notify(payload: Uint8Array, subId = 11n): void {
    this.handlers.get(MSG_SCHEDULE_NOTIFY)?.(
      encodeScheduleNotification(subId, "schedule://realm/area/resource/run", payload),
    );
  }
}

describe("ScheduleClient waitForNotifications", () => {
  it("yields notifications in order", async () => {
    const connection = new FakeScheduleConsumerConnection();
    const client = createScheduleClient(connection);
    const iterator = client
      .waitForNotifications("schedule://realm/area/resource/run")
      [Symbol.asyncIterator]();

    const first = iterator.next();
    await vi.waitFor(() => {
      expect(connection.handlers.has(MSG_SCHEDULE_NOTIFY)).toBe(true);
    });
    connection.notify(new Uint8Array([1]));
    connection.notify(new Uint8Array([2]));
    await Promise.resolve();

    await expect(first).resolves.toMatchObject({
      done: false,
      value: { payload: new Uint8Array([1]) },
    });
    await expect(iterator.next()).resolves.toMatchObject({
      done: false,
      value: { payload: new Uint8Array([2]) },
    });

    await iterator.return?.();
    expect(connection.unsubscribeCount).toBe(1);
  });

  it("does not lose notifications that arrive before the iterator waits", async () => {
    const connection = new FakeScheduleConsumerConnection();
    const client = createScheduleClient(connection);
    const iterator = client
      .waitForNotifications("schedule://realm/area/resource/run")
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(connection.handlers.has(MSG_SCHEDULE_NOTIFY)).toBe(true);
    });
    connection.notify(new Uint8Array([3]));
    await Promise.resolve();

    await expect(pending).resolves.toMatchObject({
      done: false,
      value: { payload: new Uint8Array([3]) },
    });
  });

  it("unsubscribes when waitForNotifications is aborted while waiting", async () => {
    const connection = new FakeScheduleConsumerConnection();
    const client = createScheduleClient(connection);
    const controller = new AbortController();
    const iterator = client
      .waitForNotifications("schedule://realm/area/resource/run", {
        signal: controller.signal,
      })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(connection.handlers.has(MSG_SCHEDULE_NOTIFY)).toBe(true);
    });
    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(connection.unsubscribeCount).toBe(1);
  });

  it("wakes on disconnect instead of hanging forever while idle-parked", async () => {
    const connection = new FakeScheduleConsumerConnection();
    const client = createScheduleClient(connection);
    const iterator = client
      .waitForNotifications("schedule://realm/area/resource/run")
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    // Wait until waitForNotifications has actually reached its own
    // onDisconnect registration (which happens after the subscribe() call
    // it awaits internally resolves) — not just until the notify handler
    // is registered, which happens earlier, inside subscribe() itself.
    await vi.waitFor(() => {
      expect(connection.disconnectListeners.size).toBeGreaterThan(0);
    });

    connection.disconnect();

    // Pre-fix, ScheduleConnectionPort didn't even include
    // DisconnectListenerPort, so there's no wake source at all and a
    // disconnect while idle-parked here would hang forever. The wake must
    // not be a sticky "stop" flag either (see the next test) — it should
    // just let the loop go back to waiting, ready to pick up a notification
    // once one arrives.
    connection.notify(new Uint8Array([7]));
    await expect(pending).resolves.toMatchObject({
      done: false,
      value: { payload: new Uint8Array([7]) },
    });

    await iterator.return?.();
    expect(connection.unsubscribeCount).toBe(1);
  });

  it("keeps iterating across a transient disconnect that reconnects, instead of terminating the iterator", async () => {
    const connection = new FakeScheduleConsumerConnection();
    const client = createScheduleClient(connection);
    const iterator = client
      .waitForNotifications("schedule://realm/area/resource/run")
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(connection.disconnectListeners.size).toBeGreaterThan(0);
    });

    // A disconnect immediately followed by a successful reconnect must not
    // terminate the iterator — a sticky "disconnected" flag would throw
    // here instead of letting the iterator keep waiting for (and then
    // receiving) the notification below. This matches the equivalent
    // notification-waiting generators in the KV, Lease, Notice, and Stream
    // domains, none of which treat disconnect as terminal either.
    connection.disconnect();
    await connection.reconnect();

    connection.notify(new Uint8Array([9]));
    await expect(pending).resolves.toMatchObject({
      done: false,
      value: { payload: new Uint8Array([9]) },
    });

    await iterator.return?.();
  });
});

describe("ScheduleClient subscribe/unsubscribe", () => {
  it("issues a fresh wire subscribe for a concurrent subscribe() that lands while an unsubscribe() is still in flight", async () => {
    const connection = new FakeScheduleConsumerConnection();
    const client = createScheduleClient(connection);

    let receivedA = false;
    const subA = await client.subscribe("schedule://realm/area/*/*", async () => {
      receivedA = true;
    });

    const release = connection.gate(MSG_SCHEDULE_UNSUBSCRIBE);
    const unsubscribing = subA.unsubscribe();
    await Promise.resolve();

    // A concurrent subscribe() for the SAME pattern lands while that
    // UNSUBSCRIBE is still in flight. It must wait the unsubscribe out
    // rather than reuse the not-yet-deleted shared state — reusing it
    // would register B locally with no corresponding broker subscription.
    let receivedB = false;
    const subscribingB = client.subscribe("schedule://realm/area/*/*", async () => {
      receivedB = true;
    });
    await Promise.resolve();

    connection.subscribeSubId = 22n;
    release();
    await unsubscribing;
    const subB = await subscribingB;

    // B's subscribe() only resolved once the unsubscribe settled, and it
    // sent its own fresh wire SUBSCRIBE — a genuinely new subId, not a
    // reuse of A's now-torn-down subscription.
    expect(subB.subId).toBe(22n);

    connection.notify(new Uint8Array([9]), 22n);
    await Promise.resolve();

    expect(receivedA).toBe(false);
    expect(receivedB).toBe(true);

    await subB.unsubscribe();
  });

  it("keeps a live subId across reconnect for a subscription that survives it", async () => {
    const connection = new FakeScheduleConsumerConnection();
    connection.subscribeSubId = 1n;
    const client = createScheduleClient(connection);

    const subscription = await client.subscribe("schedule://realm/area/*/*", async () => undefined);
    expect(subscription.subId).toBe(1n);

    connection.subscribeSubId = 2n;
    await connection.reconnect();

    expect(subscription.subId).toBe(2n);
  });
});

function encodeScheduleNotification(subId: bigint, route: string, payload: Uint8Array): Uint8Array {
  const writer = createBufferWriter(32);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU32BE(payload.length);
  writer.writeBytes(payload);
  return writer.getBuffer();
}
