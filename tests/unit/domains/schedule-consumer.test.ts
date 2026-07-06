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
  unsubscribeCount = 0;

  async request(messageType: number): Promise<Uint8Array> {
    if (messageType === MSG_SCHEDULE_SUBSCRIBE) {
      return new Uint8Array([0, 1, 0, 0, 0, 0, 0, 0, 0, 11]);
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

  onReconnect(): () => void {
    return () => undefined;
  }

  notify(payload: Uint8Array): void {
    this.handlers.get(MSG_SCHEDULE_NOTIFY)?.(encodeScheduleNotification(11n, payload));
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
});

function encodeScheduleNotification(subId: bigint, payload: Uint8Array): Uint8Array {
  const writer = createBufferWriter(32);
  writer.writeU64BE(subId);
  writer.writeU32BE(payload.length);
  writer.writeBytes(payload);
  return writer.getBuffer();
}
