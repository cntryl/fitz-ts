import { describe, expect, it } from "vite-plus/test";

import { MSG_SCHEDULE_SUBSCRIBE } from "../../../src/frame/types";
import { ScheduleClient } from "../../../src/domains/schedule/client";

class FakeScheduleConnection {
  readonly requestCalls: number[] = [];
  readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  readonly reconnectListeners = new Set<() => void | Promise<void>>();

  async request(messageType: number): Promise<Uint8Array> {
    this.requestCalls.push(messageType);
    if (messageType === MSG_SCHEDULE_SUBSCRIBE) {
      return new Uint8Array([0, 1, 0, 0, 0, 0, 0, 0, 0, 7]);
    }
    return new Uint8Array([0]);
  }

  registerNotificationHandler(messageType: number, handler: (payload: Uint8Array) => void): void {
    this.notificationHandlers.set(messageType, handler);
  }

  onReconnect(listener: () => void | Promise<void>): () => void {
    this.reconnectListeners.add(listener);
    return () => {
      this.reconnectListeners.delete(listener);
    };
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }
}

describe("ScheduleClient route forwarding", () => {
  it("accepts exact four-segment routes before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection);

    await expect(client.create("schedule://realm/area/resource/run", "0 0 * * *")).resolves.toBe(
      "schedule://realm/area/resource/run",
    );
    expect(connection.requestCalls).toHaveLength(1);
  });

  it("forwards legacy three-segment routes in create without local validation", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection);

    await expect(client.create("schedule://realm/area/resource", "0 0 * * *")).resolves.toBe(
      "schedule://realm/area/resource",
    );
    expect(connection.requestCalls).toHaveLength(1);
  });

  it("forwards wrong-scheme routes in create without local validation", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection);

    await expect(client.create("queue://realm/area/resource/run", "0 0 * * *")).resolves.toBe(
      "queue://realm/area/resource/run",
    );
    expect(connection.requestCalls).toHaveLength(1);
  });

  it("forwards empty-segment routes in cancel without local validation", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection);

    await expect(client.cancel("schedule://realm//resource/run")).resolves.toBeUndefined();
    expect(connection.requestCalls).toHaveLength(1);
  });

  it("forwards wildcard routes in cancel without local validation", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection);

    await expect(client.cancel("schedule://realm/area/resource/*")).resolves.toBeUndefined();
    expect(connection.requestCalls).toHaveLength(1);
  });

  it("forwards subscribe patterns without local validation", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection);

    await expect(
      client.subscribe("schedule://realm/area/*", async () => undefined),
    ).resolves.toBeDefined();
    expect(connection.requestCalls).toHaveLength(1);
  });
});
