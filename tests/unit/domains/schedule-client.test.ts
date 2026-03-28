import { describe, expect, it } from "vite-plus/test";

import type { Connection } from "../../../src/client/connection";
import { ScheduleClient } from "../../../src/domains/schedule/client";

class FakeScheduleConnection {
  readonly requestCalls: number[] = [];
  readonly notificationHandlers = new Map<
    number,
    (payload: Uint8Array) => void
  >();
  readonly reconnectListeners = new Set<() => void | Promise<void>>();

  async request(messageType: number): Promise<Uint8Array> {
    this.requestCalls.push(messageType);
    return new Uint8Array([0]);
  }

  registerNotificationHandler(
    messageType: number,
    handler: (payload: Uint8Array) => void,
  ): void {
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

describe("ScheduleClient route validation", () => {
  it("rejects legacy three-segment routes in create before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection as unknown as Connection);

    await expect(
      client.create("schedule://realm/area/resource", "0 0 * * *"),
    ).rejects.toMatchObject({
      code: "INVALID_ROUTE",
    });
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects wildcard routes in cancel before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection as unknown as Connection);

    await expect(
      client.cancel("schedule://realm/area/resource/*"),
    ).rejects.toMatchObject({
      code: "INVALID_ROUTE",
    });
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects wildcard subscribe patterns before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection as unknown as Connection);

    await expect(
      client.subscribe("schedule://realm/area/*", async () => undefined),
    ).rejects.toMatchObject({
      code: "INVALID_ROUTE",
    });
    expect(connection.requestCalls).toHaveLength(0);
  });
});
