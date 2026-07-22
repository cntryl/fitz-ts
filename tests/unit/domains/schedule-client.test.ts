import { describe, expect, it } from "vite-plus/test";

import { MSG_SCHEDULE_SUBSCRIBE } from "../../../src/frame/types";
import { createScheduleClient } from "../../../src/domains/schedule/client";
import { ScheduleError } from "../../../src/core/errors";

class FakeScheduleConnection {
  constructor(private readonly response = new Uint8Array([0])) {}
  readonly requestCalls: number[] = [];
  readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  readonly reconnectListeners = new Set<() => void | Promise<void>>();

  async request(messageType: number): Promise<Uint8Array> {
    this.requestCalls.push(messageType);
    if (messageType === MSG_SCHEDULE_SUBSCRIBE) {
      return new Uint8Array([0, 1, 0, 0, 0, 0, 0, 0, 0, 7]);
    }
    return this.response;
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

async function expectScheduleRouteFailure(action: Promise<unknown>): Promise<void> {
  let caught: unknown;

  try {
    await action;
  } catch (error) {
    caught = error;
  }

  expect(caught).toBeInstanceOf(ScheduleError);
  expect(caught).toMatchObject({ code: "SCHEDULE_INVALID_ROUTE" });
}

describe("ScheduleClient route validation", () => {
  it("accepts exact four-segment routes before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = createScheduleClient(connection);

    await expect(
      client.create("schedule://realm/area/resource/run", "0 0 * * *", "broadcast"),
    ).resolves.toBe("schedule://realm/area/resource/run");
    expect(connection.requestCalls).toHaveLength(1);
  });

  it("rejects legacy three-segment routes in create before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = createScheduleClient(connection);

    await expectScheduleRouteFailure(
      client.create("schedule://realm/area/resource", "0 0 * * *", "broadcast"),
    );
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects wrong-scheme routes in create before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = createScheduleClient(connection);

    await expectScheduleRouteFailure(
      client.create("queue://realm/area/resource/run", "0 0 * * *", "broadcast"),
    );
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects empty-segment routes in cancel before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = createScheduleClient(connection);

    await expectScheduleRouteFailure(client.cancel("schedule://realm//resource/run"));
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects wildcard routes in cancel before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = createScheduleClient(connection);

    await expectScheduleRouteFailure(client.cancel("schedule://realm/area/resource/*"));
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects subscribe patterns before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = createScheduleClient(connection);

    await expectScheduleRouteFailure(
      client.subscribe("schedule://realm/area/*", async () => undefined),
    );
    expect(connection.requestCalls).toHaveLength(0);
  });
});

describe("ScheduleClient domain errors", () => {
  it("preserves schedule error code 7008", async () => {
    const response = new Uint8Array([
      1,
      0,
      0,
      27,
      96,
      0,
      0,
      0,
      21,
      ...new TextEncoder().encode("invalid delivery mode"),
    ]);
    const client = createScheduleClient(new FakeScheduleConnection(response));

    await expect(
      client.create("schedule://realm/area/resource/run", "0 0 * * *", "single"),
    ).rejects.toMatchObject({
      domainCode: 7008,
      code: "SCHEDULE_REQUEST_FAILED",
    });
  });
});
