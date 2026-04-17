import { describe, expect, it } from "vite-plus/test";

import type { Connection } from "../../../src/client/connection";
import { ScheduleClient } from "../../../src/domains/schedule/client";

class FakeScheduleConnection {
  readonly requestCalls: number[] = [];
  readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  readonly reconnectListeners = new Set<() => void | Promise<void>>();

  async request(messageType: number): Promise<Uint8Array> {
    this.requestCalls.push(messageType);
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

async function expectRouteValidationFailure(
  action: Promise<unknown>,
  messageFragment: string,
  expectedCode = "INVALID_ROUTE",
): Promise<void> {
  let error: unknown = null;
  let resolved = false;

  try {
    await action;
    resolved = true;
  } catch (caught) {
    error = caught;
  }

  expect(resolved).toBe(false);
  expect(error).not.toBeNull();
  expect(error).toMatchObject({ code: expectedCode });
  expect((error as Error).message).toContain(messageFragment);
}

describe("ScheduleClient route validation", () => {
  it("accepts exact four-segment routes before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection as unknown as Connection);

    await expect(client.create("schedule://realm/area/resource/run", "0 0 * * *")).resolves.toBe(
      "schedule://realm/area/resource/run",
    );
    expect(connection.requestCalls).toHaveLength(1);
  });

  it("rejects legacy three-segment routes in create before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.create("schedule://realm/area/resource", "0 0 * * *"),
      "expected schedule://{realm}/{area}/{resource}/{operation}",
    );
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects wrong-scheme routes in create before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.create("queue://realm/area/resource/run", "0 0 * * *"),
      "expected schedule://{realm}/{area}/{resource}/{operation}",
    );
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects empty-segment routes in cancel before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.cancel("schedule://realm//resource/run"),
      "no empty segments or wildcards",
    );
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects wildcard routes in cancel before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.cancel("schedule://realm/area/resource/*"),
      "no empty segments or wildcards",
    );
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects wildcard subscribe patterns before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = new ScheduleClient(connection as unknown as Connection);

    await expectRouteValidationFailure(
      client.subscribe("schedule://realm/area/*", async () => undefined),
      "no empty segments or wildcards",
    );
    expect(connection.requestCalls).toHaveLength(0);
  });
});
