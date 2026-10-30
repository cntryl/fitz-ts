import { errorPayload } from "../helpers/wire-errors";
import { writeErrorCode } from "../helpers/wire-errors";
import { describe, expect, it } from "vite-plus/test";

import { MSG_SCHEDULE_SUBSCRIBE } from "../../../src/frame/types";
import { createScheduleClient } from "../../../src/domains/schedule/client";
import { ScheduleError } from "../../../src/core/errors";
import { createBufferWriter } from "../../../src/core/buffer";

class FakeScheduleConnection {
  constructor(private readonly response: Uint8Array = new Uint8Array([0])) {}
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

  onDisconnect(): () => void {
    return () => undefined;
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
      client.create("schedule://realm/area/resource/run", {
        cron: "0 0 * * *",
        deliveryMode: "Broadcast",
      }),
    ).resolves.toBeUndefined();
    expect(connection.requestCalls).toHaveLength(1);
  });

  it("rejects obsolete three-segment routes in create before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = createScheduleClient(connection);

    await expectScheduleRouteFailure(
      client.create("schedule://realm/area/resource", {
        cron: "0 0 * * *",
        deliveryMode: "Broadcast",
      }),
    );
    expect(connection.requestCalls).toHaveLength(0);
  });

  it("rejects wrong-scheme routes in create before sending", async () => {
    const connection = new FakeScheduleConnection();
    const client = createScheduleClient(connection);

    await expectScheduleRouteFailure(
      client.create("queue://realm/area/resource/run", {
        cron: "0 0 * * *",
        deliveryMode: "Broadcast",
      }),
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
  it.each(["createBatch", "listV2"] as const)(
    "preserves coded errors for %s",
    async (operation) => {
      const client = createScheduleClient(new FakeScheduleConnection(errorPayload(7005, "busy")));
      const action = operation === "createBatch" ? client.createBatch([]) : client.listV2();
      await expect(action).rejects.toMatchObject({ domainCode: 7005, errorClass: 5, effect: 0 });
    },
  );
  it("preserves the message in a common CREATE error", async () => {
    const client = createScheduleClient(
      new FakeScheduleConnection(errorPayload(7054, "invalid delivery mode")),
    );
    await expect(
      client.create("schedule://realm/area/resource/run", {
        cron: "0 0 * * *",
        deliveryMode: "Single",
      }),
    ).rejects.toMatchObject({ domainCode: 7054, message: "CREATE failed: invalid delivery mode" });
  });
  it("preserves coded backend disposition in LIST", async () => {
    const client = createScheduleClient(
      new FakeScheduleConnection(errorPayload(7006, "backend busy")),
    );
    await expect(client.entries("schedule://realm/**").next()).rejects.toMatchObject({
      domainCode: 7006,
      errorClass: 6,
      effect: 1,
    });
  });
  it("rejects a legacy uncoded error", async () => {
    const client = createScheduleClient(
      new FakeScheduleConnection(new Uint8Array([1, 0, 0, 0, 4, 98, 117, 115, 121])),
    );
    await expect(client.createBatch([])).rejects.toThrow();
  });
});

it("should preserve ingress capacity when canceling schedule", async () => {
  // Arrange
  const writer = createBufferWriter(100);
  writer.writeU8(1);
  writeErrorCode(writer, 7010);
  writer.writeString("not accepted");
  const client = createScheduleClient(new FakeScheduleConnection(writer.getBuffer()));
  // Act
  const result = client.cancel("schedule://realm/area/resource/run");
  // Assert
  await expect(result).rejects.toMatchObject({ domainCode: 7010 });
});
