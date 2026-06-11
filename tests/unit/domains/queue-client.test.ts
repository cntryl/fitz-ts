import { describe, expect, it, vi } from "vite-plus/test";

import type { Connection } from "../../../src/client/connection";
import { BufferWriter } from "../../../src/core/buffer";
import { QueueClient } from "../../../src/domains/queue/client";
import {
  MSG_QUEUE_NOTIFY,
  MSG_QUEUE_RESERVE,
  MSG_QUEUE_SUBSCRIBE,
  MSG_QUEUE_UNSUBSCRIBE,
} from "../../../src/frame/types";

type Handler = (payload: Uint8Array) => void;

class FakeQueueConnection {
  readonly requests: Array<{ messageType: number; payload: Uint8Array }> = [];
  readonly handlers = new Map<number, Handler>();
  reserveResponses: Uint8Array[] = [];
  unsubscribeCount = 0;
  onReserve: (() => void) | null = null;

  async request(messageType: number, payload: Uint8Array): Promise<Uint8Array> {
    this.requests.push({ messageType, payload });
    if (messageType === MSG_QUEUE_SUBSCRIBE) {
      return encodeQueueSubscribeResponse(7n);
    }
    if (messageType === MSG_QUEUE_UNSUBSCRIBE) {
      this.unsubscribeCount += 1;
      return new Uint8Array([0]);
    }
    if (messageType === MSG_QUEUE_RESERVE) {
      this.onReserve?.();
      return this.reserveResponses.shift() ?? encodeQueueReserveResponse([]);
    }
    throw new Error(`unexpected message type ${messageType}`);
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

  onDisconnect(): () => void {
    return () => undefined;
  }

  notify(route: string = "queue://realm/area/resource/ready"): void {
    this.handlers.get(MSG_QUEUE_NOTIFY)?.(encodeQueueNotification(7n, route));
  }
}

describe("QueueClient reserveWhenAvailable", () => {
  it("reserves before waiting and wakes after an empty reserve", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(
      encodeQueueReserveResponse([]),
      encodeQueueReserveResponse([{ id: 1n, token: 2n, body: new Uint8Array([3]) }]),
    );
    const client = new QueueClient(connection as unknown as Connection);
    const iterator = client
      .reserveWhenAvailable("queue://realm/area/resource", {
        leaseSeconds: 30,
        batchSize: 2,
      })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(
        connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
      ).toHaveLength(1);
    });

    connection.notify();

    const result = await pending;
    expect(result.done).toBe(false);
    expect(result.value).toHaveLength(1);
    expect(result.value?.[0].body).toEqual(new Uint8Array([3]));

    await iterator.return?.();
    expect(connection.unsubscribeCount).toBe(1);
  });

  it("does not lose a notification that arrives during an empty reserve", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(
      encodeQueueReserveResponse([]),
      encodeQueueReserveResponse([{ id: 1n, token: 2n, body: new Uint8Array([4]) }]),
    );
    connection.onReserve = () => {
      if (
        connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE).length === 1
      ) {
        connection.notify();
      }
    };
    const client = new QueueClient(connection as unknown as Connection);

    const result = await client
      .reserveWhenAvailable("queue://realm/area/resource", { leaseSeconds: 30 })
      [Symbol.asyncIterator]()
      .next();

    expect(result.done).toBe(false);
    expect(result.value).toHaveLength(1);
    expect(
      connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
    ).toHaveLength(2);
  });

  it("unsubscribes when the iterator is aborted", async () => {
    const connection = new FakeQueueConnection();
    connection.reserveResponses.push(encodeQueueReserveResponse([]));
    const client = new QueueClient(connection as unknown as Connection);
    const controller = new AbortController();
    const iterator = client
      .reserveWhenAvailable("queue://realm/area/resource", {
        leaseSeconds: 30,
        signal: controller.signal,
      })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(
        connection.requests.filter((call) => call.messageType === MSG_QUEUE_RESERVE),
      ).toHaveLength(1);
    });
    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(connection.unsubscribeCount).toBe(1);
  });
});

function encodeQueueSubscribeResponse(subId: bigint): Uint8Array {
  const payload = new Uint8Array(9);
  payload[0] = 0;
  new DataView(payload.buffer).setBigUint64(1, subId, false);
  return payload;
}

function encodeQueueReserveResponse(
  items: Array<{ id: bigint; token: bigint; body: Uint8Array }>,
): Uint8Array {
  const writer = new BufferWriter(64);
  writer.writeU8(0);
  writer.writeU32BE(items.length);
  for (const item of items) {
    writer.writeU64BE(item.id);
    writer.writeU64BE(item.token);
    writer.writeU32BE(item.body.length);
    writer.writeBytes(item.body);
  }
  return writer.getBuffer();
}

function encodeQueueNotification(subId: bigint, route: string): Uint8Array {
  const writer = new BufferWriter(64);
  writer.writeU64BE(subId);
  writer.writeRoute(route);
  return writer.getBuffer();
}
