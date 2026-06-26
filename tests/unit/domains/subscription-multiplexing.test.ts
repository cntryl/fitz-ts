import { describe, expect, it } from "vite-plus/test";

import type { Connection } from "../../../src/client/connection";
import { BufferWriter, utf8Decoder, utf8Encoder } from "../../../src/core/buffer";
import { LeaseClient } from "../../../src/domains/lease/client";
import { NoticeClient } from "../../../src/domains/notice/client";
import { QueueClient } from "../../../src/domains/queue/client";
import { ScheduleClient } from "../../../src/domains/schedule/client";
import { StreamClient } from "../../../src/domains/stream/client";
import {
  MSG_LEASE_NOTIFY,
  MSG_LEASE_SUBSCRIBE,
  MSG_LEASE_UNSUBSCRIBE,
  MSG_NOTICE_NOTIFY,
  MSG_NOTICE_SUBSCRIBE,
  MSG_NOTICE_UNSUBSCRIBE,
  MSG_QUEUE_NOTIFY,
  MSG_QUEUE_SUBSCRIBE,
  MSG_QUEUE_UNSUBSCRIBE,
  MSG_SCHEDULE_NOTIFY,
  MSG_SCHEDULE_SUBSCRIBE,
  MSG_SCHEDULE_UNSUBSCRIBE,
  MSG_STREAM_NOTIFY,
  MSG_STREAM_SUBSCRIBE,
  MSG_STREAM_UNSUBSCRIBE,
} from "../../../src/frame/types";

class FakeSubscriptionConnection {
  private readonly responses = new Map<number, Uint8Array[]>();
  private readonly notificationHandlers = new Map<number, (payload: Uint8Array) => void>();
  private readonly reconnectListeners = new Set<() => void | Promise<void>>();
  private readonly pendingHandlers: Promise<void>[] = [];
  readonly requestCalls: number[] = [];

  constructor(responses: Array<[number, Uint8Array]>) {
    for (const [messageType, payload] of responses) {
      const existing = this.responses.get(messageType);
      if (existing) {
        existing.push(payload);
      } else {
        this.responses.set(messageType, [payload]);
      }
    }
  }

  async request(messageType: number): Promise<Uint8Array> {
    this.requestCalls.push(messageType);
    const responses = this.responses.get(messageType);
    if (!responses || responses.length === 0) {
      throw new Error(`No response configured for message type ${messageType}`);
    }

    const response = responses.length > 1 ? responses.shift()! : responses[0];
    return response.slice();
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
    this.pendingHandlers.push(Promise.resolve().then(task));
  }

  emitNotification(messageType: number, payload: Uint8Array): void {
    const handler = this.notificationHandlers.get(messageType);
    if (!handler) {
      throw new Error(`No notification handler registered for ${messageType}`);
    }

    handler(payload);
  }

  countRequests(messageType: number): number {
    return this.requestCalls.filter((value) => value === messageType).length;
  }

  async reconnect(): Promise<void> {
    for (const listener of this.reconnectListeners) {
      await listener();
    }
  }

  async flushHandlers(): Promise<void> {
    while (this.pendingHandlers.length > 0) {
      const handlers = this.pendingHandlers.splice(0);
      await Promise.all(handlers);
    }
  }
}

function encodeOptionalSubIdResponse(subId: bigint): Uint8Array {
  const writer = new BufferWriter(16);
  writer.writeU8(0);
  writer.writeU8(1);
  writer.writeU64BE(subId);
  return writer.getBuffer();
}

function encodeQueueSubIdResponse(subId: bigint): Uint8Array {
  const writer = new BufferWriter(16);
  writer.writeU8(0);
  writer.writeU64BE(subId);
  return writer.getBuffer();
}

function encodeLeaseSubscribeResponse(subId: bigint): Uint8Array {
  const writer = new BufferWriter(16);
  writer.writeU8(0);
  writer.writeU64BE(subId);
  return writer.getBuffer();
}

function encodeStatusOnlyResponse(): Uint8Array {
  return new Uint8Array([0]);
}

function encodeNoticeNotification(subId: bigint, route: string, body: Uint8Array): Uint8Array {
  const writer = new BufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU32BE(body.length);
  writer.writeBytes(body);
  return writer.getBuffer();
}

function encodeQueueNotification(subId: bigint, route: string): Uint8Array {
  const writer = new BufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  return writer.getBuffer();
}

function encodeLeaseNotification(subId: bigint, route: string): Uint8Array {
  const writer = new BufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  return writer.getBuffer();
}

function encodeScheduleNotification(subId: bigint, payload: Uint8Array): Uint8Array {
  const writer = new BufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeU32BE(payload.length);
  writer.writeBytes(payload);
  return writer.getBuffer();
}

function encodeStreamNotification(subId: bigint, route: string, payload: Uint8Array): Uint8Array {
  const writer = new BufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU32BE(payload.length);
  writer.writeBytes(payload);
  return writer.getBuffer();
}

describe("Subscription Multiplexing", () => {
  it("notice client multiplexes duplicate subscriptions locally and on reconnect", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_NOTICE_SUBSCRIBE, encodeOptionalSubIdResponse(11n)],
      [MSG_NOTICE_SUBSCRIBE, encodeOptionalSubIdResponse(12n)],
      [MSG_NOTICE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = new NoticeClient(connection as unknown as Connection);
    const firstRoutes: string[] = [];
    const secondRoutes: string[] = [];
    const pattern = "notice://realm/area/resource";

    const first = await client.subscribe(pattern, async (msg) => {
      firstRoutes.push(msg.route);
    });
    const second = await client.subscribe(pattern, async (msg) => {
      secondRoutes.push(msg.route);
    });

    expect(first.subId).toBe(11n);
    expect(second.subId).toBe(11n);
    expect(connection.countRequests(MSG_NOTICE_SUBSCRIBE)).toBe(1);

    connection.emitNotification(
      MSG_NOTICE_NOTIFY,
      encodeNoticeNotification(11n, pattern, utf8Encoder.encode("first")),
    );
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern]);

    await first.unsubscribe();
    expect(connection.countRequests(MSG_NOTICE_UNSUBSCRIBE)).toBe(0);

    connection.emitNotification(
      MSG_NOTICE_NOTIFY,
      encodeNoticeNotification(11n, pattern, utf8Encoder.encode("second")),
    );
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern]);

    await connection.reconnect();
    expect(connection.countRequests(MSG_NOTICE_SUBSCRIBE)).toBe(2);

    connection.emitNotification(
      MSG_NOTICE_NOTIFY,
      encodeNoticeNotification(11n, pattern, utf8Encoder.encode("stale")),
    );
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern]);

    connection.emitNotification(
      MSG_NOTICE_NOTIFY,
      encodeNoticeNotification(12n, pattern, utf8Encoder.encode("after")),
    );
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern, pattern]);

    await second.unsubscribe();
    expect(connection.countRequests(MSG_NOTICE_UNSUBSCRIBE)).toBe(1);
  });

  it("queue client keeps one wire subscription per pattern", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_QUEUE_SUBSCRIBE, encodeQueueSubIdResponse(21n)],
      [MSG_QUEUE_SUBSCRIBE, encodeQueueSubIdResponse(22n)],
      [MSG_QUEUE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = new QueueClient(connection as unknown as Connection);
    const firstRoutes: string[] = [];
    const secondRoutes: string[] = [];
    const pattern = "queue://realm/area/resource";

    const first = await client.subscribe(pattern, async (notification) => {
      firstRoutes.push(notification.route);
    });
    const second = await client.subscribe(pattern, async (notification) => {
      secondRoutes.push(notification.route);
    });

    expect(first.subId).toBe(21n);
    expect(second.subId).toBe(21n);
    expect(connection.countRequests(MSG_QUEUE_SUBSCRIBE)).toBe(1);

    connection.emitNotification(MSG_QUEUE_NOTIFY, encodeQueueNotification(21n, pattern));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern]);

    await first.unsubscribe();
    expect(connection.countRequests(MSG_QUEUE_UNSUBSCRIBE)).toBe(0);

    connection.emitNotification(MSG_QUEUE_NOTIFY, encodeQueueNotification(21n, pattern));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern]);

    await connection.reconnect();
    expect(connection.countRequests(MSG_QUEUE_SUBSCRIBE)).toBe(2);

    connection.emitNotification(MSG_QUEUE_NOTIFY, encodeQueueNotification(21n, `${pattern}/ready`));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern]);

    connection.emitNotification(MSG_QUEUE_NOTIFY, encodeQueueNotification(22n, `${pattern}/ready`));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern, pattern]);

    await second.unsubscribe();
    expect(connection.countRequests(MSG_QUEUE_UNSUBSCRIBE)).toBe(1);
  });

  it("lease client keeps one wire subscription per pattern", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_LEASE_SUBSCRIBE, encodeLeaseSubscribeResponse(31n)],
      [MSG_LEASE_SUBSCRIBE, encodeLeaseSubscribeResponse(32n)],
      [MSG_LEASE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = new LeaseClient(connection as unknown as Connection);
    const firstRoutes: string[] = [];
    const secondRoutes: string[] = [];
    const pattern = "lease://realm/area/resource";

    const first = await client.subscribe(pattern, async (notification) => {
      firstRoutes.push(notification.route);
    });
    const second = await client.subscribe(pattern, async (notification) => {
      secondRoutes.push(notification.route);
    });

    expect(first.subId).toBe(31n);
    expect(second.subId).toBe(31n);
    expect(connection.countRequests(MSG_LEASE_SUBSCRIBE)).toBe(1);

    connection.emitNotification(MSG_LEASE_NOTIFY, encodeLeaseNotification(31n, pattern));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern]);

    await first.unsubscribe();
    expect(connection.countRequests(MSG_LEASE_UNSUBSCRIBE)).toBe(0);

    connection.emitNotification(MSG_LEASE_NOTIFY, encodeLeaseNotification(31n, pattern));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern]);

    await connection.reconnect();
    expect(connection.countRequests(MSG_LEASE_SUBSCRIBE)).toBe(2);

    connection.emitNotification(MSG_LEASE_NOTIFY, encodeLeaseNotification(32n, pattern));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern, pattern]);

    await second.unsubscribe();
    expect(connection.countRequests(MSG_LEASE_UNSUBSCRIBE)).toBe(1);
  });

  it("schedule client keeps one wire subscription per pattern", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_SCHEDULE_SUBSCRIBE, encodeOptionalSubIdResponse(41n)],
      [MSG_SCHEDULE_SUBSCRIBE, encodeOptionalSubIdResponse(42n)],
      [MSG_SCHEDULE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = new ScheduleClient(connection as unknown as Connection);
    const firstPayloads: string[] = [];
    const secondPayloads: string[] = [];
    const pattern = "schedule://realm/area/resource/run";

    const first = await client.subscribe(pattern, async (notification) => {
      firstPayloads.push(utf8Decoder.decode(notification.payload));
    });
    const second = await client.subscribe(pattern, async (notification) => {
      secondPayloads.push(utf8Decoder.decode(notification.payload));
    });

    expect(first.subId).toBe(41n);
    expect(second.subId).toBe(41n);
    expect(connection.countRequests(MSG_SCHEDULE_SUBSCRIBE)).toBe(1);

    connection.emitNotification(
      MSG_SCHEDULE_NOTIFY,
      encodeScheduleNotification(41n, utf8Encoder.encode("first")),
    );
    await connection.flushHandlers();
    expect(firstPayloads).toEqual(["first"]);
    expect(secondPayloads).toEqual(["first"]);

    await first.unsubscribe();
    expect(connection.countRequests(MSG_SCHEDULE_UNSUBSCRIBE)).toBe(0);

    connection.emitNotification(
      MSG_SCHEDULE_NOTIFY,
      encodeScheduleNotification(41n, utf8Encoder.encode("second")),
    );
    await connection.flushHandlers();
    expect(firstPayloads).toEqual(["first"]);
    expect(secondPayloads).toEqual(["first", "second"]);

    await connection.reconnect();
    expect(connection.countRequests(MSG_SCHEDULE_SUBSCRIBE)).toBe(2);

    connection.emitNotification(
      MSG_SCHEDULE_NOTIFY,
      encodeScheduleNotification(41n, utf8Encoder.encode("stale")),
    );
    await connection.flushHandlers();
    expect(firstPayloads).toEqual(["first"]);
    expect(secondPayloads).toEqual(["first", "second"]);

    connection.emitNotification(
      MSG_SCHEDULE_NOTIFY,
      encodeScheduleNotification(42n, utf8Encoder.encode("after")),
    );
    await connection.flushHandlers();
    expect(firstPayloads).toEqual(["first"]);
    expect(secondPayloads).toEqual(["first", "second", "after"]);

    await second.unsubscribe();
    expect(connection.countRequests(MSG_SCHEDULE_UNSUBSCRIBE)).toBe(1);
  });

  it("stream client keeps one wire subscription per pattern", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_STREAM_SUBSCRIBE, encodeOptionalSubIdResponse(51n)],
      [MSG_STREAM_SUBSCRIBE, encodeOptionalSubIdResponse(52n)],
      [MSG_STREAM_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = new StreamClient(connection as unknown as Connection);
    const firstNotifications: Array<{
      route: string;
      event?: string;
      firstResourceOffset?: bigint;
      firstAreaOffset?: bigint;
      firstRealmOffset?: bigint;
      batchSize?: number;
    }> = [];
    const secondRoutes: string[] = [];
    const pattern = "stream://realm/area/resource";

    const first = await client.subscribe(pattern, async (notification) => {
      firstNotifications.push({
        route: notification.route,
        event: notification.event,
        firstResourceOffset: notification.firstResourceOffset,
        firstAreaOffset: notification.firstAreaOffset,
        firstRealmOffset: notification.firstRealmOffset,
        batchSize: notification.batchSize,
      });
    });
    const second = await client.subscribe(pattern, async (notification) => {
      secondRoutes.push(notification.route);
    });

    expect(first.subId).toBe(51n);
    expect(second.subId).toBe(51n);
    expect(connection.countRequests(MSG_STREAM_SUBSCRIBE)).toBe(1);

    connection.emitNotification(
      MSG_STREAM_NOTIFY,
      encodeStreamNotification(
        51n,
        pattern,
        utf8Encoder.encode(
          JSON.stringify({
            event: "committed",
            first_resource_offset: 0,
            last_resource_offset: 0,
            first_area_offset: 11,
            last_area_offset: 11,
            first_realm_offset: 21,
            last_realm_offset: 21,
            batch_size: 1,
          }),
        ),
      ),
    );
    await connection.flushHandlers();
    expect(firstNotifications).toEqual([
      {
        route: pattern,
        event: "committed",
        firstResourceOffset: 0n,
        firstAreaOffset: 11n,
        firstRealmOffset: 21n,
        batchSize: 1,
      },
    ]);
    expect(secondRoutes).toEqual([pattern]);

    await first.unsubscribe();
    expect(connection.countRequests(MSG_STREAM_UNSUBSCRIBE)).toBe(0);

    connection.emitNotification(
      MSG_STREAM_NOTIFY,
      encodeStreamNotification(51n, pattern, utf8Encoder.encode("{}")),
    );
    await connection.flushHandlers();
    expect(firstNotifications).toHaveLength(1);
    expect(secondRoutes).toEqual([pattern, pattern]);

    await connection.reconnect();
    expect(connection.countRequests(MSG_STREAM_SUBSCRIBE)).toBe(2);

    connection.emitNotification(
      MSG_STREAM_NOTIFY,
      encodeStreamNotification(51n, pattern, utf8Encoder.encode("{}")),
    );
    await connection.flushHandlers();
    expect(firstNotifications).toHaveLength(1);
    expect(secondRoutes).toEqual([pattern, pattern]);

    connection.emitNotification(
      MSG_STREAM_NOTIFY,
      encodeStreamNotification(52n, pattern, utf8Encoder.encode("{}")),
    );
    await connection.flushHandlers();
    expect(firstNotifications).toHaveLength(1);
    expect(secondRoutes).toEqual([pattern, pattern, pattern]);

    await second.unsubscribe();
    expect(connection.countRequests(MSG_STREAM_UNSUBSCRIBE)).toBe(1);
  });
});
