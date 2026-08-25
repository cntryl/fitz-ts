import { describe, expect, it, vi } from "vite-plus/test";

import { createBufferWriter, utf8Decoder, utf8Encoder } from "../../../src/core/buffer";
import { createLeaseClient } from "../../../src/domains/lease/client";
import { createKvClient } from "../../../src/domains/kv/client";
import { createNoticeClient } from "../../../src/domains/notice/client";
import { createQueueClient } from "../../../src/domains/queue/client";
import { createScheduleClient } from "../../../src/domains/schedule/client";
import { createStreamClient } from "../../../src/domains/stream/client";
import { AsyncHandlerOverflowError } from "../../../src/core/errors";
import {
  MSG_LEASE_NOTIFY,
  MSG_LEASE_SUBSCRIBE,
  MSG_LEASE_UNSUBSCRIBE,
  MSG_KV_NOTIFY,
  MSG_KV_SUBSCRIBE,
  MSG_KV_UNSUBSCRIBE,
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
  asyncDispatchAccepted = true;
  asyncDispatchAttempts = 0;

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

  async sendFireAndForget(messageType: number): Promise<void> {
    this.requestCalls.push(messageType);
  }

  expectOptionalResponse(): () => void {
    return () => undefined;
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

  dispatchAsyncHandler(task: () => void | Promise<void>): boolean {
    this.asyncDispatchAttempts += 1;
    if (!this.asyncDispatchAccepted) {
      return false;
    }
    this.pendingHandlers.push(Promise.resolve().then(task));
    return true;
  }

  emitNotification(messageType: number, payload: Uint8Array): void {
    const handler = this.notificationHandlers.get(messageType);
    if (!handler) {
      throw new Error(`No notification handler registered for ${messageType}`);
    }

    handler(payload);
  }

  hasNotificationHandler(messageType: number): boolean {
    return this.notificationHandlers.has(messageType);
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
  const writer = createBufferWriter(16);
  writer.writeU8(0);
  writer.writeU8(1);
  writer.writeU64BE(subId);
  return writer.getBuffer();
}

function encodeQueueSubIdResponse(subId: bigint): Uint8Array {
  const writer = createBufferWriter(16);
  writer.writeU8(0);
  writer.writeU8(1);
  writer.writeU64BE(subId);
  return writer.getBuffer();
}

function encodeKvSubIdResponse(subId: bigint): Uint8Array {
  const writer = createBufferWriter(16);
  writer.writeU8(0);
  writer.writeU64BE(subId);
  return writer.getBuffer();
}

function encodeLeaseSubscribeResponse(subId: bigint): Uint8Array {
  const writer = createBufferWriter(16);
  writer.writeU8(0);
  writer.writeU64BE(subId);
  return writer.getBuffer();
}

function encodeStatusOnlyResponse(): Uint8Array {
  return new Uint8Array([0]);
}

function encodePlainErrorResponse(message: string): Uint8Array {
  const writer = createBufferWriter(64);
  writer.writeU8(1);
  writer.writeU32BE(1);
  writer.writeString(message);
  return writer.getBuffer();
}

function encodeNoticeNotification(subId: bigint, route: string, body: Uint8Array): Uint8Array {
  const writer = createBufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU32BE(body.length);
  writer.writeBytes(body);
  return writer.getBuffer();
}

function encodeKvNotification(subId: bigint, route: string): Uint8Array {
  const writer = createBufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU64BE(1n);
  return writer.getBuffer();
}

function encodeQueueNotification(subId: bigint, route: string): Uint8Array {
  const writer = createBufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU64BE(3n);
  writer.writeU64BE(2n);
  writer.writeU64BE(1n);
  return writer.getBuffer();
}

function encodeLeaseNotification(subId: bigint, route: string): Uint8Array {
  const writer = createBufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU32BE(0);
  return writer.getBuffer();
}

function encodeScheduleNotification(subId: bigint, route: string, payload: Uint8Array): Uint8Array {
  const writer = createBufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU32BE(payload.length);
  writer.writeBytes(payload);
  return writer.getBuffer();
}

function encodeStreamNotification(subId: bigint, route: string, payload: Uint8Array): Uint8Array {
  const writer = createBufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU32BE(payload.length);
  writer.writeBytes(payload);
  return writer.getBuffer();
}

type OverflowCase = {
  name: string;
  responses: Array<[number, Uint8Array]>;
  subscribeType: number;
  notificationType: number;
  notification: Uint8Array;
  unsubscribeType: number;
  subscribe(connection: FakeSubscriptionConnection): Promise<{ completion: Promise<void> }>;
  notifications(connection: FakeSubscriptionConnection): AsyncIterable<unknown>;
};

const overflowCases: OverflowCase[] = [
  {
    name: "KV",
    responses: [
      [MSG_KV_SUBSCRIBE, encodeKvSubIdResponse(1n)],
      [MSG_KV_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ],
    subscribeType: MSG_KV_SUBSCRIBE,
    notificationType: MSG_KV_NOTIFY,
    notification: encodeKvNotification(1n, "kv://realm/area/resource"),
    unsubscribeType: MSG_KV_UNSUBSCRIBE,
    subscribe: (connection) =>
      createKvClient(connection).subscribe("kv://realm/area/resource", () => undefined),
    notifications: (connection) =>
      createKvClient(connection).notifications("kv://realm/area/resource"),
  },
  {
    name: "Queue",
    responses: [
      [MSG_QUEUE_SUBSCRIBE, encodeQueueSubIdResponse(2n)],
      [MSG_QUEUE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ],
    subscribeType: MSG_QUEUE_SUBSCRIBE,
    notificationType: MSG_QUEUE_NOTIFY,
    notification: encodeQueueNotification(2n, "queue://realm/area/resource"),
    unsubscribeType: MSG_QUEUE_UNSUBSCRIBE,
    subscribe: (connection) =>
      createQueueClient(connection).subscribe("queue://realm/area/resource", () => undefined),
    notifications: (connection) =>
      createQueueClient(connection).notifications("queue://realm/area/resource"),
  },
  {
    name: "Lease",
    responses: [
      [MSG_LEASE_SUBSCRIBE, encodeLeaseSubscribeResponse(3n)],
      [MSG_LEASE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ],
    subscribeType: MSG_LEASE_SUBSCRIBE,
    notificationType: MSG_LEASE_NOTIFY,
    notification: encodeLeaseNotification(3n, "lease://realm/area/resource"),
    unsubscribeType: MSG_LEASE_UNSUBSCRIBE,
    subscribe: (connection) =>
      createLeaseClient(connection).subscribe("lease://realm/area/resource", () => undefined),
    notifications: (connection) =>
      createLeaseClient(connection).notifications("lease://realm/area/resource"),
  },
  {
    name: "Notice",
    responses: [
      [MSG_NOTICE_SUBSCRIBE, encodeOptionalSubIdResponse(4n)],
      [MSG_NOTICE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ],
    subscribeType: MSG_NOTICE_SUBSCRIBE,
    notificationType: MSG_NOTICE_NOTIFY,
    notification: encodeNoticeNotification(
      4n,
      "notice://realm/area/resource",
      utf8Encoder.encode("overflow"),
    ),
    unsubscribeType: MSG_NOTICE_UNSUBSCRIBE,
    subscribe: (connection) =>
      createNoticeClient(connection).subscribe("notice://realm/area/resource", () => undefined),
    notifications: (connection) =>
      createNoticeClient(connection).notifications("notice://realm/area/resource"),
  },
  {
    name: "Schedule",
    responses: [
      [MSG_SCHEDULE_SUBSCRIBE, encodeOptionalSubIdResponse(5n)],
      [MSG_SCHEDULE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ],
    subscribeType: MSG_SCHEDULE_SUBSCRIBE,
    notificationType: MSG_SCHEDULE_NOTIFY,
    notification: encodeScheduleNotification(
      5n,
      "schedule://realm/area/resource/run",
      utf8Encoder.encode("overflow"),
    ),
    unsubscribeType: MSG_SCHEDULE_UNSUBSCRIBE,
    subscribe: (connection) =>
      createScheduleClient(connection).subscribe(
        "schedule://realm/area/resource/run",
        () => undefined,
      ),
    notifications: (connection) =>
      createScheduleClient(connection).notifications("schedule://realm/area/resource/run"),
  },
  {
    name: "Stream",
    responses: [
      [MSG_STREAM_SUBSCRIBE, encodeOptionalSubIdResponse(6n)],
      [MSG_STREAM_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ],
    subscribeType: MSG_STREAM_SUBSCRIBE,
    notificationType: MSG_STREAM_NOTIFY,
    notification: encodeStreamNotification(
      6n,
      "stream://realm/area/resource",
      utf8Encoder.encode("{}"),
    ),
    unsubscribeType: MSG_STREAM_UNSUBSCRIBE,
    subscribe: (connection) =>
      createStreamClient(connection).subscribe("stream://realm/area/resource", () => undefined),
    notifications: (connection) =>
      createStreamClient(connection).notifications("stream://realm/area/resource"),
  },
];

describe("Subscription Multiplexing", () => {
  it("retains Notice bookkeeping until a failed wire unsubscribe can be retried", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_NOTICE_SUBSCRIBE, encodeOptionalSubIdResponse(11n)],
      [MSG_NOTICE_UNSUBSCRIBE, encodePlainErrorResponse("try again")],
      [MSG_NOTICE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = createNoticeClient(connection);
    const subscription = await client.subscribe("notice://realm/area/*", async () => undefined);

    await expect(subscription.unsubscribe()).rejects.toThrow("try again");
    await subscription.unsubscribe();

    expect(connection.countRequests(MSG_NOTICE_UNSUBSCRIBE)).toBe(2);
  });

  it("retains KV bookkeeping until a failed wire unsubscribe can be retried", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_KV_SUBSCRIBE, encodeKvSubIdResponse(12n)],
      [MSG_KV_UNSUBSCRIBE, encodePlainErrorResponse("try again")],
      [MSG_KV_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = createKvClient(connection);
    const subscription = await client.subscribe("kv://realm/area/resource", async () => undefined);

    await expect(subscription.unsubscribe()).rejects.toThrow("try again");
    await subscription.unsubscribe();

    expect(connection.countRequests(MSG_KV_UNSUBSCRIBE)).toBe(2);
  });

  it("should single-flight concurrent KV subscriptions and retry after failure", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_KV_SUBSCRIBE, encodeStatusOnlyResponse()],
      [MSG_KV_SUBSCRIBE, encodeKvSubIdResponse(7n)],
      [MSG_KV_SUBSCRIBE, encodeKvSubIdResponse(8n)],
      [MSG_KV_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = createKvClient(connection);
    const pattern = "kv://realm/area/resource";

    const failed = await Promise.allSettled([
      client.subscribe(pattern, async () => undefined),
      client.subscribe(pattern, async () => undefined),
    ]);
    expect(failed.every((result) => result.status === "rejected")).toBe(true);
    expect(connection.countRequests(MSG_KV_SUBSCRIBE)).toBe(1);

    const [first, second] = await Promise.all([
      client.subscribe(pattern, async () => undefined),
      client.subscribe(pattern, async () => undefined),
    ]);
    expect(first).not.toHaveProperty("subId");
    expect(second).not.toHaveProperty("subId");
    expect(connection.countRequests(MSG_KV_SUBSCRIBE)).toBe(2);
    await connection.reconnect();
    expect(first).not.toHaveProperty("subId");
    expect(second).not.toHaveProperty("subId");
    await first.unsubscribe();
    expect(connection.countRequests(MSG_KV_UNSUBSCRIBE)).toBe(0);
    await second.unsubscribe();
    expect(connection.countRequests(MSG_KV_UNSUBSCRIBE)).toBe(1);
  });

  it("should restore notice subscriptions given reconnect when the application still wants them active", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_NOTICE_SUBSCRIBE, encodeOptionalSubIdResponse(11n)],
      [MSG_NOTICE_SUBSCRIBE, encodeOptionalSubIdResponse(12n)],
      [MSG_NOTICE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = createNoticeClient(connection);
    const firstRoutes: string[] = [];
    const secondRoutes: string[] = [];
    const pattern = "notice://realm/area/resource";

    const firstPromise = client.subscribe(pattern, async (msg) => {
      firstRoutes.push(msg.route);
    });
    const secondPromise = client.subscribe(pattern, async (msg) => {
      secondRoutes.push(msg.route);
    });
    const [first, second] = await Promise.all([firstPromise, secondPromise]);

    expect(first).not.toHaveProperty("subId");
    expect(second).not.toHaveProperty("subId");
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
    expect(second).not.toHaveProperty("subId");

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

  it.each(overflowCases)(
    "$name callback subscription fails programmatically when async dispatch overflows",
    async (testCase) => {
      const connection = new FakeSubscriptionConnection(testCase.responses);
      const subscription = await testCase.subscribe(connection);
      connection.asyncDispatchAccepted = false;

      connection.emitNotification(testCase.notificationType, testCase.notification);

      await expect(subscription.completion).rejects.toBeInstanceOf(AsyncHandlerOverflowError);
      await vi.waitFor(() => {
        expect(connection.countRequests(testCase.unsubscribeType)).toBe(1);
      });
      expect(connection.asyncDispatchAttempts).toBe(1);
    },
  );

  it.each(overflowCases)(
    "$name notification iterator rejects when async dispatch overflows",
    async (testCase) => {
      const connection = new FakeSubscriptionConnection(testCase.responses);
      const iterator = testCase.notifications(connection)[Symbol.asyncIterator]();
      const pending = iterator.next();
      await vi.waitFor(() => {
        expect(connection.hasNotificationHandler(testCase.notificationType)).toBe(true);
        expect(connection.countRequests(testCase.subscribeType)).toBe(1);
      });
      await new Promise<void>((resolve) => setImmediate(resolve));
      connection.asyncDispatchAccepted = false;

      connection.emitNotification(testCase.notificationType, testCase.notification);

      await expect(pending).rejects.toBeInstanceOf(AsyncHandlerOverflowError);
      await vi.waitFor(() => {
        expect(connection.countRequests(testCase.unsubscribeType)).toBe(1);
      });
    },
  );

  it("should restore queue subscriptions given reconnect when the application still wants them active", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_QUEUE_SUBSCRIBE, encodeQueueSubIdResponse(21n)],
      [MSG_QUEUE_SUBSCRIBE, encodeQueueSubIdResponse(22n)],
      [MSG_QUEUE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = createQueueClient(connection);
    const firstRoutes: string[] = [];
    const secondRoutes: string[] = [];
    const pattern = "queue://realm/area/resource";

    const firstPromise = client.subscribe(pattern, async (notification) => {
      firstRoutes.push(notification.route);
    });
    const secondPromise = client.subscribe(pattern, async (notification) => {
      secondRoutes.push(notification.route);
    });
    const [first, second] = await Promise.all([firstPromise, secondPromise]);

    expect(first).not.toHaveProperty("subId");
    expect(second).not.toHaveProperty("subId");
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

    connection.emitNotification(MSG_QUEUE_NOTIFY, encodeQueueNotification(21n, pattern));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern]);

    connection.emitNotification(MSG_QUEUE_NOTIFY, encodeQueueNotification(22n, pattern));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([pattern]);
    expect(secondRoutes).toEqual([pattern, pattern, pattern]);

    await second.unsubscribe();
    expect(connection.countRequests(MSG_QUEUE_UNSUBSCRIBE)).toBe(1);
  });

  it("should restore lease subscriptions given reconnect when the application still wants them active", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_LEASE_SUBSCRIBE, encodeLeaseSubscribeResponse(31n)],
      [MSG_LEASE_SUBSCRIBE, encodeLeaseSubscribeResponse(32n)],
      [MSG_LEASE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = createLeaseClient(connection);
    const firstRoutes: string[] = [];
    const secondRoutes: string[] = [];
    const route = "lease://realm/area/resource";

    const firstPromise = client.subscribe(route, async (notification) => {
      firstRoutes.push(notification.route);
    });
    const secondPromise = client.subscribe(route, async (notification) => {
      secondRoutes.push(notification.route);
    });
    const [first, second] = await Promise.all([firstPromise, secondPromise]);

    expect(first).not.toHaveProperty("subId");
    expect(second).not.toHaveProperty("subId");
    expect(connection.countRequests(MSG_LEASE_SUBSCRIBE)).toBe(1);

    connection.emitNotification(MSG_LEASE_NOTIFY, encodeLeaseNotification(31n, route));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([route]);
    expect(secondRoutes).toEqual([route]);

    await first.unsubscribe();
    expect(connection.countRequests(MSG_LEASE_UNSUBSCRIBE)).toBe(0);

    connection.emitNotification(MSG_LEASE_NOTIFY, encodeLeaseNotification(31n, route));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([route]);
    expect(secondRoutes).toEqual([route, route]);

    await connection.reconnect();
    expect(connection.countRequests(MSG_LEASE_SUBSCRIBE)).toBe(2);
    expect(first).not.toHaveProperty("subId");
    expect(second).not.toHaveProperty("subId");

    connection.emitNotification(MSG_LEASE_NOTIFY, encodeLeaseNotification(32n, route));
    await connection.flushHandlers();
    expect(firstRoutes).toEqual([route]);
    expect(secondRoutes).toEqual([route, route, route]);

    await second.unsubscribe();
    expect(connection.countRequests(MSG_LEASE_UNSUBSCRIBE)).toBe(1);
  });

  it("should restore schedule subscriptions given reconnect when the application still wants them active", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_SCHEDULE_SUBSCRIBE, encodeOptionalSubIdResponse(41n)],
      [MSG_SCHEDULE_SUBSCRIBE, encodeOptionalSubIdResponse(42n)],
      [MSG_SCHEDULE_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = createScheduleClient(connection);
    const firstPayloads: string[] = [];
    const secondPayloads: string[] = [];
    const pattern = "schedule://realm/area/resource/run";

    const firstPromise = client.subscribe(pattern, async (notification) => {
      firstPayloads.push(utf8Decoder.decode(notification.payload));
    });
    const secondPromise = client.subscribe(pattern, async (notification) => {
      secondPayloads.push(utf8Decoder.decode(notification.payload));
    });
    const [first, second] = await Promise.all([firstPromise, secondPromise]);

    expect(first).not.toHaveProperty("subId");
    expect(second).not.toHaveProperty("subId");
    expect(connection.countRequests(MSG_SCHEDULE_SUBSCRIBE)).toBe(1);

    connection.emitNotification(
      MSG_SCHEDULE_NOTIFY,
      encodeScheduleNotification(41n, pattern, utf8Encoder.encode("first")),
    );
    await connection.flushHandlers();
    expect(firstPayloads).toEqual(["first"]);
    expect(secondPayloads).toEqual(["first"]);

    await first.unsubscribe();
    expect(connection.countRequests(MSG_SCHEDULE_UNSUBSCRIBE)).toBe(0);

    connection.emitNotification(
      MSG_SCHEDULE_NOTIFY,
      encodeScheduleNotification(41n, pattern, utf8Encoder.encode("second")),
    );
    await connection.flushHandlers();
    expect(firstPayloads).toEqual(["first"]);
    expect(secondPayloads).toEqual(["first", "second"]);

    await connection.reconnect();
    expect(connection.countRequests(MSG_SCHEDULE_SUBSCRIBE)).toBe(2);
    expect(first).not.toHaveProperty("subId");
    expect(second).not.toHaveProperty("subId");

    connection.emitNotification(
      MSG_SCHEDULE_NOTIFY,
      encodeScheduleNotification(41n, pattern, utf8Encoder.encode("stale")),
    );
    await connection.flushHandlers();
    expect(firstPayloads).toEqual(["first"]);
    expect(secondPayloads).toEqual(["first", "second"]);

    connection.emitNotification(
      MSG_SCHEDULE_NOTIFY,
      encodeScheduleNotification(42n, pattern, utf8Encoder.encode("after")),
    );
    await connection.flushHandlers();
    expect(firstPayloads).toEqual(["first"]);
    expect(secondPayloads).toEqual(["first", "second", "after"]);

    await second.unsubscribe();
    expect(connection.countRequests(MSG_SCHEDULE_UNSUBSCRIBE)).toBe(1);
  });

  it("should restore stream subscriptions given reconnect when the application still wants them active", async () => {
    const connection = new FakeSubscriptionConnection([
      [MSG_STREAM_SUBSCRIBE, encodeOptionalSubIdResponse(51n)],
      [MSG_STREAM_SUBSCRIBE, encodeOptionalSubIdResponse(52n)],
      [MSG_STREAM_UNSUBSCRIBE, encodeStatusOnlyResponse()],
    ]);
    const client = createStreamClient(connection);
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

    const firstPromise = client.subscribe(pattern, async (notification) => {
      firstNotifications.push({
        route: notification.route,
        event: notification.event,
        firstResourceOffset: notification.firstResourceOffset,
        firstAreaOffset: notification.firstAreaOffset,
        firstRealmOffset: notification.firstRealmOffset,
        batchSize: notification.batchSize,
      });
    });
    const secondPromise = client.subscribe(pattern, async (notification) => {
      secondRoutes.push(notification.route);
    });
    const [first, second] = await Promise.all([firstPromise, secondPromise]);

    expect(first).not.toHaveProperty("subId");
    expect(second).not.toHaveProperty("subId");
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
