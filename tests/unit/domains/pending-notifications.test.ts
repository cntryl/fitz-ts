import { describe, expect, it } from "vite-plus/test";

import { createPendingNotificationBuffer } from "../../../src/domains/internal/pending-notifications";

type FakeNotification = { value: string };
type FakeSubscription = { handlers: Map<number, (notification: FakeNotification) => void> };

describe("createPendingNotificationBuffer", () => {
  it("buffers a notification when dispatchOrQueue finds no known subscription yet", () => {
    const dispatched: FakeNotification[] = [];
    const buffer = createPendingNotificationBuffer<FakeNotification, FakeSubscription>(
      () => undefined,
      (handler, notification) => handler(notification),
    );

    buffer.dispatchOrQueue(1n, { value: "early" });
    expect(dispatched).toEqual([]);
  });

  it("dispatches immediately via dispatchOrQueue once the subscription is already known", () => {
    const dispatched: FakeNotification[] = [];
    const subscription: FakeSubscription = {
      handlers: new Map([[1, (n) => dispatched.push(n)]]),
    };
    const buffer = createPendingNotificationBuffer<FakeNotification, FakeSubscription>(
      () => subscription,
      (handler, notification) => handler(notification),
    );

    buffer.dispatchOrQueue(1n, { value: "now" });
    expect(dispatched).toEqual([{ value: "now" }]);
  });

  it("flushes buffered notifications, in order, once the subscription becomes known", () => {
    const dispatched: FakeNotification[] = [];
    let subscription: FakeSubscription | undefined;
    const buffer = createPendingNotificationBuffer<FakeNotification, FakeSubscription>(
      () => subscription,
      (handler, notification) => handler(notification),
    );

    buffer.dispatchOrQueue(1n, { value: "first" });
    buffer.dispatchOrQueue(1n, { value: "second" });
    expect(dispatched).toEqual([]);

    subscription = { handlers: new Map([[1, (n) => dispatched.push(n)]]) };
    buffer.flush(1n);

    expect(dispatched).toEqual([{ value: "first" }, { value: "second" }]);
  });

  it("does not redeliver an already-flushed notification on a second flush() call", () => {
    const dispatched: FakeNotification[] = [];
    const subscription: FakeSubscription = {
      handlers: new Map([[1, (n) => dispatched.push(n)]]),
    };
    const buffer = createPendingNotificationBuffer<FakeNotification, FakeSubscription>(
      () => subscription,
      (handler, notification) => handler(notification),
    );

    buffer.queue(1n, { value: "once" });
    buffer.flush(1n);
    buffer.flush(1n);

    expect(dispatched).toEqual([{ value: "once" }]);
  });

  it("fans out one notification to every registered handler", () => {
    const dispatchedA: FakeNotification[] = [];
    const dispatchedB: FakeNotification[] = [];
    const subscription: FakeSubscription = {
      handlers: new Map([
        [1, (n) => dispatchedA.push(n)],
        [2, (n) => dispatchedB.push(n)],
      ]),
    };
    const buffer = createPendingNotificationBuffer<FakeNotification, FakeSubscription>(
      () => subscription,
      (handler, notification) => handler(notification),
    );

    buffer.dispatchOrQueue(1n, { value: "fanout" });

    expect(dispatchedA).toEqual([{ value: "fanout" }]);
    expect(dispatchedB).toEqual([{ value: "fanout" }]);
  });

  it("drops buffered notifications for a subId once remove() is called", () => {
    const dispatched: FakeNotification[] = [];
    let subscription: FakeSubscription | undefined;
    const buffer = createPendingNotificationBuffer<FakeNotification, FakeSubscription>(
      () => subscription,
      (handler, notification) => handler(notification),
    );

    buffer.queue(1n, { value: "orphaned" });
    buffer.remove(1n);

    subscription = { handlers: new Map([[1, (n) => dispatched.push(n)]]) };
    buffer.flush(1n);

    expect(dispatched).toEqual([]);
  });
});
