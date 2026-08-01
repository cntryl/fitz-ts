import { describe, expect, it } from "vite-plus/test";

import { sleep } from "./helpers";
import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports, runWithTransportsOnly } from "./fixture/transport";

const b = (value: string) => Buffer.from(value);

describe("Schedule integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should create schedule with a valid cron expression", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const id = await f
        .client()
        .schedule.create(f.uniqueRoute("schedule"), "*/5 * * * *", "broadcast", b("task-payload"));

      expect(id.length).toBeGreaterThan(0);
    });

    it("should reject invalid cron syntax", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      await expect(
        f
          .client()
          .schedule.create(f.uniqueRoute("schedule"), "not a cron", "broadcast", b("payload")),
      ).rejects.toBeTruthy();
    });

    it("should cancel an existing schedule", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("schedule");
      await f.client().schedule.create(route, "0 9 * * 1", "broadcast", b("weekly"));
      await expect(f.client().schedule.cancel(route)).resolves.toBeUndefined();
    });

    it("should list schedules without error", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("schedule");
      const secondRoute = route.replace(/\/run$/, "/send");
      await f.client().schedule.create(route, "0 9 * * 1", "broadcast", b("s1"));
      await f.client().schedule.create(secondRoute, "0 12 * * *", "broadcast", b("s2"));

      const [entries, totalCount] = await f.client().schedule.list(0n, 100n);
      expect(Array.isArray(entries)).toBe(true);
      expect(typeof totalCount).toBe("bigint");
    });

    it("should tolerate cancel of a nonexistent schedule", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const cancel = f.client().schedule.cancel(`${f.uniqueRoute("schedule")}-missing`);
      try {
        await cancel;
      } catch (error) {
        expect(error).toBeTruthy();
      }
    });

    it("should subscribe and unsubscribe without error", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const sub = await f
        .client()
        .schedule.subscribe(f.uniqueRoute("schedule"), async () => undefined);

      expect(sub).toBeTruthy();
      await expect(sub.unsubscribe()).resolves.toBeUndefined();
    });
  });

  it("should fire every minute and deliver notifications given a wildcard cron when schedule boundaries occur", async () => {
    const f = new TestFixture("tcp", "anonymous");
    await f.connectOrFail();
    const route = f.uniqueRoute("schedule");
    const receivedAt: number[] = [];
    let resolveSecond!: () => void;
    let rejectSecond!: (reason?: unknown) => void;
    const secondNotification = new Promise<void>((resolve, reject) => {
      resolveSecond = resolve;
      rejectSecond = reject;
    });
    const timeout = setTimeout(() => {
      rejectSecond(new Error("timed out waiting for two schedule notifications"));
    }, 130_000);
    const subscription = await f.client().schedule.subscribe(route, async () => {
      receivedAt.push(Date.now());
      if (receivedAt.length === 2) {
        clearTimeout(timeout);
        resolveSecond();
      }
    });

    try {
      await f.client().schedule.create(route, "* * * * *", "broadcast", b("every-minute"));
      await secondNotification;

      expect(receivedAt[1] - receivedAt[0]).toBeGreaterThanOrEqual(50_000);
      expect(receivedAt[1] - receivedAt[0]).toBeLessThanOrEqual(70_000);
    } finally {
      clearTimeout(timeout);
      await f
        .client()
        .schedule.cancel(route)
        .catch(() => undefined);
      await subscription.unsubscribe().catch(() => undefined);
    }
  }, 140_000);

  it("should honor ranges and lists given weekday and hour constraints when time advances", async () => {
    const f = new TestFixture("tcp", "anonymous");
    await f.connectOrFail();
    const route = f.uniqueRoute("schedule");
    const now = new Date();
    const hour = now.getUTCHours();
    const weekday = now.getUTCDay();
    const cron = `* ${hour}-${hour} * * ${weekday},${(weekday + 1) % 7}`;
    let resolveNotification!: () => void;
    let rejectNotification!: (reason?: unknown) => void;
    const notification = new Promise<void>((resolve, reject) => {
      resolveNotification = resolve;
      rejectNotification = reject;
    });
    const timeout = setTimeout(() => {
      rejectNotification(new Error(`timed out waiting for constrained cron ${cron}`));
    }, 70_000);
    const subscription = await f.client().schedule.subscribe(route, async () => {
      clearTimeout(timeout);
      resolveNotification();
    });

    try {
      await f.client().schedule.create(route, cron, "broadcast", b("constrained"));
      await notification;

      expect(new Date().getUTCHours()).toBe(hour);
    } finally {
      clearTimeout(timeout);
      await f
        .client()
        .schedule.cancel(route)
        .catch(() => undefined);
      await subscription.unsubscribe().catch(() => undefined);
    }
  }, 80_000);

  it("should not deliver a fire notification before the next boundary given a wildcard cron", async () => {
    const f = new TestFixture("tcp", "anonymous");
    await f.connectOrFail();
    const route = f.uniqueRoute("schedule");
    let notifications = 0;
    const subscription = await f.client().schedule.subscribe(route, async () => {
      notifications += 1;
    });

    try {
      await f.client().schedule.create(route, "* * * * *", "broadcast", b("not-yet"));
      await sleep(2_000);

      expect(notifications).toBe(0);
    } finally {
      await f
        .client()
        .schedule.cancel(route)
        .catch(() => undefined);
      await subscription.unsubscribe().catch(() => undefined);
    }
  });

  runWithTransportsOnly(({ transport }) => {
    it("should reject unauthorized create given read-only permissions when create is called", async () => {
      const f = new TestFixture(transport);
      f.setPermissions(["schedule://**#read"]);
      await f.connectOrFail();

      await expect(
        f
          .client()
          .schedule.create(f.uniqueRoute("schedule"), "* * * * *", "broadcast", b("forbidden")),
      ).rejects.toBeTruthy();
    });
  });
});
