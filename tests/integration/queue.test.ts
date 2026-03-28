import { describe, expect, it } from "vitest";

import { sleep, waitFor } from "./helpers";
import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

const b = (value: string) => Buffer.from(value);

describe("Queue integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should complete basic enqueue reserve complete workflow", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("queue");
      const messageId = await f.client().queue().enqueue(route, b("task-payload"));
      expect(messageId).toBeGreaterThan(0n);

      const items = await f.client().queue().reserve(route, 30, 1);
      expect(items).toHaveLength(1);
      expect(Buffer.from(items[0].body).toString()).toBe("task-payload");

      await expect(items[0].complete()).resolves.toBeUndefined();
    });

    it("should return message to queue after lease expiry", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("queue");
      await f.client().queue().enqueue(route, b("expire-me"));

      const firstReserve = await f.client().queue().reserve(route, 1, 1);
      expect(firstReserve).toHaveLength(1);

      await waitFor(
        async () => {
          const secondReserve = await f.client().queue().reserve(route, 30, 1);
          if (secondReserve.length === 0) {
            return false;
          }
          return Buffer.from(secondReserve[0].body).toString() === "expire-me";
        },
        {
          timeoutMs: 3000,
          intervalMs: 100,
          timeoutMessage: "message did not return to queue after lease expiry",
        },
      );
    });

    it("should extend lease with a valid token", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("queue");
      await f.client().queue().enqueue(route, b("extend-me"));
      const items = await f.client().queue().reserve(route, 5, 1);

      expect(items).toHaveLength(1);
      await expect(items[0].extend(60)).resolves.toBeUndefined();
    });

    it("should reject complete with invalid token", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("queue");
      await f.client().queue().enqueue(route, b("token-check"));
      const items = await f.client().queue().reserve(route, 30, 1);

      expect(items).toHaveLength(1);
      await expect(
        items[0].testOnlyCompleteWithToken(items[0].testOnlyInvalidToken()),
      ).rejects.toBeTruthy();
    });

    it("should reserve up to the requested batch size", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("queue");
      for (let i = 0; i < 5; i += 1) {
        await f
          .client()
          .queue()
          .enqueue(route, b(`batch-${i}`));
      }

      const items = await f.client().queue().reserve(route, 30, 3);
      expect(items.length).toBeGreaterThanOrEqual(1);
      expect(items.length).toBeLessThanOrEqual(3);
    });

    it("should distribute reservations across consumers", async () => {
      const f1 = new TestFixture(transport, authMode);
      const f2 = new TestFixture(transport, authMode);
      await f1.connectOrFail();
      await f2.connectOrFail();

      const route = f1.uniqueRoute("queue");
      await f1.client().queue().enqueue(route, b("one"));
      await f1.client().queue().enqueue(route, b("two"));

      const [items1, items2] = await Promise.all([
        f1.client().queue().reserve(route, 30, 1),
        f2.client().queue().reserve(route, 30, 1),
      ]);

      expect(items1.length + items2.length).toBe(2);
    });

    it("should long poll when waitSeconds is provided", async () => {
      const consumer = new TestFixture(transport, authMode);
      const producer = new TestFixture(transport, authMode);
      await consumer.connectOrFail();
      await producer.connectOrFail();

      const route = consumer.uniqueRoute("queue");
      const pendingReserve = consumer.client().queue().reserve(route, 30, 1, 2);

      await sleep(250);
      await producer.client().queue().enqueue(route, b("late-msg"));

      const items = await pendingReserve;
      expect(items).toHaveLength(1);
      expect(Buffer.from(items[0].body).toString()).toBe("late-msg");
    });

    it("should handle reserve with zero limit", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("queue");
      const reserve = f.client().queue().reserve(route, 30, 0);

      try {
        const items = await reserve;
        expect(items).toEqual([]);
      } catch (error) {
        expect(error).toBeTruthy();
      }
    });

    it("should reject complete after lease expiry", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("queue");
      await f.client().queue().enqueue(route, b("expire-then-complete"));
      const staleLeaseItems = await f.client().queue().reserve(route, 1, 1);

      expect(staleLeaseItems).toHaveLength(1);
      let refreshedLeaseItem: (typeof staleLeaseItems)[number] | null = null;
      await waitFor(
        async () => {
          const items = await f.client().queue().reserve(route, 30, 1);
          if (items.length === 0) {
            return false;
          }

          refreshedLeaseItem = items[0];
          return Buffer.from(items[0].body).toString() === "expire-then-complete";
        },
        {
          timeoutMs: 3000,
          intervalMs: 100,
          timeoutMessage: "message was not re-reserved after lease expiry",
        },
      );

      await expect(staleLeaseItems[0].complete()).rejects.toBeTruthy();
      if (refreshedLeaseItem) {
        await refreshedLeaseItem.complete().catch(() => undefined);
      }
    });

    it("should return empty array for an empty queue", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const items = await f.client().queue().reserve(f.uniqueRoute("queue"), 30, 1);
      expect(items).toEqual([]);
    });

    it("should notify subscribers when a queue becomes available", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("queue");
      const notifications: string[] = [];
      const sub = await f
        .client()
        .queue()
        .subscribe(route, async (notif) => {
          notifications.push(notif.route);
        });

      await f.client().queue().enqueue(route, b("notify-me"));
      await sleep(500);
      expect(notifications).toContain(route);

      await sub.unsubscribe();
      await f.client().queue().enqueue(route, b("no-notify"));
      await sleep(500);

      expect(notifications).toHaveLength(1);
    });
  });
});
