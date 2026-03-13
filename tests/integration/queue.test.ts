import { describe, expect, it } from "vitest";

import { sleep } from "./helpers";
import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

const b = (value: string) => Buffer.from(value);

describe("Queue integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should complete basic enqueue reserve complete workflow", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("queue");
      const messageId = await f
        .client()
        .queue()
        .enqueue(route, b("task-payload"));
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

      const firstReserve = await f.client().queue().reserve(route, 2, 1);
      expect(firstReserve).toHaveLength(1);

      await sleep(4000);

      const secondReserve = await f.client().queue().reserve(route, 30, 1);
      expect(secondReserve.length).toBeGreaterThanOrEqual(1);
      expect(Buffer.from(secondReserve[0].body).toString()).toBe("expire-me");
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
        items[0].completeWithToken(items[0].token + 1n),
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
      const items = await f.client().queue().reserve(route, 2, 1);

      expect(items).toHaveLength(1);
      await sleep(4000);
      await expect(items[0].complete()).rejects.toBeTruthy();
    });

    it("should return empty array for an empty queue", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const items = await f
        .client()
        .queue()
        .reserve(f.uniqueRoute("queue"), 30, 1);
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
