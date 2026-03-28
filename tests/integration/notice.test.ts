import { describe, expect, it, vi } from "vitest";

import { sleep } from "./helpers";
import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

const b = (value: string) => Buffer.from(value);

describe("Notice integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should receive notification for matching publish", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("notice");
      let resolveReceived!: (value: { route: string; body: string }) => void;
      let rejectReceived!: (reason?: unknown) => void;
      const received = new Promise<{ route: string; body: string }>((resolve, reject) => {
        resolveReceived = resolve;
        rejectReceived = reject;
      });
      const timer = setTimeout(() => {
        rejectReceived(new Error("timed out waiting for notice"));
      }, 5000);

      await f
        .client()
        .notice()
        .subscribe(route, async (msg) => {
          clearTimeout(timer);
          resolveReceived({
            route: msg.route,
            body: Buffer.from(msg.body).toString(),
          });
        });

      await f.client().notice().publish(route, b("hello"));
      await expect(received).resolves.toEqual({ route, body: "hello" });
    });

    it("should fan out to all subscribers on the same route", async () => {
      const f1 = new TestFixture(transport, authMode);
      const f2 = new TestFixture(transport, authMode);
      const publisher = new TestFixture(transport, authMode);
      await f1.connectOrFail();
      await f2.connectOrFail();
      await publisher.connectOrFail();

      const route = f1.uniqueRoute("notice");
      let count = 0;

      await f1
        .client()
        .notice()
        .subscribe(route, async () => {
          count += 1;
        });
      await f2
        .client()
        .notice()
        .subscribe(route, async () => {
          count += 1;
        });

      await publisher.client().notice().publish(route, b("fanout"));
      await sleep(500);

      expect(count).toBe(2);
    });

    it("should succeed when publishing with no subscribers", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      await expect(
        f.client().notice().publish(f.uniqueRoute("notice"), b("nobody")),
      ).resolves.toBeUndefined();
    });

    it("should publish without runtime warning noise", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
      const error = vi.spyOn(console, "error").mockImplementation(() => undefined);

      try {
        await expect(
          f.client().notice().publish(f.uniqueRoute("notice"), b("quiet")),
        ).resolves.toBeUndefined();
        await sleep(250);
        expect(warn).not.toHaveBeenCalled();
        expect(error).not.toHaveBeenCalled();
      } finally {
        warn.mockRestore();
        error.mockRestore();
      }
    });

    it("should stop receiving after unsubscribe", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("notice");
      const received: string[] = [];

      const sub = await f
        .client()
        .notice()
        .subscribe(route, async (msg) => {
          received.push(Buffer.from(msg.body).toString());
        });

      await f.client().notice().publish(route, b("before"));
      await sleep(500);
      expect(received).toEqual(["before"]);

      await sub.unsubscribe();
      await f.client().notice().publish(route, b("after"));
      await sleep(500);

      expect(received).toEqual(["before"]);
    });

    it("should match wildcard subscriptions", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const realm = f.uniqueRealm();
      const area = f.uniqueArea();
      const pattern = `notice://${realm}/${area}/*`;
      const route = `notice://${realm}/${area}/events`;

      let resolveReceived!: (value: { route: string; body: string }) => void;
      let rejectReceived!: (reason?: unknown) => void;
      const received = new Promise<{ route: string; body: string }>((resolve, reject) => {
        resolveReceived = resolve;
        rejectReceived = reject;
      });
      const timer = setTimeout(() => {
        rejectReceived(new Error("timed out waiting for wildcard notice"));
      }, 5000);

      await f
        .client()
        .notice()
        .subscribe(pattern, async (msg) => {
          clearTimeout(timer);
          resolveReceived({
            route: msg.route,
            body: Buffer.from(msg.body).toString(),
          });
        });

      await f.client().notice().publish(route, b("wildcard-test"));
      await expect(received).resolves.toEqual({
        route,
        body: "wildcard-test",
      });
    });
  });
});
