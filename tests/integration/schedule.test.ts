import { describe, expect, it } from "vitest";

import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

const b = (value: string) => Buffer.from(value);

describe("Schedule integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should create schedule with a valid cron expression", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const id = await f
        .client()
        .schedule()
        .create(f.uniqueRoute("schedule"), "*/5 * * * *", b("task-payload"));

      expect(id.length).toBeGreaterThan(0);
    });

    it("should reject invalid cron syntax", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      await expect(
        f
          .client()
          .schedule()
          .create(f.uniqueRoute("schedule"), "not a cron", b("payload")),
      ).rejects.toBeTruthy();
    });

    it("should cancel an existing schedule", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("schedule");
      const id = await f
        .client()
        .schedule()
        .create(route, "0 9 * * 1", b("weekly"));
      await expect(f.client().schedule().cancel(id)).resolves.toBeUndefined();
    });

    it("should list schedules without error", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("schedule");
      await f.client().schedule().create(route, "0 9 * * 1", b("s1"));
      await f.client().schedule().create(`${route}-2`, "0 12 * * *", b("s2"));

      const [entries, totalCount] = await f.client().schedule().list(0n, 100n);
      expect(Array.isArray(entries)).toBe(true);
      expect(typeof totalCount).toBe("bigint");
    });

    it("should tolerate cancel of a nonexistent schedule", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const cancel = f
        .client()
        .schedule()
        .cancel(`${f.uniqueRoute("schedule")}-missing`);
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
        .schedule()
        .subscribe(f.uniqueRoute("schedule"), async () => undefined);

      expect(sub).toBeTruthy();
      await expect(sub.unsubscribe()).resolves.toBeUndefined();
    });
  });
});
