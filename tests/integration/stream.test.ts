import { describe, expect, it } from "vite-plus/test";

import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

const b = (value: string) => Buffer.from(value);

describe("Stream integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should append records and commit a stream session", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const session = await f.client().stream().begin(f.uniqueRoute("stream"));
      const offset1 = await session.append(0n, b("record-1"));
      const offset2 = await session.append(offset1 + 1n, b("record-2"));
      await session.commit("Sync");

      expect(offset1).toBeGreaterThanOrEqual(0n);
      expect(offset2).toBeGreaterThanOrEqual(offset1);
    });

    it("should read records in offset order", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream().begin(route);
      await session.append(0n, Uint8Array.of(0));
      await session.append(1n, Uint8Array.of(1));
      await session.append(2n, Uint8Array.of(2));
      await session.commit("Sync");

      const records = await f.client().stream().read(route, 0n, 10);
      expect(records.length).toBeGreaterThanOrEqual(3);
      for (let i = 1; i < records.length; i += 1) {
        expect(records[i].offset).toBeGreaterThan(records[i - 1].offset);
      }
    });

    it("should reject append when expected offset is mismatched", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream().begin(route);
      await session.append(0n, b("first"));
      await session.commit("Sync");

      const wrongSession = await f.client().stream().begin(route);
      await expect(wrongSession.append(0n, b("second"))).rejects.toBeTruthy();
    });

    it("should discard uncommitted appends on rollback", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream().begin(route);
      await session.append(0n, b("ephemeral"));
      await session.rollback();

      const records = await f.client().stream().read(route, 0n, 10);
      expect(records).toEqual([]);
    });

    it("should return the last record when available", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream().begin(route);
      await session.append(0n, b("first"));
      await session.append(1n, b("last-one"));
      await session.commit("Sync");

      const record = await f.client().stream().peek(route);
      expect(record).not.toBeNull();
      if (!record) {
        throw new Error("Expected a stream record");
      }

      expect(Buffer.from(record.body).toString()).toBe("last-one");
    });

    it("should return metadata for an existing stream", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream().begin(route);
      await session.append(0n, b("data"));
      await session.commit("Sync");

      const metadata = await f.client().stream().metadata(route);
      expect(metadata.recordCount).toBeGreaterThanOrEqual(1n);
    });

    it("should reject or return empty when reading beyond watermark", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream().begin(route);
      await session.append(0n, b("only"));
      await session.commit("Sync");

      const read = f.client().stream().read(route, 999999n, 10);
      try {
        const records = await read;
        expect(records).toEqual([]);
      } catch (error) {
        expect(error).toBeTruthy();
      }
    });

    it("should deliver commit notifications to active subscriptions", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const notification = new Promise<{
        route: string;
        event?: string;
        firstResourceOffset?: bigint;
        firstAreaOffset?: bigint;
        firstRealmOffset?: bigint;
        batchSize?: number;
      }>((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error("timed out waiting for stream notification"));
        }, 5000);

        void f
          .client()
          .stream()
          .subscribe(route, async (notif) => {
            clearTimeout(timer);
            resolve({
              route: notif.route,
              event: notif.event,
              firstResourceOffset: notif.firstResourceOffset,
              firstAreaOffset: notif.firstAreaOffset,
              firstRealmOffset: notif.firstRealmOffset,
              batchSize: notif.batchSize,
            });
          });
      });

      const session = await f.client().stream().begin(route);
      await session.append(0n, b("notify"));
      await session.commit("Sync");

      await expect(notification).resolves.toMatchObject({
        route,
        event: "committed",
        firstResourceOffset: 0n,
        firstAreaOffset: 0n,
        firstRealmOffset: 0n,
        batchSize: 1,
      });
    });
  });
});
