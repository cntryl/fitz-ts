import { describe, expect, it } from "vite-plus/test";

import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";
import type { StreamFilterSet } from "../../src/domains/stream/types";

const b = (value: string) => Buffer.from(value);

async function collectBatches(
  iterator: AsyncIterableIterator<import("../../src").StreamReadBatch>,
) {
  const batches: import("../../src").StreamReadBatch[] = [];
  for await (const batch of iterator) batches.push(batch);
  return batches;
}

async function collectRecords(
  iterator: AsyncIterableIterator<import("../../src").StreamReadBatch>,
) {
  return (await collectBatches(iterator)).flatMap((batch) => batch.records);
}

describe("Stream integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should append records and commit a stream session", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const session = await f.client().stream.begin(f.uniqueRoute("stream"));
      const offset1 = await session.append({ expectedOffset: 0n, body: b("record-1") });
      const offset2 = await session.append({ expectedOffset: offset1 + 1n, body: b("record-2") });
      await session.commit({ mode: "Sync" });

      expect(offset1).toBeGreaterThanOrEqual(0n);
      expect(offset2).toBeGreaterThanOrEqual(offset1);
    });

    it("should read records in offset order", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: Uint8Array.of(0) });
      await session.append({ expectedOffset: 1n, body: Uint8Array.of(1) });
      await session.append({ expectedOffset: 2n, body: Uint8Array.of(2) });
      await session.commit({ mode: "Sync" });

      const records = await collectRecords(
        f.client().stream.read(route, { fromOffset: 0n, mode: "replay", batchSize: 10 }),
      );
      expect(records.length).toBeGreaterThanOrEqual(3);
      for (let i = 1; i < records.length; i += 1) {
        expect(records[i].offset).toBeGreaterThan(records[i - 1].offset);
      }
    });

    it("should read only matching discriminator records", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: b("alpha"), discriminator: "proj.alpha" });
      await session.append({ expectedOffset: 1n, body: b("beta"), discriminator: "audit.beta" });
      await session.commit({ mode: "Sync" });

      const filter: StreamFilterSet = {
        clauses: [{ kind: "Equals", value: "proj.alpha" }],
      };
      const records = await collectRecords(
        f.client().stream.read(route, {
          fromOffset: 0n,
          mode: "replay",
          batchSize: 10,
          filter,
        }),
      );

      expect(records).toHaveLength(1);
      expect(Buffer.from(records[0].body).toString()).toBe("alpha");
    });

    it("should expose synthetic filtered markers in read pages", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: b("alpha"), discriminator: "proj.alpha" });
      await session.append({ expectedOffset: 1n, body: b("beta"), discriminator: "audit.beta" });
      await session.commit({ mode: "Sync" });

      const filter: StreamFilterSet = {
        clauses: [{ kind: "Equals", value: "proj.alpha" }],
      };

      const result = await f
        .client()
        .stream.read(route, {
          fromOffset: 0n,
          mode: "replay",
          batchSize: 10,
          filter,
        })
        .next();
      expect(result.done).toBe(false);
      const page = result.value!;
      expect(page.items).toHaveLength(2);
      expect(page.items[0]).toMatchObject({ kind: "event" });
      expect(page.items[1]).toEqual({
        kind: "filtered",
        route,
        offset: 1n,
        reason: "server_filter",
      });
      expect(page.nextOffset).toBe(2n);
      expect(page.caughtUp).toBe(true);

      const records = await collectRecords(
        f.client().stream.read(route, {
          fromOffset: 0n,
          mode: "replay",
          batchSize: 10,
          filter,
        }),
      );
      expect(records).toHaveLength(1);
      expect(Buffer.from(records[0].body).toString()).toBe("alpha");
    });

    it("should read filtered realm-wildcard streams on the global cursor axis", async () => {
      // Arrange
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const area = f.uniqueArea();
      const resource = f.uniqueResource();
      const otherArea = f.uniqueArea();
      const routes = [
        `stream://${f.uniqueRealm()}/${area}/${resource}`,
        `stream://${f.uniqueRealm()}/${area}/${resource}`,
        `stream://${f.uniqueRealm()}/${otherArea}/${resource}`,
      ];
      for (const [index, route] of routes.entries()) {
        const session = await f.client().stream.begin(route);
        await session.append({ expectedOffset: 0n, body: b(`record-${index}`) });
        await session.commit({ mode: "Sync" });
      }

      // Act
      const result = await f
        .client()
        .stream.read(`stream://*/${area}/${resource}`, {
          fromOffset: 0n,
          mode: "replay",
          batchSize: 100,
        })
        .next();
      expect(result.done).toBe(false);
      const page = result.value!;

      // Assert
      const records = page.items.flatMap((item: import("../../src").StreamReadItem) =>
        item.kind === "event" ? [item.record] : [],
      );
      expect(
        records.map((record: import("../../src").StreamRecord) =>
          Buffer.from(record.body).toString(),
        ),
      ).toEqual(["record-0", "record-1"]);
      expect(
        records.every(
          (record: import("../../src").StreamRecord) => record.globalOffset !== undefined,
        ),
      ).toBe(true);
      expect(page.nextOffset).toBeGreaterThan(0n);
      expect(page.caughtUp).toBe(true);
    });

    it("should reject append when expected offset is mismatched", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: b("first") });
      await session.commit({ mode: "Sync" });

      const wrongSession = await f.client().stream.begin(route);
      await expect(
        wrongSession.append({ expectedOffset: 0n, body: b("second") }),
      ).rejects.toBeTruthy();
    });

    it("should discard uncommitted appends on rollback", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: b("ephemeral") });
      await session.rollback();

      const records = await collectRecords(
        f.client().stream.read(route, { fromOffset: 0n, mode: "replay", batchSize: 10 }),
      );
      expect(records).toEqual([]);
    });

    it("should return the last record when available", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: b("first") });
      await session.append({ expectedOffset: 1n, body: b("last-one") });
      await session.commit({ mode: "Sync" });

      const record = await f.client().stream.peek(route);
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
      const session = await f.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: b("data") });
      await session.commit({ mode: "Sync" });

      const metadata = await f.client().stream.metadata(route);
      expect(metadata.recordCount).toBeGreaterThanOrEqual(1n);
    });

    it("should reject or return empty when reading beyond watermark", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      const session = await f.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: b("only") });
      await session.commit({ mode: "Sync" });

      const read = f.client().stream.read(route, {
        fromOffset: 999999n,
        mode: "replay",
        batchSize: 10,
      });
      try {
        const records = await collectRecords(read);
        expect(records).toEqual([]);
      } catch (error) {
        expect(error).toBeTruthy();
      }
    });

    it("should deliver commit notifications to active subscriptions", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("stream");
      let resolveNotification!: (value: {
        route: string;
        event?: string;
        firstResourceOffset?: bigint;
        firstAreaOffset?: bigint;
        firstRealmOffset?: bigint;
        batchSize?: number;
      }) => void;
      let timer: ReturnType<typeof setTimeout>;
      let subscription: { unsubscribe(): Promise<void> } | null = null;
      const notification = new Promise<{
        route: string;
        event?: string;
        firstResourceOffset?: bigint;
        firstAreaOffset?: bigint;
        firstRealmOffset?: bigint;
        batchSize?: number;
      }>((resolve, reject) => {
        resolveNotification = resolve;
        timer = setTimeout(() => {
          reject(new Error("timed out waiting for stream notification"));
        }, 5000);
      });

      subscription = await f.client().stream.subscribe(route, async (notif) => {
        clearTimeout(timer);
        resolveNotification({
          route: notif.route,
          event: notif.event,
          firstResourceOffset: notif.firstResourceOffset,
          firstAreaOffset: notif.firstAreaOffset,
          firstRealmOffset: notif.firstRealmOffset,
          batchSize: notif.batchSize,
        });
      });

      const session = await f.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: b("notify") });
      await session.commit({ mode: "Sync" });

      await expect(notification).resolves.toMatchObject({
        route,
        event: "committed",
        firstResourceOffset: 0n,
        firstAreaOffset: 0n,
        firstRealmOffset: 0n,
        batchSize: 1,
      });
      await subscription.unsubscribe();
    });
  });
});
