import { describe, expect, it } from "vite-plus/test";

import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

describe("Stream boundaries", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("replays a record on a 4096-byte route", async () => {
      const fixture = new TestFixture(transport, authMode);
      await fixture.connectOrFail();
      const prefix = fixture.uniqueRoute("stream");
      const route = prefix + "r".repeat(4096 - Buffer.byteLength(prefix));
      const session = await fixture.client().stream.begin(route);
      await session.append({ expectedOffset: 0n, body: Buffer.from("boundary") });
      await session.commit({ mode: "Sync" });

      const page = await fixture
        .client()
        .stream.read(route, {
          fromOffset: 0n,
          mode: "replay",
        })
        .next();

      expect(page.done).toBe(false);
      expect(page.value?.records).toHaveLength(1);
      expect(page.value?.records[0]?.route).toBe(route);
      expect(Buffer.from(page.value?.records[0]?.body ?? [])).toEqual(Buffer.from("boundary"));
    });

    it("preserves per-event metadata through append and replay", async () => {
      const fixture = new TestFixture(transport, authMode);
      await fixture.connectOrFail();
      const route = fixture.uniqueRoute("stream");
      const session = await fixture.client().stream.begin(route);
      const metadata = Uint8Array.from(Buffer.from("event metadata"));
      await session.append({ expectedOffset: 0n, body: Buffer.from("event"), metadata });
      await session.append({
        expectedOffset: 1n,
        body: Buffer.from("empty"),
        metadata: new Uint8Array(),
      });
      await session.append({ expectedOffset: 2n, body: Buffer.from("absent") });
      await session.commit({ mode: "Sync" });

      const page = await fixture
        .client()
        .stream.read(route, {
          fromOffset: 0n,
          mode: "replay",
        })
        .next();

      expect(page.done).toBe(false);
      expect(page.value?.records).toHaveLength(3);
      expect(page.value?.records[0]?.metadata).toEqual(metadata);
      expect(page.value?.records[1]?.metadata).toEqual(new Uint8Array());
      expect(page.value?.records[2]?.metadata).toBeUndefined();
    });

    it("enforces the combined 400 KiB event limit and pages long-route replay", async () => {
      const fixture = new TestFixture(transport, authMode);
      await fixture.connectOrFail();
      const prefix = fixture.uniqueRoute("stream");
      const route = prefix + "r".repeat(4096 - Buffer.byteLength(prefix));
      const metadata = new Uint8Array(16).fill(7);
      const body = new Uint8Array(400 * 1024 - metadata.length).fill(42);
      const oversized = new Uint8Array(body.length + 1).fill(42);

      for (let offset = 0; offset < 3; offset++) {
        const session = await fixture.client().stream.begin(route);
        if (offset === 0) {
          await expect(
            session.append({ expectedOffset: 0n, body: oversized, metadata }),
          ).rejects.toMatchObject({ domainCode: 2008, effect: 0 });
        }
        body[0] = offset;
        expect(await session.append({ expectedOffset: BigInt(offset), body, metadata })).toBe(
          BigInt(offset),
        );
        await session.commit({ mode: "Sync" });
      }

      const batches = [];
      for await (const batch of fixture.client().stream.read(route, {
        fromOffset: 0n,
        mode: "replay",
        batchSize: 10,
      })) {
        batches.push(batch);
      }
      const records = batches.flatMap((batch) => batch.records);
      expect(batches.length).toBeGreaterThan(1);
      expect(records).toHaveLength(3);
      for (const [offset, record] of records.entries()) {
        expect(record.route).toBe(route);
        expect(record.offset).toBe(BigInt(offset));
        expect(record.body).toHaveLength(body.length);
        expect(record.body[0]).toBe(offset);
        expect(record.body.subarray(1)).toEqual(body.subarray(1));
        expect(record.metadata).toEqual(metadata);
      }
    });
  });
});
