import { describe, expect, it } from "vite-plus/test";

import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";
import { exerciseStreamBoundary } from "./fixture/stream-boundary";

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
      await exerciseStreamBoundary(fixture.client(), fixture.uniqueRoute("stream"));
    });
  });
});
