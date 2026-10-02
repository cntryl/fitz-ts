import { describe, expect, it } from "vite-plus/test";

import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";
import { waitFor } from "./helpers";

// These acceptance tests require the next server contracts, even when main is older.
// Keep legacy compatibility coverage in unit tests; never skip this release gate.
describe("server 0.2.0 contracts", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should advertise correlation, session metadata and exclusive KV scan", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      await waitFor(() => f.client().getServerCapabilities().protocolVersion !== 0);
      const hello = f.client().getServerCapabilities();
      expect(hello.protocolVersion).toBe(1);
      expect(hello.capabilities & 7, "server 0.2.0 requires capability bits 0, 1 and 2").toBe(7);
    });

    for (const reverse of [false, true]) {
      it(`should exclude the resume key in a ${reverse ? "reverse" : "forward"} KV scan`, async () => {
        const f = new TestFixture(transport, authMode);
        await f.connectOrFail();
        const route = f.uniqueRoute("kv");
        const seed = await f.client().kv.begin(route, { durability: "Sync" });
        for (const key of [
          new Uint8Array([0x10]),
          new Uint8Array([0x10, 0]),
          new Uint8Array([0x20]),
        ]) {
          await seed.put({ key, value: key });
        }
        await seed.commit();
        const tx = await f.client().kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
        try {
          const first = await tx.scan({ limit: 1, reverse });
          expect(first.entries).toHaveLength(1);
          expect(first.hasMore).toBe(true);
          const resumed = await tx.scan({
            startKey: first.entries[0]!.key,
            startExclusive: true,
            limit: 2,
            reverse,
          });
          expect(resumed.entries.map(({ key }) => Array.from(key))).toEqual(
            reverse ? [[0x10, 0], [0x10]] : [[0x10, 0], [0x20]],
          );
          expect(resumed.hasMore).toBe(false);
        } finally {
          await tx.rollback();
        }
      });
    }
  });
});
