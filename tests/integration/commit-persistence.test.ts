import { describe, expect, it } from "vite-plus/test";
import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

describe("commit-time persistence", () => {
  runWithBothTransports(({ transport, authMode }) => {
    for (const mode of ["Buffered", "Sync"] as const) {
      it(`commits KV and Stream with ${mode}`, async () => {
        const fixture = new TestFixture(transport, authMode);
        await fixture.connectOrFail();
        const route = fixture.uniqueRoute("kv");
        const tx = await fixture.client().kv.begin(route);
        await tx.put({ key: Buffer.from("key"), value: Buffer.from("value") });
        await tx.commit({ durability: mode });
        const read = await fixture.client().kv.begin(route, { mode: "ReadOnly" });
        expect(await read.get({ key: Buffer.from("key") })).toMatchObject({ type: "found" });
        await read.rollback();
        const session = await fixture.client().stream.begin(fixture.uniqueRoute("stream"));
        expect(await session.append({ expectedOffset: 0n, body: Buffer.from("record") })).toBe(0n);
        await session.commit({ mode });
      });
    }
  });
});
