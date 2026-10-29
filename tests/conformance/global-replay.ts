import { expect } from "vite-plus/test";

import type { Client } from "../../src/client/client.js";
import type { StreamFilterSet } from "../../src/domains/stream/types.js";

export async function exerciseGlobalFilteredReplay(
  client: Client,
  route: string,
): Promise<string[]> {
  const otherRoute = route.replace("stream://conformance-realm/", "stream://other-realm/");
  const discriminator = `match-${route}`;
  const filter: StreamFilterSet = { clauses: [{ kind: "Equals", value: discriminator }] };

  let startOffset = 0n;
  for await (const page of client.stream.read("stream://**", {
    fromOffset: 0n,
    mode: "replay",
    batchSize: 1000,
    filter,
  })) {
    startOffset = page.nextOffset;
  }

  for (const [index, concreteRoute] of [route, otherRoute].entries()) {
    const session = await client.stream.begin(concreteRoute);
    await session.append({ expectedOffset: 0n, body: Uint8Array.of(index), discriminator: "skip" });
    await session.append({ expectedOffset: 1n, body: Uint8Array.of(index), discriminator });
    await session.commit({ mode: "Sync" });
  }

  const pages = [];
  for await (const page of client.stream.read("stream://**", {
    fromOffset: startOffset,
    mode: "replay",
    batchSize: 1,
    filter,
  })) {
    pages.push(page);
  }

  expect(pages.length).toBeGreaterThan(1);
  expect(pages.some((page) => page.items.length > 0 && page.records.length === 0)).toBe(true);
  expect(pages.at(-1)?.caughtUp).toBe(true);
  for (const page of pages) {
    if (page.items.length > 0) expect(page.nextOffset).toBeGreaterThan(page.fromOffset);
  }
  const records = pages.flatMap((page) => page.records);
  expect(records.map((record) => record.route)).toEqual([route, otherRoute]);
  expect(records[0]?.globalOffset).toBeDefined();
  expect(records[1]?.globalOffset).toBeDefined();
  expect(records[1]!.globalOffset!).toBeGreaterThan(records[0]!.globalOffset!);
  return [
    `global filtered replay yielded ${pages.length} pages across two realms`,
    "filtered-only pages advanced the global cursor, and replay completed at its captured tail",
  ];
}
