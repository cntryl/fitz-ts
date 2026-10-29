import { expect } from "vite-plus/test";
import type { Client } from "../../../src/client/client.js";

export async function exerciseStreamBoundary(client: Client, prefix: string): Promise<void> {
  const route = prefix + "r".repeat(4096 - Buffer.byteLength(prefix));
  const metadata = new Uint8Array(16).fill(7);
  const body = new Uint8Array(400 * 1024 - metadata.length).fill(42);
  const oversized = new Uint8Array(body.length + 1).fill(42);
  for (let offset = 0; offset < 3; offset++) {
    const session = await client.stream.begin(route);
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
  for await (const batch of client.stream.read(route, {
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
}
