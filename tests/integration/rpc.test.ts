import { describe, expect, it } from "vite-plus/test";

import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

const b = (value: string) => Buffer.from(value);

async function collectResponses(
  iterator: AsyncIterable<{ body: Uint8Array; sequence: bigint }>,
): Promise<Array<{ body: string; sequence: bigint }>> {
  const frames: Array<{ body: string; sequence: bigint }> = [];
  for await (const frame of iterator) {
    frames.push({
      body: Buffer.from(frame.body).toString(),
      sequence: frame.sequence,
    });
  }
  return frames;
}

describe("RPC integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should route request to a registered worker", async () => {
      const worker = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      await worker.connectOrFail();
      await caller.connectOrFail();

      const route = worker.uniqueRoute("rpc");
      const sub = await worker.client().rpc.registerWorker(route, async (req, writer) => {
        await writer.end({ body: req.body });
      });

      const iterator = caller.client().rpc.call(route, {
        body: b("ping"),
        timeoutMs: 5000,
      });
      const frames = await collectResponses(iterator);

      expect(frames).toEqual([{ body: "ping", sequence: 0n }]);
      await sub.unsubscribe();
    });

    it("should preserve sequence order for streaming responses", async () => {
      const worker = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      await worker.connectOrFail();
      await caller.connectOrFail();

      const route = worker.uniqueRoute("rpc");
      const sub = await worker.client().rpc.registerWorker(route, async (_req, writer) => {
        await writer.write({ body: Uint8Array.of(0) });
        await writer.write({ body: Uint8Array.of(1) });
        await writer.write({ body: Uint8Array.of(2) });
        await writer.end();
      });

      const iterator = caller.client().rpc.call(route, {
        body: b("stream-me"),
        timeoutMs: 5000,
      });
      const frames = await collectResponses(iterator);

      expect(frames.map((frame) => Number(frame.sequence))).toEqual([0, 1, 2]);
      expect(frames.map((frame) => frame.body.charCodeAt(0))).toEqual([0, 1, 2]);
      await sub.unsubscribe();
    });

    it("should time out when no worker is registered", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const iterator = f.client().rpc.call(f.uniqueRoute("rpc"), {
        body: b("nobody-home"),
        timeoutMs: 500,
      });

      await expect(iterator.next()).rejects.toBeTruthy();
    });

    it("should load balance requests across multiple workers", async () => {
      const worker1 = new TestFixture(transport, authMode);
      const worker2 = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      await worker1.connectOrFail();
      await worker2.connectOrFail();
      await caller.connectOrFail();

      const route = worker1.uniqueRoute("rpc");
      const seen: Record<string, number> = { w1: 0, w2: 0 };

      const sub1 = await worker1.client().rpc.registerWorker(route, async (req, writer) => {
        seen.w1 += 1;
        await writer.end({ body: req.body });
      });
      const sub2 = await worker2.client().rpc.registerWorker(route, async (req, writer) => {
        seen.w2 += 1;
        await writer.end({ body: req.body });
      });

      for (let i = 0; i < 4; i += 1) {
        const iterator = caller.client().rpc.call(route, {
          body: b("req"),
          timeoutMs: 5000,
        });
        await collectResponses(iterator);
      }

      expect(seen.w1 + seen.w2).toBe(4);
      await sub1.unsubscribe();
      await sub2.unsubscribe();
    });

    it("should correlate responses with the correct request", async () => {
      const worker = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      await worker.connectOrFail();
      await caller.connectOrFail();

      const route = worker.uniqueRoute("rpc");
      const sub = await worker.client().rpc.registerWorker(route, async (req, writer) => {
        await writer.end({ body: req.body });
      });

      const [framesA, framesB] = await Promise.all([
        collectResponses(caller.client().rpc.call(route, { body: b("req-A"), timeoutMs: 5000 })),
        collectResponses(caller.client().rpc.call(route, { body: b("req-B"), timeoutMs: 5000 })),
      ]);

      expect(framesA[0]?.body).toBe("req-A");
      expect(framesB[0]?.body).toBe("req-B");
      await sub.unsubscribe();
    });

    it("should stop routing requests after worker unsubscribe", async () => {
      const worker = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      await worker.connectOrFail();
      await caller.connectOrFail();

      const route = worker.uniqueRoute("rpc");
      const sub = await worker.client().rpc.registerWorker(route, async (req, writer) => {
        await writer.end({ body: req.body });
      });

      const first = caller.client().rpc.call(route, {
        body: b("alive"),
        timeoutMs: 5000,
      });
      await collectResponses(first);

      await sub.unsubscribe();

      const dead = caller.client().rpc.call(route, {
        body: b("dead"),
        timeoutMs: 500,
      });

      await expect(dead.next()).rejects.toBeTruthy();
    });
  });
});
