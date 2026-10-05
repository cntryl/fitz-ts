import { describe, expect, it } from "vite-plus/test";

import type { RpcHandlerContext } from "../../src/domains/rpc/types";
import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

function gate() {
  let resolve!: () => void;
  return {
    promise: new Promise<void>((done) => {
      resolve = done;
    }),
    resolve: () => resolve(),
  };
}

async function aborted(context: RpcHandlerContext): Promise<void> {
  if (!context.signal.aborted) {
    await new Promise<void>((resolve) =>
      context.signal.addEventListener("abort", () => resolve(), { once: true }),
    );
  }
}

async function within(promise: Promise<void>): Promise<void> {
  const signal = AbortSignal.timeout(3000);
  await Promise.race([
    promise,
    new Promise<never>((_resolve, reject) =>
      signal.addEventListener("abort", () => reject(new Error("chain did not settle")), {
        once: true,
      }),
    ),
  ]);
}

describe("RPC cooperative cancellation", () => {
  runWithBothTransports(({ transport, authMode }) => {
    for (const reason of ["cancel", "deadline"] as const) {
      it(`should propagate ${reason} through SDK call chains and retain unrelated calls`, async () => {
        const a = new TestFixture(transport, authMode);
        const b = new TestFixture(transport, authMode);
        const c = new TestFixture(transport, authMode);
        await Promise.all([a.connectOrFail(), b.connectOrFail(), c.connectOrFail()]);
        const middle = a.uniqueRoute("rpc");
        const leaf = c.uniqueRoute("rpc");
        const started = gate();
        const middleCleaned = gate();
        const leafCleaned = gate();
        const unrelatedStarted = gate();
        const unrelatedRelease = gate();
        let unrelatedContext: RpcHandlerContext | undefined;
        let parentBudget: number | undefined;
        let leafBudget: number | undefined;
        const leafRegistration = await c.client().rpc.registerWorker(
          leaf,
          async (req, writer, context) => {
            const body = new TextDecoder().decode(req.body);
            if (body === "unrelated") {
              unrelatedContext = context;
              unrelatedStarted.resolve();
              await unrelatedRelease.promise;
              await writer.end({ body: req.body });
            } else if (body === "probe") {
              await writer.end({ body: req.body });
            } else {
              leafBudget = context.remainingTimeMs();
              started.resolve();
              await aborted(context);
              leafCleaned.resolve();
            }
          },
          { maxConcurrency: 2 },
        );
        const middleRegistration = await b
          .client()
          .rpc.registerWorker(middle, async (req, writer, context) => {
            parentBudget = context.remainingTimeMs();
            const child = b.client().rpc.call(leaf, {
              body: req.body,
              timeoutMs: context.remainingTimeMs(),
              signal: context.signal,
            });
            try {
              for await (const frame of child) await writer.write({ body: frame.body });
              await writer.end();
            } finally {
              await child.return?.();
              middleCleaned.resolve();
            }
          });
        const unrelated = a
          .client()
          .rpc.call(leaf, { body: new TextEncoder().encode("unrelated"), timeoutMs: 10000 });
        try {
          await within(unrelatedStarted.promise);
          const call = a.client().rpc.call(middle, {
            body: new Uint8Array(),
            timeoutMs: reason === "deadline" ? 750 : 10000,
          });
          await within(started.promise);
          expect(parentBudget).toBeGreaterThan(0);
          expect(leafBudget).toBeGreaterThan(0);
          expect(leafBudget!).toBeLessThanOrEqual(parentBudget!);
          if (reason === "cancel") {
            await call.return?.();
            await expect(call.cancellation).resolves.toBe("forwarded");
          } else {
            await expect(call.next()).rejects.toBeTruthy();
          }
          await within(
            Promise.all([middleCleaned.promise, leafCleaned.promise]).then(() => undefined),
          );
          expect(unrelatedContext?.signal.aborted).toBe(false);
          unrelatedRelease.resolve();
          expect(new TextDecoder().decode((await unrelated.next()).value?.body)).toBe("unrelated");
          const probe = a
            .client()
            .rpc.call(middle, { body: new TextEncoder().encode("probe"), timeoutMs: 2000 });
          expect(new TextDecoder().decode((await probe.next()).value?.body)).toBe("probe");
          await probe.return?.();
        } finally {
          unrelatedRelease.resolve();
          await unrelated.return?.();
          await middleRegistration.unsubscribe();
          await leafRegistration.unsubscribe();
        }
      });
    }
  });
});
