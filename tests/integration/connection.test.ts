import { describe, expect, it, vi } from "vitest";

import type { FitzLifecycleEvent } from "../../src/core/types";
import { generateValidTestJwt } from "./fixture/jwt";
import {
  EnvBrokerJWTAudience,
  EnvBrokerJWTHMACSecret,
  TestFixture,
} from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";
import { sleep } from "./helpers";

const b = (value: string) => Buffer.from(value);

function testSecret(): string {
  return process.env[EnvBrokerJWTHMACSecret] ?? "test-secret-key";
}

function testAudience(): string {
  return process.env[EnvBrokerJWTAudience] ?? "fitz";
}

describe("Connection hardening integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should automatically reconnect and replay notice subscriptions", async () => {
      const subscriber = new TestFixture(transport, authMode);
      const publisher = new TestFixture(transport, authMode);
      const events: FitzLifecycleEvent[] = [];

      await subscriber.connectOrFail({
        reconnect: {
          enabled: true,
          maxAttempts: 3,
          backoffMs: 50,
          maxBackoffMs: 100,
        },
        observability: {
          onLifecycleEvent: (event) => {
            events.push(event);
          },
        },
      });
      await publisher.connectOrFail();

      const route = subscriber.uniqueRoute("notice");
      const received: string[] = [];

      await subscriber
        .client()
        .notice()
        .subscribe(route, async (msg) => {
          received.push(Buffer.from(msg.body).toString());
        });

      await publisher.client().notice().publish(route, b("before-reconnect"));
      await sleep(400);
      expect(received).toContain("before-reconnect");

      const closePromise = subscriber.client().close();
      await sleep(50);
      await closePromise;

      await subscriber.connectOrFail({
        reconnect: {
          enabled: true,
          maxAttempts: 3,
          backoffMs: 50,
          maxBackoffMs: 100,
        },
        observability: {
          onLifecycleEvent: (event) => {
            events.push(event);
          },
        },
      });

      await subscriber
        .client()
        .notice()
        .subscribe(route, async (msg) => {
          received.push(`reconnected:${Buffer.from(msg.body).toString()}`);
        });

      await publisher.client().notice().publish(route, b("after-reconnect"));
      await sleep(500);

      expect(received).toContain("reconnected:after-reconnect");
      expect(events.some((event) => event.event === "connect_succeeded")).toBe(
        true,
      );
      expect(events.some((event) => event.event === "closed")).toBe(true);
    });

    it("should call tokenProvider again when reconnecting to the auth broker", async () => {
      if (authMode !== "valid_jwt") {
        return;
      }

      const worker = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      const tokenProvider = vi.fn(async () =>
        generateValidTestJwt(testSecret(), testAudience()),
      );

      caller.setTokenProvider(tokenProvider);

      await worker.connectOrFail();
      await caller.connectOrFail({
        reconnect: {
          enabled: true,
          maxAttempts: 2,
          backoffMs: 50,
          maxBackoffMs: 100,
        },
      });

      const route = worker.uniqueRoute("rpc");
      const workerSub = await worker
        .client()
        .rpc()
        .registerWorker(route, async (_request, writer) => {
          await writer.send(b("ok"), true);
        });

      const iterator = await caller
        .client()
        .rpc()
        .call(route, b("before"), { timeoutMs: 5000 });
      const firstFrame = await iterator.next();
      expect(firstFrame.done).toBe(false);
      expect(Buffer.from(firstFrame.value.body).toString()).toBe("ok");
      await expect(iterator.next()).resolves.toMatchObject({ done: true });

      await caller.client().close();

      await caller.connectOrFail({
        reconnect: {
          enabled: true,
          maxAttempts: 2,
          backoffMs: 50,
          maxBackoffMs: 100,
        },
      });

      const nextIterator = await caller
        .client()
        .rpc()
        .call(route, b("after"), { timeoutMs: 5000 });
      const secondFrame = await nextIterator.next();
      expect(secondFrame.done).toBe(false);
      expect(Buffer.from(secondFrame.value.body).toString()).toBe("ok");
      await expect(nextIterator.next()).resolves.toMatchObject({ done: true });

      expect(tokenProvider).toHaveBeenCalledTimes(2);

      await workerSub.unsubscribe();
    });
  });
});
