import { describe, expect, it, vi } from "vite-plus/test";

import { createClient } from "../../src/client/client";
import type { FitzLifecycleEvent } from "../../src/core/types";
import { generateValidTestJwt } from "./fixture/jwt";
import {
  EnvBrokerJWTAudience,
  EnvBrokerJWTHMACSecret,
  EnvBrokerJWTTenant,
  TestFixture,
  brokerAddrFor,
} from "./fixture/fixture";
import { runWithBothTransports, runWithTransportsOnly } from "./fixture/transport";
import { createTransportProxy } from "./fixture/transport-proxy";
import { sleep } from "./helpers";

const b = (value: string) => Buffer.from(value);

function testSecret(): string {
  return process.env[EnvBrokerJWTHMACSecret] ?? "dev-test-secret";
}

function testAudience(): string {
  return process.env[EnvBrokerJWTAudience] ?? "fitz";
}

function testTenant(): string {
  return process.env[EnvBrokerJWTTenant] ?? "dev";
}

function uniqueRoute(scheme: string): string {
  const suffix = `${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  return `${scheme}://test-${suffix}/area-${suffix}/resource-${suffix}`;
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

      await subscriber.client().notice.subscribe(route, async (msg) => {
        received.push(Buffer.from(msg.body).toString());
      });

      await publisher.client().notice.publish(route, { body: b("before-reconnect") });
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

      await subscriber.client().notice.subscribe(route, async (msg) => {
        received.push(`reconnected:${Buffer.from(msg.body).toString()}`);
      });

      await publisher.client().notice.publish(route, { body: b("after-reconnect") });
      await sleep(500);

      expect(received).toContain("reconnected:after-reconnect");
      expect(events.some((event) => event.event === "connect_succeeded")).toBe(true);
      expect(events.some((event) => event.event === "closed")).toBe(true);
    });

    it("should call tokenProvider again when reconnecting to the auth broker", async () => {
      if (authMode !== "valid_jwt") {
        return;
      }

      const worker = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      const tokenProvider = vi.fn(async () =>
        generateValidTestJwt(testSecret(), testAudience(), testTenant()),
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
        .rpc.registerWorker(route, async (_request, writer) => {
          await writer.end({ body: b("ok") });
        });

      const iterator = caller.client().rpc.call(route, { body: b("before"), timeoutMs: 5000 });
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

      const nextIterator = caller.client().rpc.call(route, {
        body: b("after"),
        timeoutMs: 5000,
      });
      const secondFrame = await nextIterator.next();
      expect(secondFrame.done).toBe(false);
      expect(Buffer.from(secondFrame.value.body).toString()).toBe("ok");
      await expect(nextIterator.next()).resolves.toMatchObject({ done: true });

      expect(tokenProvider).toHaveBeenCalledTimes(2);

      await workerSub.unsubscribe();
    });
  });
});

describe("Client ownership integration", () => {
  runWithTransportsOnly(({ transport }) => {
    it("should wait for Fitz to become reachable during initial connect", async () => {
      const proxy = await createTransportProxy(transport, brokerAddrFor(transport, "anonymous"));
      await proxy.goDown();
      const client = createClient({
        url: proxy.url,
        transport,
        tokenProvider: () => "",
        timeout: 500,
      });

      try {
        let settled = false;
        const ready = client
          .connectWhenReady({
            timeoutMs: 5000,
            backoffMs: 25,
            maxBackoffMs: 50,
          })
          .finally(() => {
            settled = true;
          });

        await sleep(100);
        expect(settled).toBe(false);

        await proxy.goUp();
        await ready;

        expect(client.isConnected()).toBe(true);
      } finally {
        await client.close().catch(() => undefined);
        await proxy.close();
      }
    });

    it("should wait on the active reconnect when connect is called mid-recovery", async () => {
      const publisher = new TestFixture(transport, "anonymous");
      await publisher.connectOrFail();
      const proxy = await createTransportProxy(transport, brokerAddrFor(transport, "anonymous"));
      const events: FitzLifecycleEvent[] = [];
      const subscriber = createClient({
        url: proxy.url,
        transport,
        tokenProvider: () => "",
        reconnect: {
          enabled: true,
          maxAttempts: 10,
          backoffMs: 50,
          maxBackoffMs: 100,
        },
        observability: {
          onLifecycleEvent: (event) => {
            events.push(event);
          },
        },
      });

      try {
        await subscriber.connect();

        const noticeClient = subscriber.notice;
        const leaseClient = subscriber.lease;
        const route = uniqueRoute("notice");
        const received: string[] = [];

        await noticeClient.subscribe(route, async (msg) => {
          received.push(Buffer.from(msg.body).toString());
        });

        await publisher.client().notice.publish(route, { body: b("before") });
        await sleep(400);
        expect(received).toEqual(["before"]);
        await proxy.goDown();

        await vi.waitFor(() => {
          expect(
            events.some(
              (event) => event.event === "connection_lost" || event.event === "reconnect_start",
            ),
          ).toBe(true);
        });

        const reconnectWait = subscriber.connect();
        const secondReconnectWait = subscriber.connect();
        let firstSettled = false;
        let secondSettled = false;
        void reconnectWait.finally(() => {
          firstSettled = true;
        });
        void secondReconnectWait.finally(() => {
          secondSettled = true;
        });

        await sleep(25);

        expect(firstSettled).toBe(false);
        expect(secondSettled).toBe(false);

        await proxy.goUp();
        await Promise.all([reconnectWait, secondReconnectWait]);

        expect(subscriber.notice).toBe(noticeClient);
        expect(subscriber.lease).toBe(leaseClient);

        await publisher.client().notice.publish(route, { body: b("after") });
        await vi.waitFor(() => {
          expect(received.filter((value) => value === "after")).toHaveLength(1);
        });

        await expect(leaseClient.query(uniqueRoute("lease"))).resolves.toMatchObject({
          isHeld: false,
        });

        expect(events.some((event) => event.event === "reconnect_succeeded")).toBe(true);

        await subscriber.close();
      } finally {
        await proxy.close();
      }
    });
  });
});
