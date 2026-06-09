import type { AddressInfo } from "node:net";
import * as net from "node:net";

import { describe, expect, it, vi } from "vite-plus/test";
import { WebSocket, WebSocketServer } from "ws";

import { Client } from "../../src/client/client";
import type { FitzLifecycleEvent } from "../../src/core/types";
import { generateValidTestJwt } from "./fixture/jwt";
import {
  EnvBrokerJWTAudience,
  EnvBrokerJWTHMACSecret,
  TestFixture,
  brokerAddrFor,
} from "./fixture/fixture";
import { runWithBothTransports, runWithTransportsOnly } from "./fixture/transport";
import { sleep } from "./helpers";

const b = (value: string) => Buffer.from(value);

function testSecret(): string {
  return process.env[EnvBrokerJWTHMACSecret] ?? "test-secret-key";
}

function testAudience(): string {
  return process.env[EnvBrokerJWTAudience] ?? "fitz";
}

function uniqueRoute(scheme: string): string {
  const suffix = `${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  return `${scheme}://test-${suffix}/area-${suffix}/resource-${suffix}`;
}

type ProxyController = {
  close: () => Promise<void>;
  goDown: () => Promise<void>;
  goUp: () => Promise<void>;
  url: string;
};

async function createTransportProxy(
  transport: "tcp" | "ws",
  targetUrl: string,
): Promise<ProxyController> {
  if (transport === "tcp") {
    return await createTcpProxy(targetUrl);
  }

  return await createWsProxy(targetUrl);
}

async function createTcpProxy(targetUrl: string): Promise<ProxyController> {
  const target = new URL(targetUrl.startsWith("tcp://") ? targetUrl : `tcp://${targetUrl}`);
  const targetHost = target.hostname || "127.0.0.1";
  const targetPort = Number(target.port || "4090");
  const links = new Set<{ client: net.Socket; upstream: net.Socket }>();
  let listenPort = 0;
  let server: net.Server | null = null;

  const destroyLinks = (): void => {
    for (const link of Array.from(links)) {
      links.delete(link);
      link.client.destroy();
      link.upstream.destroy();
    }
  };

  const createServer = () => {
    const nextServer = net.createServer((client) => {
      const upstream = net.createConnection({
        host: targetHost,
        port: targetPort,
      });
      const link = { client, upstream };
      links.add(link);

      const cleanup = () => {
        links.delete(link);
        client.destroy();
        upstream.destroy();
      };

      client.on("error", () => undefined);
      upstream.on("error", () => undefined);
      client.on("close", cleanup);
      upstream.on("close", cleanup);

      client.pipe(upstream);
      upstream.pipe(client);
    });

    return nextServer;
  };

  const listen = async (port?: number): Promise<void> => {
    const nextServer = createServer();
    await new Promise<void>((resolve, reject) => {
      nextServer.once("error", reject);
      nextServer.listen(port ?? 0, "127.0.0.1", () => {
        nextServer.off("error", reject);
        listenPort = (nextServer.address() as AddressInfo).port;
        server = nextServer;
        resolve();
      });
    });
  };

  const closeServer = async (): Promise<void> => {
    if (!server) {
      return;
    }

    const activeServer = server;
    server = null;
    await new Promise<void>((resolve, reject) => {
      activeServer.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve();
      });
    });
  };

  await listen();

  return {
    url: `127.0.0.1:${listenPort}`,
    goDown: async () => {
      destroyLinks();
      await closeServer();
    },
    goUp: async () => {
      if (server) {
        return;
      }

      await listen(listenPort);
    },
    close: async () => {
      destroyLinks();
      await closeServer();
    },
  };
}

async function createWsProxy(targetUrl: string): Promise<ProxyController> {
  const links = new Set<{ client: WebSocket; upstream: WebSocket }>();
  let listenPort = 0;
  let server: WebSocketServer | null = null;

  const destroyLinks = (): void => {
    for (const link of Array.from(links)) {
      links.delete(link);
      link.client.terminate();
      link.upstream.terminate();
    }
  };

  const createServer = (port?: number) => {
    const nextServer = new WebSocketServer({
      host: "127.0.0.1",
      port: port ?? 0,
    });

    nextServer.on("connection", (client) => {
      const upstream = new WebSocket(targetUrl);
      const buffered: Array<{ binary: boolean; data: Buffer }> = [];
      const link = { client, upstream };
      links.add(link);
      let cleanedUp = false;

      const cleanup = () => {
        if (cleanedUp) {
          return;
        }

        cleanedUp = true;
        links.delete(link);
        client.terminate();
        upstream.terminate();
      };

      client.on("message", (data, isBinary) => {
        const normalized = Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
        if (upstream.readyState === WebSocket.OPEN) {
          upstream.send(normalized, { binary: isBinary });
          return;
        }

        buffered.push({ binary: isBinary, data: normalized });
      });

      upstream.on("open", () => {
        for (const message of buffered.splice(0)) {
          upstream.send(message.data, { binary: message.binary });
        }
      });

      upstream.on("message", (data, isBinary) => {
        const normalized = Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
        if (client.readyState === WebSocket.OPEN) {
          client.send(normalized, { binary: isBinary });
        }
      });

      client.on("close", cleanup);
      upstream.on("close", cleanup);
      client.on("error", () => undefined);
      upstream.on("error", cleanup);
    });

    return nextServer;
  };

  const listen = async (port?: number): Promise<void> => {
    const nextServer = createServer(port);
    await new Promise<void>((resolve, reject) => {
      nextServer.once("error", reject);
      nextServer.once("listening", () => {
        nextServer.off("error", reject);
        listenPort = (nextServer.address() as AddressInfo).port;
        server = nextServer;
        resolve();
      });
    });
  };

  const closeServer = async (): Promise<void> => {
    if (!server) {
      return;
    }

    const activeServer = server;
    server = null;
    await new Promise<void>((resolve, reject) => {
      activeServer.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve();
      });
    });
  };

  await listen();

  return {
    url: `ws://127.0.0.1:${listenPort}`,
    goDown: async () => {
      destroyLinks();
      await closeServer();
    },
    goUp: async () => {
      if (server) {
        return;
      }

      await listen(listenPort);
    },
    close: async () => {
      destroyLinks();
      await closeServer();
    },
  };
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
      expect(events.some((event) => event.event === "connect_succeeded")).toBe(true);
      expect(events.some((event) => event.event === "closed")).toBe(true);
    });

    it("should call tokenProvider again when reconnecting to the auth broker", async () => {
      if (authMode !== "valid_jwt") {
        return;
      }

      const worker = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      const tokenProvider = vi.fn(async () => generateValidTestJwt(testSecret(), testAudience()));

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

      const iterator = await caller.client().rpc().call(route, b("before"), { timeoutMs: 5000 });
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

      const nextIterator = await caller.client().rpc().call(route, b("after"), { timeoutMs: 5000 });
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
    it("should wait on the active reconnect when connect is called mid-recovery", async () => {
      const publisher = new TestFixture(transport, "anonymous");
      await publisher.connectOrFail();
      const proxy = await createTransportProxy(transport, brokerAddrFor(transport, "anonymous"));
      const events: FitzLifecycleEvent[] = [];
      const subscriber = new Client({
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

        const noticeClient = subscriber.notice();
        const leaseClient = subscriber.lease();
        const route = uniqueRoute("notice");
        const received: string[] = [];

        await noticeClient.subscribe(route, async (msg) => {
          received.push(Buffer.from(msg.body).toString());
        });

        await publisher.client().notice().publish(route, b("before"));
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

        expect(subscriber.notice()).toBe(noticeClient);
        expect(subscriber.lease()).toBe(leaseClient);

        await publisher.client().notice().publish(route, b("after"));
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
