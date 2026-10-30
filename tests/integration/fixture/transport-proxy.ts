import type { AddressInfo } from "node:net";
import * as net from "node:net";
import { WebSocket, WebSocketServer } from "ws";

type ProxyController = {
  close: () => Promise<void>;
  goDown: () => Promise<void>;
  goUp: () => Promise<void>;
  url: string;
};

export async function createTransportProxy(
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
