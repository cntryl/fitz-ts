/// <reference types="node" />

import { createServer, type Server, type Socket } from "node:net";

import { afterEach, describe, expect, it } from "vite-plus/test";

import { createTcpTransport } from "../../../src/transport/tcp";

const servers: Server[] = [];
const sockets: Socket[] = [];

async function listenOnLocalhost(): Promise<number> {
  const server = createServer((socket) => {
    sockets.push(socket);
  });
  servers.push(server);

  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("expected tcp server address");
  }

  return address.port;
}

afterEach(async () => {
  for (const socket of sockets.splice(0)) {
    socket.destroy();
  }

  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve, reject) => {
          server.close((error) => {
            if (error) {
              reject(error);
              return;
            }

            resolve();
          });
        }),
    ),
  );
});

describe("tcp transport", () => {
  it("rejects receive immediately after close", async () => {
    const port = await listenOnLocalhost();
    const transport = createTcpTransport(`localhost:${port}`, { timeout: 20 });

    await transport.connect();
    await transport.close();

    await expect(transport.receive()).rejects.toThrow("Connection closed");
  });
});
