/// <reference types="node" />

import { createServer, type Server, type Socket } from "node:net";

import { afterEach, describe, expect, it } from "vite-plus/test";

import { createTcpTransport } from "../../../src/transport/tcp";
import { FrameCodec } from "../../../src/frame/codec";
import { TimeoutError } from "../../../src/core/errors";

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
  it("accepts the maximum legal TLV value with the default total-frame limit", async () => {
    const payload = new Uint8Array(65_535);
    payload.fill(0x5a);
    const frame = FrameCodec.encodeFrame(65_535, payload);
    const packet = new Uint8Array(4 + frame.length);
    new DataView(packet.buffer).setUint32(0, frame.length, false);
    packet.set(frame, 4);
    const server = createServer((socket) => {
      sockets.push(socket);
      socket.write(packet);
    });
    servers.push(server);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("expected TCP address");
    const transport = createTcpTransport(`localhost:${address.port}`, { timeout: 100 });

    await transport.connect();
    const received = await transport.receive();

    expect(received).toEqual(frame);
    await transport.close();
  });

  it("rejects connect when the signal is already aborted", async () => {
    const port = await listenOnLocalhost();
    const transport = createTcpTransport(`localhost:${port}`, { timeout: 20 });
    const controller = new AbortController();
    controller.abort();

    await expect(transport.connect({ signal: controller.signal })).rejects.toHaveProperty(
      "name",
      "AbortError",
    );
  });

  it("rejects receive immediately after close", async () => {
    const port = await listenOnLocalhost();
    const transport = createTcpTransport(`localhost:${port}`, { timeout: 20 });

    await transport.connect();
    await transport.close();

    await expect(transport.receive()).rejects.toThrow("Connection closed");
  });

  it("preserves terminal socket errors for pending and future receives", async () => {
    const server = createServer((socket) => {
      sockets.push(socket);
      socket.on("error", () => undefined);
      socket.destroy(new Error("server exploded"));
    });
    servers.push(server);

    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", resolve);
    });

    const address = server.address();
    if (!address || typeof address === "string") {
      throw new Error("expected tcp server address");
    }

    const transport = createTcpTransport(`localhost:${address.port}`, { timeout: 100 });
    await transport.connect();

    const pending = transport.receive();
    await expect(pending).rejects.toThrow(/TCP error|Connection closed|reset/i);
    await expect(transport.receive()).rejects.toThrow(/TCP error|Connection closed|reset/i);
  });

  it("preserves the TimeoutError classification even after destroy() re-triggers the 'error' listener", async () => {
    // A server that accepts the connection but never sends anything, so the
    // client-side idle receive-timeout fires. That timeout calls
    // `socket.destroy(error)`, which re-emits 'error' shortly after — the
    // resulting classification must stay TimeoutError, not get silently
    // overwritten by the generic TransportError the 'error' listener builds.
    const port = await listenOnLocalhost();
    const transport = createTcpTransport(`localhost:${port}`, { timeout: 30 });
    await transport.connect();

    const first = transport.receive();
    await expect(first).rejects.toBeInstanceOf(TimeoutError);
    // A later call must observe the same terminal classification, not a
    // downgraded one recorded by a subsequent 'error' event.
    await expect(transport.receive()).rejects.toBeInstanceOf(TimeoutError);
  });

  it("does not leak a 'drain' listener when send() times out while the socket is backpressured", async () => {
    // A server that accepts the connection but never reads from it, so a
    // large client-side write backpressures and never drains before the
    // short send timeout below fires.
    let paused: Socket | undefined;
    const server = createServer((socket) => {
      sockets.push(socket);
      socket.pause();
      paused = socket;
    });
    servers.push(server);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("expected tcp address");

    const transport = createTcpTransport(`localhost:${address.port}`, { timeout: 20 });
    await transport.connect();

    const warnings: string[] = [];
    const onWarning = (warning: Error): void => {
      warnings.push(`${warning.name}: ${warning.message}`);
    };
    process.on("warning", onWarning);

    try {
      // Large enough to exceed both Node's 16KB highWaterMark and the
      // OS-level TCP send buffer on loopback, so each write reliably stays
      // backpressured past the short timeout below instead of draining
      // near-instantly over localhost.
      const bigPayload = new Uint8Array(64 * 1024 * 1024);

      // Node's default max-listener warning threshold is 10 for a single
      // event on one EventEmitter — the underlying socket is reused across
      // these sends, so a dangling 'drain' listener per timed-out send
      // would accumulate on it and eventually trip
      // MaxListenersExceededWarning if the fix's cleanup weren't working.
      for (let i = 0; i < 12; i++) {
        await expect(transport.send(bigPayload)).rejects.toThrow(/timeout/i);
        // Let this iteration's abandoned write actually flush before
        // starting the next one, so backlogged writes don't pile up
        // unboundedly across iterations.
        paused?.resume();
        await new Promise((resolve) => setTimeout(resolve, 30));
        paused?.pause();
      }
      await new Promise((resolve) => setImmediate(resolve));

      expect(warnings.some((message) => message.includes("MaxListeners"))).toBe(false);
    } finally {
      process.removeListener("warning", onWarning);
    }
  });
});
