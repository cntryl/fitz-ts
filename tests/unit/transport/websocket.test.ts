/// <reference types="node" />

import { createHash } from "node:crypto";
import { createServer, type IncomingHttpHeaders } from "node:http";
import type { Duplex } from "node:stream";

import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { WebSocketServer } from "ws";

import { createWebSocketTransport } from "../../../src/transport/websocket.node";

const servers: Array<{ close: (callback?: (err?: Error) => void) => void }> = [];
const sockets: Duplex[] = [];

function websocketAccept(key: string): string {
  return createHash("sha1").update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`).digest("base64");
}

async function captureUpgradeHeaders(
  connect: (url: string) => Promise<void>,
): Promise<IncomingHttpHeaders> {
  const server = createServer();
  servers.push(server);

  const headersPromise = new Promise<IncomingHttpHeaders>((resolve) => {
    server.once("upgrade", (req, socket) => {
      sockets.push(socket);
      const key = req.headers["sec-websocket-key"];
      if (typeof key !== "string") {
        socket.destroy();
        return;
      }

      socket.write(
        [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${websocketAccept(key)}`,
          "",
          "",
        ].join("\r\n"),
      );
      resolve(req.headers);
      socket.destroy();
    });
  });

  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("expected tcp server address");
  }

  void connect(`ws://127.0.0.1:${address.port}/ws`).catch(() => undefined);
  return headersPromise;
}

async function listenWithWebSocketServer(): Promise<{ close: () => Promise<void>; url: string }> {
  const server = new WebSocketServer({
    host: "127.0.0.1",
    port: 0,
  });
  servers.push(server);

  await new Promise<void>((resolve) => {
    server.once("listening", resolve);
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("expected websocket server address");
  }

  return {
    url: `ws://127.0.0.1:${address.port}/ws`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });
      }),
  };
}

async function listenWithHangingUpgradeServer(): Promise<{ url: string }> {
  const server = createServer();
  servers.push(server);

  server.on("upgrade", (_req, socket) => {
    sockets.push(socket);
  });

  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("expected websocket server address");
  }

  return {
    url: `ws://127.0.0.1:${address.port}/ws`,
  };
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

describe("websocket transport", () => {
  it("sends normal Node upgrade headers by default", async () => {
    const headers = await captureUpgradeHeaders(async (url) => {
      const transport = createWebSocketTransport(url);
      await transport.connect();
    });

    expect(headers["user-agent"]).toBe("@cntryl/fitz");
    expect(headers.accept).toBe("*/*");
  });

  it("merges configured Node upgrade headers over defaults", async () => {
    const headers = await captureUpgradeHeaders(async (url) => {
      const transport = createWebSocketTransport(url, {
        webSocket: {
          headers: {
            "User-Agent": "fitz-test",
            "X-Fitz-Test": "present",
          },
        },
      });
      await transport.connect();
    });

    expect(headers["user-agent"]).toBe("fitz-test");
    expect(headers.accept).toBe("*/*");
    expect(headers["x-fitz-test"]).toBe("present");
  });

  it("overrides default Node upgrade headers case-insensitively", async () => {
    const headers = await captureUpgradeHeaders(async (url) => {
      const transport = createWebSocketTransport(url, {
        webSocket: {
          headers: {
            accept: "application/octet-stream",
            "user-agent": "fitz-test",
          },
        },
      });
      await transport.connect();
    });

    expect(headers["user-agent"]).toBe("fitz-test");
    expect(headers.accept).toBe("application/octet-stream");
  });

  it("supports Node ping/pong heartbeats", async () => {
    const { url } = await listenWithWebSocketServer();
    const transport = createWebSocketTransport(url, { timeout: 100 });

    await transport.connect();

    expect(transport.supportsHeartbeat?.()).toBe(true);
    await expect(transport.sendHeartbeat?.({ timeoutMs: 100 })).resolves.toBeUndefined();

    await transport.close();
  });

  it("rejects Node sends once graceful close begins", async () => {
    const { url } = await listenWithWebSocketServer();
    const transport = createWebSocketTransport(url, { timeout: 100 });

    await transport.connect();

    const closing = transport.close();
    await expect(transport.send(new Uint8Array([1, 2, 3]))).rejects.toThrow("not connected");
    await closing;
  });

  it("rejects Node connect when close happens before open", async () => {
    const { url } = await listenWithHangingUpgradeServer();
    const transport = createWebSocketTransport(url, { timeout: 1000 });

    const connect = transport.connect();
    await vi.waitFor(() => {
      expect(sockets.length).toBeGreaterThan(0);
    });

    await transport.close();
    await expect(connect).rejects.toThrow(/closed during connect|WebSocket error/i);
  });
});

describe("browser websocket transport", () => {
  it("resolves browser sends without waiting for a Node callback", async () => {
    const originalWebSocket = globalThis.WebSocket;

    class BrowserWebSocket {
      binaryType = "";
      onopen: (() => void) | null = null;
      onmessage: ((event: { data: ArrayBuffer | Uint8Array | Blob }) => void) | null = null;
      onerror: ((event: { message?: string }) => void) | null = null;
      onclose: (() => void) | null = null;
      readonly sent: Uint8Array[] = [];

      constructor(_url: string) {
        setTimeout(() => this.onopen?.(), 0);
      }

      send(data: Uint8Array): void {
        this.sent.push(data);
      }

      close(): void {
        this.onclose?.();
      }
    }

    vi.stubGlobal("WebSocket", BrowserWebSocket);

    try {
      const { createWebSocketTransport: createBrowserTransport } =
        await import("../../../src/transport/websocket.browser");
      const transport = createBrowserTransport("ws://example.test/ws", { timeout: 20 });

      await transport.connect();

      await expect(transport.send(new Uint8Array([1, 2, 3]))).resolves.toBeUndefined();
    } finally {
      vi.stubGlobal("WebSocket", originalWebSocket);
    }
  });

  it("rejects browser sends once graceful close begins", async () => {
    const originalWebSocket = globalThis.WebSocket;

    class BrowserWebSocket {
      static instances: BrowserWebSocket[] = [];

      binaryType = "";
      onopen: (() => void) | null = null;
      onmessage: ((event: { data: ArrayBuffer | Uint8Array | Blob }) => void) | null = null;
      onerror: ((event: { message?: string }) => void) | null = null;
      onclose: (() => void) | null = null;

      constructor(_url: string) {
        BrowserWebSocket.instances.push(this);
        setTimeout(() => this.onopen?.(), 0);
      }

      send(_data: Uint8Array): void {}

      close(): void {}
    }

    vi.stubGlobal("WebSocket", BrowserWebSocket);

    try {
      const { createWebSocketTransport: createBrowserTransport } =
        await import("../../../src/transport/websocket.browser");
      const transport = createBrowserTransport("ws://example.test/ws", { timeout: 20 });

      await transport.connect();
      const socket = BrowserWebSocket.instances[0];

      const closing = transport.close();
      await expect(transport.send(new Uint8Array([1, 2, 3]))).rejects.toThrow("not connected");

      socket.onclose?.();
      await closing;
    } finally {
      vi.stubGlobal("WebSocket", originalWebSocket);
    }
  });

  it("rejects browser connect when close happens before open", async () => {
    const originalWebSocket = globalThis.WebSocket;

    class BrowserWebSocket {
      binaryType = "";
      onopen: (() => void) | null = null;
      onmessage: ((event: { data: ArrayBuffer | Uint8Array | Blob }) => void) | null = null;
      onerror: ((event: { message?: string }) => void) | null = null;
      onclose: (() => void) | null = null;

      constructor(_url: string) {}

      send(_data: Uint8Array): void {}

      close(): void {
        this.onclose?.();
      }
    }

    vi.stubGlobal("WebSocket", BrowserWebSocket);

    try {
      const { createWebSocketTransport: createBrowserTransport } =
        await import("../../../src/transport/websocket.browser");
      const transport = createBrowserTransport("ws://example.test/ws", { timeout: 1000 });

      const connect = transport.connect();
      await transport.close();

      await expect(connect).rejects.toThrow("closed during connect");
    } finally {
      vi.stubGlobal("WebSocket", originalWebSocket);
    }
  });

  it("stays disconnected when browser onopen fires after connect timeout", async () => {
    vi.useFakeTimers();

    const originalWebSocket = globalThis.WebSocket;

    class SlowWebSocket {
      static instances: SlowWebSocket[] = [];

      binaryType = "";
      onopen: (() => void) | null = null;
      onmessage: ((event: { data: ArrayBuffer | Uint8Array | Blob }) => void) | null = null;
      onerror: ((event: { message?: string }) => void) | null = null;
      onclose: (() => void) | null = null;

      constructor(_url: string) {
        SlowWebSocket.instances.push(this);
      }

      send(_data: Uint8Array): void {}

      close(): void {}
    }

    vi.stubGlobal("WebSocket", SlowWebSocket);

    try {
      const { createWebSocketTransport: createBrowserTransport } =
        await import("../../../src/transport/websocket.browser");
      const transport = createBrowserTransport("ws://example.test/ws", { timeout: 10 });
      const connect = transport.connect();
      const connectResult = connect.catch((error: unknown) => error);

      await vi.advanceTimersByTimeAsync(10);
      expect(await connectResult).toBeInstanceOf(Error);
      expect(await connectResult).toMatchObject({ message: expect.stringMatching(/timeout/i) });

      SlowWebSocket.instances[0].onopen?.();

      expect(transport.isConnected()).toBe(false);
    } finally {
      vi.stubGlobal("WebSocket", originalWebSocket);
      vi.useRealTimers();
    }
  });

  it("stays disconnected when browser onopen fires after connect error", async () => {
    const originalWebSocket = globalThis.WebSocket;

    class ErrorWebSocket {
      static instances: ErrorWebSocket[] = [];

      binaryType = "";
      onopen: (() => void) | null = null;
      onmessage: ((event: { data: ArrayBuffer | Uint8Array | Blob }) => void) | null = null;
      onerror: ((event: { message?: string }) => void) | null = null;
      onclose: (() => void) | null = null;

      constructor(_url: string) {
        ErrorWebSocket.instances.push(this);
      }

      send(_data: Uint8Array): void {}

      close(): void {}
    }

    vi.stubGlobal("WebSocket", ErrorWebSocket);

    try {
      const { createWebSocketTransport: createBrowserTransport } =
        await import("../../../src/transport/websocket.browser");
      const transport = createBrowserTransport("ws://example.test/ws", { timeout: 20 });
      const connect = transport.connect();

      ErrorWebSocket.instances[0].onerror?.({ message: "dial failed" });
      await expect(connect).rejects.toThrow("dial failed");

      ErrorWebSocket.instances[0].onopen?.();

      expect(transport.isConnected()).toBe(false);
    } finally {
      vi.stubGlobal("WebSocket", originalWebSocket);
    }
  });

  it("receives browser ArrayBuffer, Uint8Array, and Blob messages in worker-like globals", async () => {
    const originalWebSocket = globalThis.WebSocket;

    class BrowserWebSocket {
      static instances: BrowserWebSocket[] = [];

      binaryType: BinaryType = "blob";
      onopen: (() => void) | null = null;
      onmessage: ((event: { data: ArrayBuffer | Uint8Array | Blob }) => void) | null = null;
      onerror: ((event: { message?: string }) => void) | null = null;
      onclose: (() => void) | null = null;
      readonly url: string;

      constructor(url: string) {
        this.url = url;
        BrowserWebSocket.instances.push(this);
      }

      send(_data: Uint8Array): void {}

      close(): void {
        this.onclose?.();
      }
    }

    vi.stubGlobal("WebSocket", BrowserWebSocket);

    try {
      const { createWebSocketTransport: createBrowserTransport } =
        await import("../../../src/transport/websocket.browser");
      const transport = createBrowserTransport("https://example.test/ws", { timeout: 20 });
      const connect = transport.connect();
      const socket = BrowserWebSocket.instances[0];

      socket.onopen?.();
      await connect;

      expect(socket.url).toBe("wss://example.test/ws");
      expect(socket.binaryType).toBe("arraybuffer");
      expect(transport.supportsHeartbeat?.()).toBe(false);
      await expect(transport.sendHeartbeat?.({ timeoutMs: 1 })).rejects.toThrow(
        "not supported in browsers",
      );

      const uint8Receive = transport.receive();
      socket.onmessage?.({ data: new Uint8Array([1, 2, 3]) });
      await expect(uint8Receive).resolves.toEqual(new Uint8Array([1, 2, 3]));

      const arrayBufferReceive = transport.receive();
      socket.onmessage?.({ data: new Uint8Array([4, 5]).buffer });
      await expect(arrayBufferReceive).resolves.toEqual(new Uint8Array([4, 5]));

      const blobReceive = transport.receive();
      socket.onmessage?.({ data: new Blob([new Uint8Array([6, 7])]) });
      await expect(blobReceive).resolves.toEqual(new Uint8Array([6, 7]));

      await transport.close();
    } finally {
      vi.stubGlobal("WebSocket", originalWebSocket);
    }
  });
});
