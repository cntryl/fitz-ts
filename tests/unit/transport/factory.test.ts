import { describe, expect, it } from "vite-plus/test";

import { createBrowserTransport } from "../../../src/transport/factory.browser";
import { createNodeTransport, createTransport } from "../../../src/transport/factory";

describe("transport factory", () => {
  it("normalizes http and https URLs to Node websocket transports", () => {
    const httpTransport = createNodeTransport("http://example.test/ws", "auto");
    const httpsTransport = createNodeTransport("https://example.test/ws", "auto");

    expect(httpTransport.getUrl()).toBe("ws://example.test/ws");

    expect(httpsTransport.getUrl()).toBe("wss://example.test/ws");
  });

  it("returns tcp transports explicitly in Node.js", () => {
    const transport = createNodeTransport("tcp://example.test:4090", "tcp");

    expect(transport.getUrl()).toBe("tcp://example.test:4090");
  });

  it("keeps websocket URLs unchanged in auto mode", () => {
    const transport = createTransport("ws://example.test/ws", "auto");

    expect(transport.getUrl()).toBe("ws://example.test/ws");
  });

  it("normalizes http and https URLs to browser websocket transports", () => {
    const httpTransport = createBrowserTransport("http://example.test/ws", "auto");
    const httpsTransport = createBrowserTransport("https://example.test/ws", "auto");

    expect(httpTransport.getUrl()).toBe("ws://example.test/ws");
    expect(httpsTransport.getUrl()).toBe("wss://example.test/ws");
  });

  it("rejects explicit and URL-detected TCP in browsers", () => {
    expect(() => createBrowserTransport("tcp://example.test:4090", "auto")).toThrow(
      "TCP transport is not available in browsers",
    );
    expect(() => createBrowserTransport("example.test:4090", "tcp")).toThrow(
      "TCP transport is not available in browsers",
    );
  });
});
