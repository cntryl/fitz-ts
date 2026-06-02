import { describe, expect, it } from "vite-plus/test";

import { createTransport } from "../../../src/transport/factory";

describe("transport factory", () => {
  it("normalizes http and https URLs to websocket transports", () => {
    const httpTransport = createTransport("http://example.test/ws", "auto");
    const httpsTransport = createTransport("https://example.test/ws", "auto");

    expect(httpTransport.getUrl()).toBe("ws://example.test/ws");

    expect(httpsTransport.getUrl()).toBe("wss://example.test/ws");
  });

  it("returns tcp transports explicitly in Node.js", () => {
    const transport = createTransport("tcp://example.test:4090", "tcp");

    expect(transport.getUrl()).toBe("tcp://example.test:4090");
  });

  it("keeps websocket URLs unchanged in auto mode", () => {
    const transport = createTransport("ws://example.test/ws", "auto");

    expect(transport.getUrl()).toBe("ws://example.test/ws");
  });
});
