import { describe, expect, it } from "vite-plus/test";
import { createConnection } from "../../../src/client/connection";
import { wire2HelloFrame } from "./wire-fixtures";
import type { Transport } from "../../../src/transport/types";

class NegotiationTransport implements Transport {
  connected = false;
  private receiveFrame: ((frame: Uint8Array) => void) | undefined;
  private rejectRead: ((error: Error) => void) | undefined;
  async connect() {
    this.connected = true;
  }
  async send(_data: Uint8Array) {}
  async receive(): Promise<Uint8Array> {
    return new Promise((resolve, reject) => {
      this.receiveFrame = resolve;
      this.rejectRead = reject;
    });
  }
  async close() {
    this.connected = false;
    this.rejectRead?.(new Error("closed"));
  }
  getUrl() {
    return "tcp://fixture";
  }
  isConnected() {
    return this.connected;
  }
  hello(capabilities: number) {
    this.receiveFrame?.(wire2HelloFrame(capabilities));
  }
}

describe("connection negotiation readiness", () => {
  it("waits for a positive hello beyond the authentication settlement window", async () => {
    const transport = new NegotiationTransport();
    const connection = createConnection(
      () => transport,
      () => "",
      { authSettleDelayMs: 1, timeout: 1000 },
    );
    let ready = false;
    const connected = connection.connect().then(() => {
      ready = true;
    });
    try {
      await new Promise((resolve) => setTimeout(resolve, 25));
      expect(ready).toBe(false);
      transport.hello(0);
      await connected;
      expect(connection.getServerCapabilities().protocolVersion).toBe(2);
      expect(connection.getServerCapabilities().capabilities).toBe(0);
    } finally {
      await connection.close();
    }
  });

  it("rejects a connection within its timeout when hello is absent", async () => {
    const transport = new NegotiationTransport();
    const connection = createConnection(
      () => transport,
      () => "",
      { authSettleDelayMs: 1, timeout: 25 },
    );
    await expect(connection.connect()).rejects.toThrow(/negotiation|hello|timed out/i);
    expect(transport.connected).toBe(false);
    await connection.close();
  });
});
