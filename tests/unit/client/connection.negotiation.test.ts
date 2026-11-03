import { describe, expect, it } from "vite-plus/test";
import { createConnection } from "../../../src/client/connection";
import { createBufferWriter } from "../../../src/core/buffer";
import { ProtocolError } from "../../../src/core/errors";
import { FrameCodec } from "../../../src/frame/codec";
import { MSG_CONNECT_REJECTED } from "../../../src/frame/types";
import { wire3HelloFrame } from "./wire-fixtures";
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
  hello(capabilities: number, maxMessageBytes = 1024 * 1024) {
    this.receiveFrame?.(wire3HelloFrame(capabilities, maxMessageBytes));
  }
  reject(code: number, message: string) {
    const writer = createBufferWriter();
    writer.writeU16BE(code);
    writer.writeString(message);
    this.receiveFrame?.(FrameCodec.encodeFrame(MSG_CONNECT_REJECTED, writer.getBuffer()));
  }
}

describe("connection negotiation readiness", () => {
  it("closes the session if the broker repeats HELLO", async () => {
    const transport = new NegotiationTransport();
    const connection = createConnection(
      () => transport,
      () => "",
      { timeout: 1000 },
    );
    try {
      const connected = connection.connect();
      await new Promise((resolve) => setTimeout(resolve, 10));
      transport.hello(0);
      await connected;
      transport.hello(0);
      await expect.poll(() => connection.isConnected(), { timeout: 300 }).toBe(false);
    } finally {
      await connection.close();
    }
  });

  it("retains the local frame limit if HELLO advertises a larger limit", async () => {
    const transport = new NegotiationTransport();
    const connection = createConnection(
      () => transport,
      () => "",
      { timeout: 1000 },
    );
    try {
      const connected = connection.connect();
      await new Promise((resolve) => setTimeout(resolve, 10));
      transport.hello(0, 2 * 1024 * 1024);
      await connected;
      expect(connection.getServerCapabilities().maxMessageBytes).toBe(1024 * 1024);
    } finally {
      await connection.close();
    }
  });

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
      expect(connection.getServerCapabilities().protocolVersion).toBe(3);
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

  it("surfaces version mismatch with its broker code and message", async () => {
    const transport = new NegotiationTransport();
    const connection = createConnection(
      () => transport,
      () => "",
      { timeout: 1000 },
    );
    const connecting = connection.connect();
    try {
      await new Promise<void>((resolve) => setTimeout(resolve, 25));
      transport.reject(50, "no common wire version");
      await expect(connecting).rejects.toMatchObject({
        name: "ProtocolError",
        domainCode: 50,
        message: expect.stringContaining("no common wire version"),
      } satisfies Partial<ProtocolError>);
    } finally {
      await connection.close();
    }
  });
});
