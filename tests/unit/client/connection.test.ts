import { describe, expect, it, vi } from "vitest";

import { Connection } from "../../../src/client/connection";
import { MSG_CONNECT } from "../../../src/frame/types";
import type { Transport } from "../../../src/transport/types";

class FakeTransport implements Transport {
  public sent: Uint8Array[] = [];
  public connected = false;
  private reads: Array<Uint8Array | Error> = [];
  private pendingRead: {
    resolve: (value: Uint8Array) => void;
    reject: (error: Error) => void;
  } | null = null;

  constructor(reads: Array<Uint8Array | Error> = []) {
    this.reads = reads;
  }

  async connect(): Promise<void> {
    this.connected = true;
  }

  async send(data: Uint8Array): Promise<void> {
    this.sent.push(data);
  }

  async receive(): Promise<Uint8Array> {
    const next = this.reads.shift();
    if (next instanceof Error) {
      this.connected = false;
      throw next;
    }

    if (next) {
      return next;
    }

    return await new Promise<Uint8Array>((resolve, reject) => {
      this.pendingRead = {
        resolve,
        reject: (error: Error) => {
          this.connected = false;
          reject(error);
        },
      };
    });
  }

  async close(): Promise<void> {
    this.connected = false;
    this.pendingRead?.reject(new Error("closed"));
    this.pendingRead = null;
  }

  getUrl(): string {
    return "ws://example.test";
  }

  isConnected(): boolean {
    return this.connected;
  }

  fail(error: Error): void {
    this.pendingRead?.reject(error);
    this.pendingRead = null;
  }
}

describe("Connection", () => {
  it("authenticates using the token provider and sends CONNECT first", async () => {
    const tokenProvider = vi.fn(async () => "jwt-token");
    const transport = new FakeTransport();
    const connection = new Connection(() => transport, tokenProvider, {
      authSettleDelayMs: 0,
    });

    await connection.connect();

    expect(connection.isConnected()).toBe(true);
    expect(tokenProvider).toHaveBeenCalledTimes(1);
    expect(transport.sent).toHaveLength(1);
    expect(transport.sent[0][0]).toBe(MSG_CONNECT);

    await connection.close();
  });

  it("supports anonymous mode with an empty token", async () => {
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
      },
    );

    await connection.connect();

    expect(connection.isConnected()).toBe(true);
    expect(transport.sent).toHaveLength(1);

    await connection.close();
  });

  it("reconnects and replays reconnect listeners after transport loss", async () => {
    const first = new FakeTransport();
    const second = new FakeTransport();
    const factory = vi
      .fn<() => Transport>()
      .mockReturnValueOnce(first)
      .mockReturnValueOnce(second);
    const tokenProvider = vi.fn(async () => "jwt-token");
    const restore = vi.fn(async () => undefined);

    const connection = new Connection(factory, tokenProvider, {
      authSettleDelayMs: 0,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });
    connection.onReconnect(restore);

    await connection.connect();
    first.fail(new Error("boom"));
    await vi.waitFor(() => {
      expect(connection.isConnected()).toBe(true);
      expect(factory).toHaveBeenCalledTimes(2);
      expect(restore).toHaveBeenCalledTimes(1);
    });

    await connection.close();
  });
});
