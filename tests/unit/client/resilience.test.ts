import { describe, expect, it, vi } from "vite-plus/test";

import { Connection } from "../../../src/client/connection";
import { ConnectionError, RequestQueueFullError } from "../../../src/core/errors";
import { Frame, FrameCodec } from "../../../src/frame/codec";
import { MSG_LEASE_QUERY, MSG_QUEUE_ENQUEUE } from "../../../src/frame/types";
import { LeaseClient } from "../../../src/domains/lease/client";
import { QueueClient } from "../../../src/domains/queue/client";
import type { Transport } from "../../../src/transport/types";

class ScriptedTransport implements Transport {
  public sent: Uint8Array[] = [];
  public connected = false;
  public connectStarted = false;
  public connectGate: Promise<void> | null = null;
  public onFrame: ((frame: Frame, transport: ScriptedTransport) => void | Promise<void>) | null =
    null;
  private reads: Array<Uint8Array | Error> = [];
  private pendingRead: {
    resolve: (value: Uint8Array) => void;
    reject: (error: Error) => void;
  } | null = null;

  async connect(): Promise<void> {
    this.connectStarted = true;
    if (this.connectGate) {
      await this.connectGate;
    }
    this.connected = true;
  }

  async send(data: Uint8Array): Promise<void> {
    this.sent.push(data);
    if (this.onFrame) {
      await this.onFrame(FrameCodec.decodeFrame(data), this);
    }
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

  pushRead(data: Uint8Array): void {
    if (this.pendingRead) {
      this.pendingRead.resolve(data);
      this.pendingRead = null;
      return;
    }

    this.reads.push(data);
  }

  fail(error: Error): void {
    if (this.pendingRead) {
      this.pendingRead.reject(error);
      this.pendingRead = null;
      return;
    }

    this.reads.push(error);
  }

  sentCount(messageType: number): number {
    return this.sent
      .map((frame) => FrameCodec.decodeFrame(frame))
      .filter((frame) => frame.messageType === messageType).length;
  }
}

function u64Bytes(value: bigint): Uint8Array {
  const bytes = new Uint8Array(8);
  let remaining = value;
  for (let index = 7; index >= 0; index -= 1) {
    bytes[index] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
  return bytes;
}

function encodeQueueSuccess(messageId: bigint): Uint8Array {
  const payload = new Uint8Array(9);
  payload[0] = 0;
  payload.set(u64Bytes(messageId), 1);
  return payload;
}

function encodeQueueErrorMessage(message: string): Uint8Array {
  const messageBytes = new TextEncoder().encode(message);
  const payload = new Uint8Array(5 + messageBytes.length);
  payload[0] = 1;
  payload[1] = (messageBytes.length >> 24) & 0xff;
  payload[2] = (messageBytes.length >> 16) & 0xff;
  payload[3] = (messageBytes.length >> 8) & 0xff;
  payload[4] = messageBytes.length & 0xff;
  payload.set(messageBytes, 5);
  return payload;
}

function encodeQueueErrorCode(errorCode: number): Uint8Array {
  return Uint8Array.of(1, errorCode);
}

function encodeLeaseQueryFree(): Uint8Array {
  return Uint8Array.of(0, 0, 0, 0, 0, 0);
}

describe("Connection resilience", () => {
  it("waits for reconnect before sending a new operation", async () => {
    const first = new ScriptedTransport();
    const second = new ScriptedTransport();
    let releaseReconnect: () => void = () => undefined;
    second.connectGate = new Promise<void>((resolve) => {
      releaseReconnect = resolve;
    });

    second.onFrame = async (frame, transport) => {
      if (frame.messageType === MSG_LEASE_QUERY) {
        transport.pushRead(FrameCodec.encodeFrame(MSG_LEASE_QUERY, encodeLeaseQueryFree()));
      }
    };

    const factory = vi.fn<() => Transport>().mockReturnValueOnce(first).mockReturnValueOnce(second);
    const connection = new Connection(factory, () => "", {
      authSettleDelayMs: 0,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });
    const lease = new LeaseClient(connection);

    await connection.connect();
    first.fail(new Error("boom"));

    await vi.waitFor(() => {
      expect(second.connectStarted).toBe(true);
      expect(connection.getState()).toBe("RECONNECTING");
    });

    let settled = false;
    const queryPromise = lease.query("lease://realm/area/resource").then((result) => {
      settled = true;
      return result;
    });

    await Promise.resolve();
    expect(settled).toBe(false);

    releaseReconnect();

    await expect(queryPromise).resolves.toMatchObject({ isHeld: false });
    expect(second.sentCount(MSG_LEASE_QUERY)).toBe(1);

    await connection.close();
  });

  it("bounds reconnect waiters with the configured request queue size", async () => {
    const first = new ScriptedTransport();
    const second = new ScriptedTransport();
    let releaseReconnect: () => void = () => undefined;
    second.connectGate = new Promise<void>((resolve) => {
      releaseReconnect = resolve;
    });

    second.onFrame = async (frame, transport) => {
      if (frame.messageType === MSG_LEASE_QUERY) {
        transport.pushRead(FrameCodec.encodeFrame(MSG_LEASE_QUERY, encodeLeaseQueryFree()));
      }
    };

    const factory = vi.fn<() => Transport>().mockReturnValueOnce(first).mockReturnValueOnce(second);
    const connection = new Connection(factory, () => "", {
      authSettleDelayMs: 0,
      maxRequestQueueSize: 1,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });
    const lease = new LeaseClient(connection);

    await connection.connect();
    first.fail(new Error("boom"));

    await vi.waitFor(() => {
      expect(connection.getState()).not.toBe("AUTHENTICATED");
    });

    const firstQuery = lease.query("lease://realm/area/resource");
    await Promise.resolve();

    await expect(lease.query("lease://realm/area/resource")).rejects.toBeInstanceOf(
      RequestQueueFullError,
    );

    releaseReconnect();

    await expect(firstQuery).resolves.toMatchObject({ isHeld: false });
    await connection.close();
  });

  it("fails immediately during disconnects when reconnect is disabled", async () => {
    const transport = new ScriptedTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        reconnect: {
          enabled: false,
        },
      },
    );
    const lease = new LeaseClient(connection);

    await connection.connect();
    transport.fail(new Error("boom"));

    await vi.waitFor(() => {
      expect(connection.getState()).toBe("DISCONNECTED");
    });

    await expect(lease.query("lease://realm/area/resource")).rejects.toBeInstanceOf(
      ConnectionError,
    );
    await connection.close();
  });

  it("retries a replayable read after transient transport loss", async () => {
    const first = new ScriptedTransport();
    const second = new ScriptedTransport();

    first.onFrame = async (frame, transport) => {
      if (frame.messageType === MSG_LEASE_QUERY) {
        transport.fail(new Error("boom"));
      }
    };

    second.onFrame = async (frame, transport) => {
      if (frame.messageType === MSG_LEASE_QUERY) {
        transport.pushRead(FrameCodec.encodeFrame(MSG_LEASE_QUERY, encodeLeaseQueryFree()));
      }
    };

    const factory = vi.fn<() => Transport>().mockReturnValueOnce(first).mockReturnValueOnce(second);
    const connection = new Connection(factory, () => "", {
      authSettleDelayMs: 0,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
      retry: {
        enabled: true,
        maxAttempts: 2,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });
    const lease = new LeaseClient(connection);

    await connection.connect();

    await expect(lease.query("lease://realm/area/resource")).resolves.toMatchObject({
      isHeld: false,
    });
    expect(first.sentCount(MSG_LEASE_QUERY)).toBe(1);
    expect(second.sentCount(MSG_LEASE_QUERY)).toBe(1);

    await connection.close();
  });

  it("retries queue enqueue after a classified transient commit failure", async () => {
    const transport = new ScriptedTransport();
    let enqueueAttempts = 0;
    transport.onFrame = async (frame, activeTransport) => {
      if (frame.messageType !== MSG_QUEUE_ENQUEUE) {
        return;
      }

      enqueueAttempts += 1;
      if (enqueueAttempts === 1) {
        activeTransport.pushRead(
          FrameCodec.encodeFrame(
            MSG_QUEUE_ENQUEUE,
            encodeQueueErrorMessage(
              'Failed to commit transaction: WriteStall("Memory budget exceeded")',
            ),
          ),
        );
        return;
      }

      activeTransport.pushRead(FrameCodec.encodeFrame(MSG_QUEUE_ENQUEUE, encodeQueueSuccess(7n)));
    };

    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        retry: {
          enabled: true,
          maxAttempts: 3,
          backoffMs: 0,
          maxBackoffMs: 0,
        },
      },
    );
    const queue = new QueueClient(connection);

    await connection.connect();

    await expect(queue.enqueue("queue://realm/area/resource", new Uint8Array([1]))).resolves.toBe(
      7n,
    );
    expect(enqueueAttempts).toBe(2);

    await connection.close();
  });

  it("does not retry permanent queue enqueue failures", async () => {
    const transport = new ScriptedTransport();
    transport.onFrame = async (frame, activeTransport) => {
      if (frame.messageType === MSG_QUEUE_ENQUEUE) {
        activeTransport.pushRead(
          FrameCodec.encodeFrame(MSG_QUEUE_ENQUEUE, encodeQueueErrorCode(3)),
        );
      }
    };

    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        retry: {
          enabled: true,
          maxAttempts: 3,
          backoffMs: 0,
          maxBackoffMs: 0,
        },
      },
    );
    const queue = new QueueClient(connection);

    await connection.connect();

    await expect(
      queue.enqueue("queue://realm/area/resource", new Uint8Array([1])),
    ).rejects.toMatchObject({
      code: "QUEUE_InvalidToken",
    });
    expect(transport.sentCount(MSG_QUEUE_ENQUEUE)).toBe(1);

    await connection.close();
  });

  it("does not retry queue enqueue after an ambiguous post-send disconnect", async () => {
    const transport = new ScriptedTransport();
    transport.onFrame = async (frame, activeTransport) => {
      if (frame.messageType === MSG_QUEUE_ENQUEUE) {
        activeTransport.fail(new Error("boom"));
      }
    };

    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        reconnect: {
          enabled: false,
        },
        retry: {
          enabled: true,
          maxAttempts: 3,
          backoffMs: 0,
          maxBackoffMs: 0,
        },
      },
    );
    const queue = new QueueClient(connection);

    await connection.connect();

    await expect(
      queue.enqueue("queue://realm/area/resource", new Uint8Array([1])),
    ).rejects.toBeInstanceOf(ConnectionError);
    expect(transport.sentCount(MSG_QUEUE_ENQUEUE)).toBe(1);

    await connection.close();
  });
});
