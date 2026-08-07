import { describe, expect, it } from "vite-plus/test";

import type { Connection } from "../../../src/client/connection";
import { createLeaseClient } from "../../../src/domains/lease/client";
import { LeaseCodec } from "../../../src/domains/lease/codec";
import { MSG_LEASE_ACQUIRE } from "../../../src/frame/types";

function acquireResponse(kind: 0 | 1 | 2 | 3, token: bigint): Uint8Array {
  const bytes = new Uint8Array(10);
  bytes[0] = 0;
  bytes[1] = kind;
  new DataView(bytes.buffer).setBigUint64(2, token);
  return bytes;
}

class FakeLeaseConnection {
  readonly handlers = new Map<number, (payload: Uint8Array) => void>();
  readonly requests: Array<{ messageType: number; payload: Uint8Array }> = [];
  private readonly disconnectListeners = new Set<() => void>();

  constructor(private readonly responses: Uint8Array[]) {}

  async request(messageType: number, payload: Uint8Array): Promise<Uint8Array> {
    this.requests.push({ messageType, payload });
    const response = this.responses.shift();
    if (!response) throw new Error("missing fake response");
    return response;
  }

  registerNotificationHandler(type: number, handler: (payload: Uint8Array) => void): void {
    this.handlers.set(type, handler);
  }

  onReconnect(): () => void {
    return () => undefined;
  }
  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => this.disconnectListeners.delete(listener);
  }
  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void task();
  }
}

describe("lease acquisition", () => {
  it("encodes the canonical wait_seconds field, including zero", () => {
    const zero = LeaseCodec.encodeAcquire("lease://realm/area/resource", 30);
    const waiting = LeaseCodec.encodeAcquire("lease://realm/area/resource", 30, 17);

    expect(
      new DataView(zero.buffer, zero.byteOffset, zero.byteLength).getUint32(zero.length - 4),
    ).toBe(0);
    expect(
      new DataView(waiting.buffer, waiting.byteOffset, waiting.byteLength).getUint32(
        waiting.length - 4,
      ),
    ).toBe(17);
  });

  it("resolves a queued acquisition from the deferred ACQUIRE frame", async () => {
    const connection = new FakeLeaseConnection([acquireResponse(2, 0n)]);
    const client = createLeaseClient(connection as unknown as Connection);

    const pending = client.acquire("lease://realm/area/resource", 30, { waitSeconds: 12 });
    await Promise.resolve();
    connection.handlers.get(MSG_LEASE_ACQUIRE)?.(acquireResponse(0, 42n));

    await expect(pending).resolves.toBeDefined();
    expect(connection.requests[0]?.messageType).toBe(MSG_LEASE_ACQUIRE);
    const payload = connection.requests[0]!.payload;
    expect(
      new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getUint32(
        payload.length - 4,
      ),
    ).toBe(12);
  });

  it("serializes acquisition lifecycles until deferred completion arrives", async () => {
    const connection = new FakeLeaseConnection([acquireResponse(2, 0n), acquireResponse(0, 99n)]);
    const client = createLeaseClient(connection as unknown as Connection);

    const first = client.acquire("lease://realm/area/first", 30, { waitSeconds: 12 });
    const second = client.acquire("lease://realm/area/second", 30);
    await Promise.resolve();
    await Promise.resolve();
    expect(connection.requests).toHaveLength(1);

    connection.handlers.get(MSG_LEASE_ACQUIRE)?.(acquireResponse(0, 42n));
    await first;
    await second;
    expect(connection.requests).toHaveLength(2);
  });

  it("preserves the broker message on a canonical deferred timeout", async () => {
    const connection = new FakeLeaseConnection([acquireResponse(2, 0n)]);
    const client = createLeaseClient(connection as unknown as Connection);
    const pending = client.acquire("lease://realm/area/resource", 30, { waitSeconds: 1 });
    await Promise.resolve();
    const message = new TextEncoder().encode("lease wait timed out");
    const error = new Uint8Array(1 + 4 + message.length);
    error[0] = 1;
    const view = new DataView(error.buffer);
    view.setUint32(1, message.length);
    error.set(message, 5);
    connection.handlers.get(MSG_LEASE_ACQUIRE)?.(error);

    await expect(pending).rejects.toMatchObject({ domainCode: 1 });
    await expect(pending).rejects.toThrow("lease wait timed out");
  });
});
