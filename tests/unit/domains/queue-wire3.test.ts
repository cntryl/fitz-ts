import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vite-plus/test";
import { createQueueClient } from "../../../src/domains/queue/client";
import { createQueueItem } from "../../../src/domains/queue/types";
import { FrameCodec } from "../../../src/frame/codec";

function connection(capabilityIds: readonly number[] = [1, 2]) {
  const response = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 1]);
  return {
    request: vi.fn(async () => new Uint8Array([0])),
    requestWithFlags: vi.fn(
      async (_type: number, _payload: Uint8Array, _flags: number) => response,
    ),
    onDisconnect: () => () => {},
    onReconnect: () => () => {},
    registerNotificationHandler: () => {},
    dispatchAsyncHandler: () => {},
    getServerCapabilities: () => ({ protocolVersion: 3, capabilities: 0, capabilityIds }),
  };
}

const vectors = new Map(
  readFileSync(new URL("../../fixtures/wire3_vectors.txt", import.meta.url), "utf8")
    .trim()
    .split("\n")
    .map((line) => line.split(" ") as [string, string]),
);

describe("Queue Wire 3 delivery controls", () => {
  it("encodes negotiated TTL exactly like the broker vector", async () => {
    const port = connection();
    const queue = createQueueClient(port);
    await queue.enqueue("queue://acme/jobs/q", {
      body: new TextEncoder().encode("job"),
      ttlHours: 2,
    });
    const [type, payload, flags] = port.requestWithFlags.mock.calls[0]!;
    expect(
      Buffer.from(FrameCodec.encodeFrame(type, payload, { flags, correlation: 1n })).toString(
        "hex",
      ),
    ).toBe(vectors.get("enqueue_wire3_ttl2h_corr1"));
  });

  it("rejects requested TTL without negotiated support before sending", async () => {
    const port = connection([]);
    const queue = createQueueClient(port);
    await expect(
      queue.enqueue("queue://acme/jobs/q", { body: new Uint8Array(), ttlHours: 2 }),
    ).rejects.toThrow("not negotiated");
    expect(port.requestWithFlags).not.toHaveBeenCalled();
  });

  it.each([0, -1, 1.5, 0x100000000])("rejects invalid TTL %s", async (ttlHours) => {
    const port = connection();
    await expect(
      createQueueClient(port).enqueue("queue://acme/jobs/q", { body: new Uint8Array(), ttlHours }),
    ).rejects.toThrow("positive u32");
    expect(port.requestWithFlags).not.toHaveBeenCalled();
  });

  it("encodes delayed NACK and closes the acknowledged reservation", async () => {
    const port = connection();
    const item = createQueueItem(1n, 9n, new Uint8Array(), "queue://acme/jobs/q", port);
    await item.nack({ visibilitySeconds: 60 });
    const call = port.request.mock.calls[0] as unknown as [number, Uint8Array];
    expect(
      Buffer.from(FrameCodec.encodeFrame(call[0], call[1], { correlation: 3n })).toString("hex"),
    ).toBe(vectors.get("nack_wire3_delay60_corr3"));
    await expect(item.complete()).rejects.toThrow("no longer valid");
  });

  it("rejects NACK without negotiated support before sending", async () => {
    const port = connection([]);
    const item = createQueueItem(1n, 9n, new Uint8Array(), "queue://acme/jobs/q", port);
    await expect(item.nack()).rejects.toThrow("not negotiated");
    expect(port.request).not.toHaveBeenCalled();
  });
});
