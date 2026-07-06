import { describe, expect, it, vi } from "vite-plus/test";

import { createBufferReader, createBufferWriter } from "../../../src/core/buffer";
import { createStreamClient } from "../../../src/domains/stream/client";
import {
  MSG_STREAM_NOTIFY,
  MSG_STREAM_READ,
  MSG_STREAM_SUBSCRIBE,
  MSG_STREAM_UNSUBSCRIBE,
} from "../../../src/frame/types";

type Handler = (payload: Uint8Array) => void;

class FakeStreamConsumerConnection {
  readonly requests: Array<{ messageType: number; payload: Uint8Array }> = [];
  readonly handlers = new Map<number, Handler>();
  readonly reconnectListeners = new Set<() => void | Promise<void>>();
  readResponses: Uint8Array[] = [];
  unsubscribeCount = 0;

  async request(messageType: number, payload: Uint8Array): Promise<Uint8Array> {
    this.requests.push({ messageType, payload });
    if (messageType === MSG_STREAM_SUBSCRIBE) {
      return encodeStreamSubscribeResponse(9n);
    }
    if (messageType === MSG_STREAM_UNSUBSCRIBE) {
      this.unsubscribeCount += 1;
      return new Uint8Array([0]);
    }
    if (messageType === MSG_STREAM_READ) {
      return this.readResponses.shift() ?? encodeWrappedReadResponse([], 0n, false);
    }
    throw new Error(`unexpected message type ${messageType}`);
  }

  registerNotificationHandler(messageType: number, handler: Handler): void {
    this.handlers.set(messageType, handler);
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }

  onReconnect(listener: () => void | Promise<void>): () => void {
    this.reconnectListeners.add(listener);
    return () => {
      this.reconnectListeners.delete(listener);
    };
  }

  onDisconnect(): () => void {
    return () => undefined;
  }

  notify(): void {
    this.handlers.get(MSG_STREAM_NOTIFY)?.(
      encodeStreamNotification(9n, "stream://realm/area/resource"),
    );
  }

  async reconnect(): Promise<void> {
    for (const listener of this.reconnectListeners) {
      await listener();
    }
  }
}

describe("StreamClient readWhenCommitted", () => {
  it("yields already committed records before waiting for notifications", async () => {
    const connection = new FakeStreamConsumerConnection();
    connection.readResponses.push(
      encodeWrappedReadResponse([encodeReadEvent(4n, new Uint8Array([1]))], 4n, false),
    );
    const client = createStreamClient(connection);

    const result = await client
      .readWhenCommitted("stream://realm/area/resource", { offset: 4n, batchSize: 10 })
      [Symbol.asyncIterator]()
      .next();

    expect(result.done).toBe(false);
    expect(result.value?.map((record: { offset: bigint }) => record.offset)).toEqual([4n]);
  });

  it("wakes on commit notifications and advances across filtered marker pages", async () => {
    const connection = new FakeStreamConsumerConnection();
    connection.readResponses.push(
      encodeWrappedReadResponse([encodeReadFiltered(5n)], 5n, false),
      encodeWrappedReadResponse([encodeReadEvent(6n, new Uint8Array([2]))], 6n, false),
    );
    const client = createStreamClient(connection);
    const iterator = client
      .readWhenCommitted("stream://realm/area/resource", { offset: 4n })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(readOffsets(connection)).toEqual([4n]);
    });

    connection.notify();

    const result = await pending;
    expect(result.done).toBe(false);
    expect(result.value?.map((record: { offset: bigint }) => record.offset)).toEqual([6n]);
    expect(readOffsets(connection)).toEqual([4n, 6n]);

    await iterator.return?.();
    expect(connection.unsubscribeCount).toBe(1);
  });

  it("continues immediately while read pages report more data", async () => {
    const connection = new FakeStreamConsumerConnection();
    connection.readResponses.push(
      encodeWrappedReadResponse([encodeReadFiltered(5n)], 5n, true),
      encodeWrappedReadResponse([encodeReadEvent(6n, new Uint8Array([3]))], 6n, false),
    );
    const client = createStreamClient(connection);

    const result = await client
      .readWhenCommitted("stream://realm/area/resource", { offset: 4n })
      [Symbol.asyncIterator]()
      .next();

    expect(result.done).toBe(false);
    expect(result.value?.map((record: { offset: bigint }) => record.offset)).toEqual([6n]);
    expect(readOffsets(connection)).toEqual([4n, 6n]);
  });

  it("reads again after reconnect even without a stream notification", async () => {
    const connection = new FakeStreamConsumerConnection();
    connection.readResponses.push(
      encodeWrappedReadResponse([], 3n, false),
      encodeWrappedReadResponse([encodeReadEvent(4n, new Uint8Array([4]))], 4n, false),
    );
    const client = createStreamClient(connection);
    const iterator = client
      .readWhenCommitted("stream://realm/area/resource", { offset: 4n, batchSize: 10 })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(readOffsets(connection)).toEqual([4n]);
    });

    await connection.reconnect();

    const result = await pending;
    expect(result.done).toBe(false);
    expect(result.value?.map((record: { offset: bigint }) => record.offset)).toEqual([4n]);
    expect(readOffsets(connection)).toEqual([4n, 4n]);

    await iterator.return?.();
    expect(connection.unsubscribeCount).toBe(1);
  });

  it("unsubscribes when readWhenCommitted is aborted while waiting", async () => {
    const connection = new FakeStreamConsumerConnection();
    connection.readResponses.push(encodeWrappedReadResponse([], 3n, false));
    const client = createStreamClient(connection);
    const controller = new AbortController();
    const iterator = client
      .readWhenCommitted("stream://realm/area/resource", {
        offset: 4n,
        signal: controller.signal,
      })
      [Symbol.asyncIterator]();

    const pending = iterator.next();
    await vi.waitFor(() => {
      expect(readOffsets(connection)).toEqual([4n]);
    });
    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(connection.unsubscribeCount).toBe(1);
  });
});

function readOffsets(connection: FakeStreamConsumerConnection): bigint[] {
  return connection.requests
    .filter((call) => call.messageType === MSG_STREAM_READ)
    .map((call) => {
      const reader = createBufferReader(call.payload);
      reader.readRoute();
      return reader.readU64BE();
    });
}

function encodeStreamSubscribeResponse(subId: bigint): Uint8Array {
  const payload = new Uint8Array(10);
  payload[0] = 0;
  payload[1] = 1;
  new DataView(payload.buffer).setBigUint64(2, subId, false);
  return payload;
}

function encodeWrappedReadResponse(
  items: Uint8Array[],
  lastOffset: bigint,
  hasMore: boolean,
): Uint8Array {
  const data = createBufferWriter(128);
  data.writeU32BE(items.length);
  for (const item of items) {
    data.writeBytes(item);
  }
  data.writeU64BE(lastOffset);
  data.writeU8(0);
  data.writeU8(0);
  data.writeU8(hasMore ? 1 : 0);

  const writer = createBufferWriter(160);
  writer.writeU8(0);
  writer.writeU8(0);
  writer.writeU32BE(data.getLength());
  writer.writeBytes(data.getBuffer());
  return writer.getBuffer();
}

function encodeReadEvent(offset: bigint, body: Uint8Array): Uint8Array {
  const writer = createBufferWriter(64);
  writer.writeU8(0);
  writer.writeU64BE(offset);
  writer.writeU8(0);
  writer.writeU8(0);
  writer.writeU32BE(body.length);
  writer.writeBytes(body);
  writer.writeU8(0);
  writer.writeU64BE(99n);
  return writer.getBuffer();
}

function encodeReadFiltered(offset: bigint): Uint8Array {
  const writer = createBufferWriter(16);
  writer.writeU8(1);
  writer.writeU64BE(offset);
  writer.writeU8(1);
  return writer.getBuffer();
}

function encodeStreamNotification(subId: bigint, route: string): Uint8Array {
  const writer = createBufferWriter(96);
  writer.writeU64BE(subId);
  writer.writeRoute(route);
  writer.writeU32BE(0);
  return writer.getBuffer();
}
