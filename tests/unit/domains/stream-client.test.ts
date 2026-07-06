import { describe, expect, it } from "vite-plus/test";

import { BufferReader, BufferWriter } from "../../../src/core/buffer";
import { ConnectionError } from "../../../src/core/errors";
import {
  MSG_STREAM_APPEND,
  MSG_STREAM_BEGIN,
  MSG_STREAM_COMMIT,
  MSG_STREAM_READ,
  MSG_STREAM_ROLLBACK,
} from "../../../src/frame/types";
import { StreamClient } from "../../../src/domains/stream/client";
import type { Connection } from "../../../src/client/connection";
import type { StreamFilterSet } from "../../../src/domains/stream/types";

class FakeStreamConnection {
  public lastSignal: AbortSignal | undefined;
  public lastPayload: Uint8Array | undefined;
  public responses = new Map<number, Uint8Array[]>();
  private disconnectListeners = new Set<() => void>();

  constructor(
    private readonly appendMode: "pending" | "success" = "pending",
    private readonly readResponse: Uint8Array = new Uint8Array([0]),
  ) {}

  async request(
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    this.lastPayload = payload;
    this.lastSignal = signal;

    if (signal?.aborted) {
      const error = new Error("The operation was aborted");
      error.name = "AbortError";
      throw error;
    }

    if (messageType === MSG_STREAM_BEGIN) {
      return new Uint8Array([0, 1, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    const queuedResponse = this.responses.get(messageType)?.shift();
    if (queuedResponse) {
      return queuedResponse;
    }

    if (messageType === MSG_STREAM_APPEND) {
      if (this.appendMode === "success") {
        return new Uint8Array([0]);
      }

      return await new Promise<Uint8Array>((_resolve, reject) => {
        const abort = () => {
          const error = new Error("The operation was aborted");
          error.name = "AbortError";
          reject(error);
        };

        signal?.addEventListener("abort", abort, { once: true });
      });
    }

    if (messageType === MSG_STREAM_READ) {
      return this.readResponse;
    }

    throw new ConnectionError("disconnected");
  }

  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => {
      this.disconnectListeners.delete(listener);
    };
  }

  onReconnect(): () => void {
    return () => undefined;
  }

  disconnect(): void {
    for (const listener of this.disconnectListeners) {
      listener();
    }
  }

  respond(messageType: number, response: Uint8Array): void {
    const responses = this.responses.get(messageType);
    if (responses) {
      responses.push(response);
      return;
    }

    this.responses.set(messageType, [response]);
  }
}

describe("StreamClient", () => {
  it("invalidates open stream sessions on disconnect", async () => {
    const connection = new FakeStreamConnection();
    const client = new StreamClient(connection as unknown as Connection);

    const session = await client.begin("stream://realm/area/resource");
    connection.disconnect();

    await expect(session.append(0n, new Uint8Array([1]))).rejects.toMatchObject({
      code: "STREAM_SESSION_CLOSED",
    });
  });

  it("cancels an in-flight stream append request", async () => {
    const connection = new FakeStreamConnection();
    const client = new StreamClient(connection as unknown as Connection);

    const session = await client.begin("stream://realm/area/resource");
    const controller = new AbortController();
    const pending = session.append(0n, new Uint8Array([1]), controller.signal);

    await Promise.resolve();
    controller.abort();

    await expect(pending).rejects.toHaveProperty("name", "AbortError");
    expect(connection.lastSignal).toBe(controller.signal);
  });

  it("encodes append discriminator options", async () => {
    const connection = new FakeStreamConnection("success");
    const client = new StreamClient(connection as unknown as Connection);

    const session = await client.begin("stream://realm/area/resource");
    const offset = await session.append(0n, new Uint8Array([1, 2]), {
      discriminator: "proj.alpha",
    });

    expect(offset).toBe(0n);
    const reader = new BufferReader(connection.lastPayload ?? new Uint8Array());
    expect(reader.readU64BE()).toBe(1n);
    expect(reader.readU64BE()).toBe(0n);
    expect(reader.readU32BE()).toBe(2);
    expect(Array.from(reader.readBytes(2))).toEqual([1, 2]);
    expect(reader.readU8()).toBe(0);
    expect(reader.readU8()).toBe(1);
    expect(reader.readString()).toBe("proj.alpha");
    expect(reader.isEOF()).toBe(true);
  });

  it("leaves a stream session usable after a failed commit", async () => {
    const connection = new FakeStreamConnection("success");
    const client = new StreamClient(connection as unknown as Connection);

    const session = await client.begin("stream://realm/area/resource");
    connection.respond(MSG_STREAM_COMMIT, new Uint8Array([7]));
    connection.respond(MSG_STREAM_ROLLBACK, new Uint8Array([0]));

    await expect(session.commit("Sync")).rejects.toMatchObject({
      domainCode: 7,
    });
    expect(session.isOpen()).toBe(true);
    await expect(session.rollback()).resolves.toBeUndefined();
    expect(session.isOpen()).toBe(false);
  });

  it("encodes read filter options", async () => {
    const connection = new FakeStreamConnection("success");
    const client = new StreamClient(connection as unknown as Connection);

    const filter: StreamFilterSet = {
      clauses: [{ kind: "Equals", value: "proj.alpha" }],
    };

    const records = await client.read("stream://realm/area/resource", 5n, 10, { filter });

    expect(records).toEqual([]);
    const reader = new BufferReader(connection.lastPayload ?? new Uint8Array());
    expect(reader.readRoute()).toBe("stream://realm/area/resource");
    expect(reader.readU64BE()).toBe(5n);
    expect(reader.readU64BE()).toBe(10n);
    expect(reader.readU8()).toBe(0);
    expect(reader.readU8()).toBe(1);
    const filterLength = reader.readU32BE();
    expect(filterLength).toBeGreaterThan(0);
    expect(reader.readBytes(filterLength)).toBeInstanceOf(Uint8Array);
    expect(reader.isEOF()).toBe(true);
  });

  it("returns read pages with filtered markers and preserves event-only read compatibility", async () => {
    const readResponse = encodeWrappedReadResponse(
      [
        encodeReadEvent({
          offset: 4n,
          areaOffset: 8n,
          realmOffset: 12n,
          body: new Uint8Array([1, 2, 3]),
          timestamp: 99n,
        }),
        encodeReadFiltered(5n, "server_filter"),
      ],
      {
        lastResourceOffset: 5n,
        lastAreaOffset: 8n,
        lastRealmOffset: 12n,
        hasMore: false,
      },
    );
    const connection = new FakeStreamConnection("success", readResponse);
    const client = new StreamClient(connection as unknown as Connection);

    const page = await client.readPage("stream://realm/area/resource", 4n, 10);
    expect(page.items).toHaveLength(2);
    expect(page.items[0]).toEqual({
      kind: "event",
      record: {
        offset: 4n,
        areaOffset: 8n,
        realmOffset: 12n,
        body: new Uint8Array([1, 2, 3]),
        timestamp: 99n,
      },
    });
    expect(page.items[1]).toEqual({
      kind: "filtered",
      offset: 5n,
      reason: "server_filter",
    });
    expect(page.cursor).toEqual({
      lastResourceOffset: 5n,
      lastAreaOffset: 8n,
      lastRealmOffset: 12n,
      hasMore: false,
    });

    const records = await client.read("stream://realm/area/resource", 4n, 10);
    expect(records).toHaveLength(1);
    expect(records[0].offset).toBe(4n);
    expect(Buffer.from(records[0].body).toString("hex")).toBe("010203");
  });
});

function encodeReadEvent(options: {
  offset: bigint;
  areaOffset?: bigint;
  realmOffset?: bigint;
  body: Uint8Array;
  metadata?: Uint8Array;
  timestamp: bigint;
}): Uint8Array {
  const writer = new BufferWriter(64);
  writer.writeU8(0);
  writer.writeU64BE(options.offset);
  writeOptionalU64(writer, options.areaOffset);
  writeOptionalU64(writer, options.realmOffset);
  writer.writeU32BE(options.body.length);
  writer.writeBytes(options.body);
  writeOptionalBytes(writer, options.metadata);
  writer.writeU64BE(options.timestamp);
  return writer.getBuffer();
}

function writeOptionalU64(writer: BufferWriter, value: bigint | undefined): void {
  if (value === undefined) {
    writer.writeU8(0);
    return;
  }

  writer.writeU8(1);
  writer.writeU64BE(value);
}

function writeOptionalBytes(writer: BufferWriter, value: Uint8Array | undefined): void {
  if (!value) {
    writer.writeU8(0);
    return;
  }

  writer.writeU8(1);
  writer.writeU32BE(value.length);
  writer.writeBytes(value);
}

function encodeReadFiltered(
  offset: bigint,
  reason?: "server_filter" | "permission" | "projection",
): Uint8Array {
  const writer = new BufferWriter(16);
  writer.writeU8(1);
  writer.writeU64BE(offset);
  writer.writeU8(
    reason === undefined ? 0 : reason === "server_filter" ? 1 : reason === "permission" ? 2 : 3,
  );
  return writer.getBuffer();
}

function encodeWrappedReadResponse(
  items: Uint8Array[],
  cursor: {
    lastResourceOffset: bigint;
    lastAreaOffset?: bigint;
    lastRealmOffset?: bigint;
    hasMore: boolean;
  },
): Uint8Array {
  const data = new BufferWriter(256);
  data.writeU32BE(items.length);
  for (const item of items) {
    data.writeBytes(item);
  }
  data.writeU64BE(cursor.lastResourceOffset);
  data.writeU8(cursor.lastAreaOffset === undefined ? 0 : 1);
  if (cursor.lastAreaOffset !== undefined) {
    data.writeU64BE(cursor.lastAreaOffset);
  }
  data.writeU8(cursor.lastRealmOffset === undefined ? 0 : 1);
  if (cursor.lastRealmOffset !== undefined) {
    data.writeU64BE(cursor.lastRealmOffset);
  }
  data.writeU8(cursor.hasMore ? 1 : 0);

  const writer = new BufferWriter(320);
  writer.writeU8(0);
  writer.writeU8(0);
  writer.writeU32BE(data.getLength());
  writer.writeBytes(data.getBuffer());
  return writer.getBuffer();
}
