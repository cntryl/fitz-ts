import { describe, it, expect } from "vite-plus/test";
import { createQueueItem } from "../../../src/domains/queue/types";
import { isRetryable, QueueError, TimeoutError } from "../../../src/core/errors";
import { QueueCodec } from "../../../src/domains/queue/codec";
import { createBufferWriter } from "../../../src/core/buffer";

describe("Queue ingress errors", () => {
  for (const operation of [
    "decodeCompleteResponse",
    "decodeExtendResponse",
    "decodeSubscribeResponse",
    "decodeUnsubscribeResponse",
  ] as const) {
    it(`should preserve coded rejection for ${operation}`, () => {
      // Arrange
      const writer = createBufferWriter(100);
      writer.writeU8(1);
      writer.writeU32BE(4005);
      writer.writeString("not accepted");
      // Act
      const response = QueueCodec[operation](writer.getBuffer());
      // Assert
      expect(response).toEqual({ status: 1, errorCode: 4005, errorMessage: "not accepted" });
    });
  }
});

for (const [code, retryable] of [
  [4005, true],
  [4007, false],
] as const) {
  it(`should surface completion code ${code} without automatic replay`, async () => {
    // Arrange
    const writer = createBufferWriter(100);
    writer.writeU8(1);
    writer.writeU32BE(code);
    writer.writeString("broker rejection");
    let calls = 0;
    const item = createQueueItem(7n, 11n, new Uint8Array(), "queue://prod/app/jobs", {
      request: async () => {
        calls += 1;
        return writer.getBuffer();
      },
      onDisconnect: () => () => undefined,
    });
    // Act
    const error = await item.complete().catch((failure: unknown) => failure);
    // Assert
    expect(error).toBeInstanceOf(QueueError);
    expect(error).toMatchObject({ domainCode: code });
    expect(isRetryable(error)).toBe(retryable);
    expect(calls).toBe(1);
  });
}

it("should not replay completion after an uncertain timeout", async () => {
  // Arrange
  let calls = 0;
  const item = createQueueItem(7n, 11n, new Uint8Array(), "queue://prod/app/jobs", {
    request: async () => {
      calls += 1;
      throw new TimeoutError("outcome unknown");
    },
    onDisconnect: () => () => undefined,
  });
  // Act
  const result = item.complete();
  // Assert
  await expect(result).rejects.toBeInstanceOf(TimeoutError);
  expect(calls).toBe(1);
});

it("should reject malformed capacity responses", () => {
  // Arrange
  const payload = new Uint8Array([1, 0, 0, 15, 165, 0, 0, 0, 2, 120]);
  // Act / Assert
  expect(() => QueueCodec.decodeCompleteResponse(payload)).toThrow();
});

it("should retain reservation after rejected completion until an explicit successful retry", async () => {
  // Arrange
  const writer = createBufferWriter(100);
  writer.writeU8(1);
  writer.writeU32BE(4005);
  writer.writeString("not accepted");
  const payloads: Uint8Array[] = [];
  const item = createQueueItem(7n, 11n, new Uint8Array(), "queue://prod/app/jobs", {
    request: async (_type, payload) => {
      payloads.push(payload);
      return payloads.length === 1 ? writer.getBuffer() : new Uint8Array([0]);
    },
    onDisconnect: () => () => undefined,
  });
  await expect(item.complete()).rejects.toMatchObject({ domainCode: 4005 });
  // Act
  await item.complete();
  // Assert
  expect(payloads).toHaveLength(2);
  expect(payloads[1]).toEqual(payloads[0]);
  await expect(item.complete()).rejects.toMatchObject({ code: "QUEUE_ITEM_CLOSED" });
  expect(payloads).toHaveLength(2);
});

it("should reject trailing bytes in completion acknowledgement", () => {
  // Arrange
  const payload = new Uint8Array([0, 120]);
  // Act / Assert
  expect(() => QueueCodec.decodeCompleteResponse(payload)).toThrow();
});
