import { describe, expect, it } from "vite-plus/test";

import { BufferReader, BufferWriter } from "../../../src/core/buffer";
import { CodecError } from "../../../src/core/errors";
import { FrameCodec, FrameParser } from "../../../src/frame/codec";
import {
  MSG_CONNECT,
  MSG_KV_BEGIN,
  MSG_KV_COMMIT,
  MSG_KV_ROLLBACK,
  MSG_KV_GET,
  MSG_KV_PUT,
  MSG_KV_INSERT,
  MSG_KV_DELETE,
  MSG_KV_DELETE_RANGE,
  MSG_KV_SCAN,
  MSG_QUEUE_ENQUEUE,
  MSG_QUEUE_RESERVE,
  MSG_QUEUE_EXTEND,
  MSG_QUEUE_COMPLETE,
  MSG_QUEUE_SUBSCRIBE,
  MSG_QUEUE_UNSUBSCRIBE,
  MSG_QUEUE_NOTIFY,
  MSG_RPC_SUBSCRIBE_WORKER,
  MSG_RPC_UNSUBSCRIBE_WORKER,
  MSG_RPC_REQUEST,
  MSG_RPC_RESPONSE,
  MSG_RPC_ACK,
  MSG_LEASE_ACQUIRE,
  MSG_LEASE_RENEW,
  MSG_LEASE_RELEASE,
  MSG_LEASE_QUERY,
  MSG_LEASE_SUBSCRIBE,
  MSG_LEASE_UNSUBSCRIBE,
  MSG_LEASE_NOTIFY,
  MSG_NOTICE_PUBLISH,
  MSG_NOTICE_SUBSCRIBE,
  MSG_NOTICE_UNSUBSCRIBE,
  MSG_NOTICE_UNSUBSCRIBE_ALL,
  MSG_NOTICE_NOTIFY,
  MSG_STREAM_BEGIN,
  MSG_STREAM_APPEND,
  MSG_STREAM_COMMIT,
  MSG_STREAM_ROLLBACK,
  MSG_STREAM_READ,
  MSG_STREAM_LAST,
  MSG_STREAM_GET_METADATA,
  MSG_STREAM_SUBSCRIBE,
  MSG_STREAM_UNSUBSCRIBE,
  MSG_STREAM_NOTIFY,
  MSG_SCHEDULE_CREATE,
  MSG_SCHEDULE_CANCEL,
  MSG_SCHEDULE_LIST,
  MSG_SCHEDULE_SUBSCRIBE,
  MSG_SCHEDULE_UNSUBSCRIBE,
  MSG_SCHEDULE_NOTIFY,
} from "../../../src/frame/types";

describe("protocol primitives", () => {
  it("encodes and decodes short message-type frames", () => {
    const payload = new Uint8Array([1, 2, 3]);

    const encoded = FrameCodec.encodeFrame(MSG_CONNECT, payload);
    const decoded = FrameCodec.decodeFrame(encoded);

    expect(decoded).toEqual({ messageType: MSG_CONNECT, payload });
  });

  it("encodes and decodes escaped message-type frames", () => {
    const payload = new Uint8Array([9, 8, 7]);

    const encoded = FrameCodec.encodeFrame(MSG_SCHEDULE_NOTIFY, payload);
    expect(encoded[0]).toBe(0xff);

    const decoded = FrameCodec.decodeFrame(encoded);
    expect(decoded).toEqual({ messageType: MSG_SCHEDULE_NOTIFY, payload });
  });

  it("rejects truncated frames", () => {
    expect(() =>
      FrameCodec.decodeFrame(new Uint8Array([MSG_CONNECT, 0x00, 0x02, 0x01])),
    ).toThrowError(CodecError);
  });

  it("rejects frames larger than the u16 payload length field", () => {
    expect(() => FrameCodec.encodeFrame(MSG_CONNECT, new Uint8Array(65536))).toThrowError(
      CodecError,
    );
  });

  it("rejects message types larger than the escaped u16 field", () => {
    expect(() => FrameCodec.encodeFrame(65536, new Uint8Array())).toThrowError(CodecError);
  });

  it("parses streamed frames across chunk boundaries", () => {
    const parser = new FrameParser();
    const first = FrameCodec.encodeFrame(MSG_KV_BEGIN, new Uint8Array([1, 2]));
    const second = FrameCodec.encodeFrame(MSG_QUEUE_NOTIFY, new Uint8Array([3, 4, 5]));
    const combined = new Uint8Array(first.length + second.length);
    combined.set(first);
    combined.set(second, first.length);

    const chunkA = combined.slice(0, 4);
    const chunkB = combined.slice(4);

    expect(parser.parseFrames(chunkA)).toEqual([]);
    expect(parser.parseFrames(chunkB)).toEqual([
      { messageType: MSG_KV_BEGIN, payload: new Uint8Array([1, 2]) },
      { messageType: MSG_QUEUE_NOTIFY, payload: new Uint8Array([3, 4, 5]) },
    ]);
  });

  it("parses frames with message type 0", () => {
    const parser = new FrameParser();
    const encoded = FrameCodec.encodeFrame(0, new Uint8Array([7, 8, 9]));
    const parsed = parser.parseFrames(encoded);

    expect(parsed).toEqual([{ messageType: 0, payload: new Uint8Array([7, 8, 9]) }]);
  });

  it("parses escaped message type when header arrives one byte at a time", () => {
    const parser = new FrameParser();
    const payload = new Uint8Array([4, 5, 6]);
    const encoded = FrameCodec.encodeFrame(MSG_SCHEDULE_NOTIFY, payload);

    for (let index = 0; index < encoded.length - 1; index += 1) {
      expect(parser.parseFrames(encoded.slice(index, index + 1))).toEqual([]);
    }

    expect(parser.parseFrames(encoded.slice(encoded.length - 1))).toEqual([
      { messageType: MSG_SCHEDULE_NOTIFY, payload },
    ]);
  });

  it("round-trips primitive buffer types and optionals", () => {
    const writer = new BufferWriter();
    writer.writeU8(7);
    writer.writeU16BE(0x1234);
    writer.writeU32BE(0x12345678);
    writer.writeU64BE(0x1234567890abcdefn);
    writer.writeString("hello");
    writer.writeBytes(new Uint8Array([1, 2, 3]));
    writer.writeOptionalU64(99n);
    writer.writeOptionalU64(undefined);
    writer.writeOptionalString("fitz");
    writer.writeOptionalString(undefined);
    writer.writeOptionalBytes(new Uint8Array([4, 5]));
    writer.writeOptionalBytes(undefined);

    const reader = new BufferReader(writer.getBuffer());
    expect(reader.readU8()).toBe(7);
    expect(reader.readU16BE()).toBe(0x1234);
    expect(reader.readU32BE()).toBe(0x12345678);
    expect(reader.readU64BE()).toBe(0x1234567890abcdefn);
    expect(reader.readString()).toBe("hello");
    expect(reader.readBytes(3)).toEqual(new Uint8Array([1, 2, 3]));
    expect(reader.readOptionalU64()).toBe(99n);
    expect(reader.readOptionalU64()).toBeUndefined();
    expect(reader.readOptionalString()).toBe("fitz");
    expect(reader.readOptionalString()).toBeUndefined();
    expect(reader.readOptionalBytes()).toEqual(new Uint8Array([4, 5]));
    expect(reader.readOptionalBytes()).toBeUndefined();
  });

  it("rejects truncated primitive reads", () => {
    const reader = new BufferReader(new Uint8Array([0x12]));

    expect(() => reader.readU16BE()).toThrow("Buffer overflow");
  });

  it("rejects truncated string contents", () => {
    const reader = new BufferReader(new Uint8Array([0x00, 0x00, 0x00, 0x05, 0x68, 0x69]));

    expect(() => reader.readString()).toThrow("Buffer overflow");
  });

  it("keeps message constants aligned with the canonical registry", () => {
    expect({
      MSG_CONNECT,
      MSG_KV_BEGIN,
      MSG_KV_COMMIT,
      MSG_KV_ROLLBACK,
      MSG_KV_GET,
      MSG_KV_PUT,
      MSG_KV_INSERT,
      MSG_KV_DELETE,
      MSG_KV_DELETE_RANGE,
      MSG_KV_SCAN,
      MSG_QUEUE_ENQUEUE,
      MSG_QUEUE_RESERVE,
      MSG_QUEUE_EXTEND,
      MSG_QUEUE_COMPLETE,
      MSG_QUEUE_SUBSCRIBE,
      MSG_QUEUE_UNSUBSCRIBE,
      MSG_QUEUE_NOTIFY,
      MSG_RPC_SUBSCRIBE_WORKER,
      MSG_RPC_UNSUBSCRIBE_WORKER,
      MSG_RPC_REQUEST,
      MSG_RPC_RESPONSE,
      MSG_RPC_ACK,
      MSG_LEASE_ACQUIRE,
      MSG_LEASE_RENEW,
      MSG_LEASE_RELEASE,
      MSG_LEASE_QUERY,
      MSG_LEASE_SUBSCRIBE,
      MSG_LEASE_UNSUBSCRIBE,
      MSG_LEASE_NOTIFY,
      MSG_NOTICE_PUBLISH,
      MSG_NOTICE_SUBSCRIBE,
      MSG_NOTICE_UNSUBSCRIBE,
      MSG_NOTICE_UNSUBSCRIBE_ALL,
      MSG_NOTICE_NOTIFY,
      MSG_STREAM_BEGIN,
      MSG_STREAM_APPEND,
      MSG_STREAM_COMMIT,
      MSG_STREAM_ROLLBACK,
      MSG_STREAM_READ,
      MSG_STREAM_LAST,
      MSG_STREAM_GET_METADATA,
      MSG_STREAM_SUBSCRIBE,
      MSG_STREAM_UNSUBSCRIBE,
      MSG_STREAM_NOTIFY,
      MSG_SCHEDULE_CREATE,
      MSG_SCHEDULE_CANCEL,
      MSG_SCHEDULE_LIST,
      MSG_SCHEDULE_SUBSCRIBE,
      MSG_SCHEDULE_UNSUBSCRIBE,
      MSG_SCHEDULE_NOTIFY,
    }).toEqual({
      MSG_CONNECT: 1,
      MSG_KV_BEGIN: 100,
      MSG_KV_COMMIT: 101,
      MSG_KV_ROLLBACK: 102,
      MSG_KV_GET: 103,
      MSG_KV_PUT: 104,
      MSG_KV_INSERT: 105,
      MSG_KV_DELETE: 106,
      MSG_KV_DELETE_RANGE: 107,
      MSG_KV_SCAN: 108,
      MSG_QUEUE_ENQUEUE: 200,
      MSG_QUEUE_RESERVE: 202,
      MSG_QUEUE_EXTEND: 203,
      MSG_QUEUE_COMPLETE: 204,
      MSG_QUEUE_SUBSCRIBE: 207,
      MSG_QUEUE_UNSUBSCRIBE: 208,
      MSG_QUEUE_NOTIFY: 209,
      MSG_RPC_SUBSCRIBE_WORKER: 300,
      MSG_RPC_UNSUBSCRIBE_WORKER: 301,
      MSG_RPC_REQUEST: 302,
      MSG_RPC_RESPONSE: 303,
      MSG_RPC_ACK: 304,
      MSG_LEASE_ACQUIRE: 400,
      MSG_LEASE_RENEW: 401,
      MSG_LEASE_RELEASE: 402,
      MSG_LEASE_QUERY: 403,
      MSG_LEASE_SUBSCRIBE: 407,
      MSG_LEASE_UNSUBSCRIBE: 408,
      MSG_LEASE_NOTIFY: 409,
      MSG_NOTICE_PUBLISH: 500,
      MSG_NOTICE_SUBSCRIBE: 501,
      MSG_NOTICE_UNSUBSCRIBE: 502,
      MSG_NOTICE_UNSUBSCRIBE_ALL: 503,
      MSG_NOTICE_NOTIFY: 504,
      MSG_STREAM_BEGIN: 600,
      MSG_STREAM_APPEND: 601,
      MSG_STREAM_COMMIT: 602,
      MSG_STREAM_ROLLBACK: 603,
      MSG_STREAM_READ: 604,
      MSG_STREAM_LAST: 605,
      MSG_STREAM_GET_METADATA: 606,
      MSG_STREAM_SUBSCRIBE: 607,
      MSG_STREAM_UNSUBSCRIBE: 608,
      MSG_STREAM_NOTIFY: 609,
      MSG_SCHEDULE_CREATE: 700,
      MSG_SCHEDULE_CANCEL: 701,
      MSG_SCHEDULE_LIST: 702,
      MSG_SCHEDULE_SUBSCRIBE: 703,
      MSG_SCHEDULE_UNSUBSCRIBE: 704,
      MSG_SCHEDULE_NOTIFY: 705,
    });
  });
});
