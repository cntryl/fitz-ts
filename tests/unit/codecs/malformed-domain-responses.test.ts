import { describe, expect, it } from "vite-plus/test";

import { KvCodec } from "../../../src/domains/kv/codec";
import { LeaseCodec } from "../../../src/domains/lease/codec";
import { QueueCodec } from "../../../src/domains/queue/codec";
import { RpcCodec } from "../../../src/domains/rpc/codec";
import { ScheduleCodec } from "../../../src/domains/schedule/codec";
import { StreamCodec } from "../../../src/domains/stream/codec";

describe("malformed domain responses", () => {
  it.each([
    ["kv", () => KvCodec.decodeBeginResponse(new Uint8Array()), "Buffer overflow", undefined],
    [
      "queue",
      () =>
        QueueCodec.decodeReserveResponse(
          new Uint8Array([0, 0, 0, 0, 1]),
          "queue://realm/area/resource",
        ),
      "Buffer overflow",
      undefined,
    ],
    [
      "rpc",
      () => RpcCodec.decodeRequestResponse(new Uint8Array()),
      "Empty REQUEST response",
      "PROTOCOL_ERROR",
    ],
    [
      "lease",
      () => LeaseCodec.decodeAcquireResponse(new Uint8Array()),
      "ACQUIRE response too short",
      "PROTOCOL_ERROR",
    ],
    [
      "stream",
      () => StreamCodec.decodeBeginResponse(new Uint8Array()),
      "Buffer overflow",
      undefined,
    ],
    [
      "schedule",
      () => ScheduleCodec.decodeListPage(new Uint8Array()),
      "Buffer overflow",
      undefined,
    ],
  ])(
    "should reject malformed %s responses given missing or truncated fields when parsing",
    (_domain, decode, message, code) => {
      let caught: unknown;
      try {
        decode();
      } catch (error) {
        caught = error;
      }

      expect(caught).toMatchObject({
        message: expect.stringContaining(message),
        ...(code === undefined ? {} : { code }),
      });
    },
  );
});
