/**
 * Notice domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/notice/protocol.go
 */

import {
  createBufferReader,
  getRouteEncoding,
  writeU32BEAt,
  writeU64BEAt,
} from "../../core/buffer";
import { SubscribeResponse, UnsubscribeResponse } from "./types";

export const NoticeCodec = {
  /**
   * Encode PUBLISH request (fire-and-forget, no response)
   * Payload: [string route][bytes body]
   */
  encodePublish(route: string, body: Uint8Array): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const buffer = new Uint8Array(routeBytes.length + 4 + body.length);
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    offset = writeU32BEAt(buffer, offset, body.length);
    buffer.set(body, offset);
    return buffer;
  },

  /**
   * Encode SUBSCRIBE request
   * Payload: [string pattern]
   */
  encodeSubscribe(pattern: string): Uint8Array {
    return getRouteEncoding(pattern).slice();
  },

  /**
   * Decode SUBSCRIBE response
   * Standard response: [u8 status=0][u8 has_sub_id][u64 sub_id if has=1]
   */
  decodeSubscribeResponse(payload: Uint8Array): SubscribeResponse {
    if (payload.length < 2) {
      throw new Error("SUBSCRIBE response too short");
    }

    const reader = createBufferReader(payload);
    const status = reader.readU8();
    const hasSubId = reader.readU8();

    if (hasSubId !== 1) {
      throw new Error("SUBSCRIBE response missing subscription_id");
    }

    if (reader.remainingBytes() < 8) {
      throw new Error("SUBSCRIBE response too short for subscription_id");
    }

    const subId = reader.readU64BE();
    return { status, subId };
  },

  /**
   * Encode UNSUBSCRIBE request
   * Payload: [u64 subscription_id]
   */
  encodeUnsubscribe(subId: bigint): Uint8Array {
    const buffer = new Uint8Array(8);
    writeU64BEAt(buffer, 0, subId);
    return buffer;
  },

  /**
   * Decode UNSUBSCRIBE response
   * Standard response: [u8 status=0]
   */
  decodeUnsubscribeResponse(): UnsubscribeResponse {
    return { status: 0 };
  },

  /**
   * Decode NOTIFY (504) message
   * Payload: [u64 subscription_id][string route][bytes body]
   */
  decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
    body: Uint8Array;
  } {
    const reader = createBufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();
    const bodyLen = reader.readU32BE();
    const body = reader.readBytes(bodyLen);
    if (!reader.isEOF()) {
      throw new Error("NOTICE_NOTIFY payload has trailing bytes");
    }

    return { subId, route, body };
  },
};
