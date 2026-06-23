/**
 * Notice domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/notice/protocol.go
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import { SubscribeResponse, UnsubscribeResponse } from "./types";

export const NoticeCodec = {
  /**
   * Encode PUBLISH request (fire-and-forget, no response)
   * Payload: [string route][bytes body]
   */
  encodePublish(route: string, body: Uint8Array): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU32BE(body.length);
    writer.writeBytes(body);
    return writer.getBufferView();
  },

  /**
   * Encode SUBSCRIBE request
   * Payload: [string pattern]
   */
  encodeSubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(64);
    writer.writeRoute(pattern);
    return writer.getBufferView();
  },

  /**
   * Decode SUBSCRIBE response
   * Standard response: [u8 status=0][u8 has_sub_id][u64 sub_id if has=1]
   */
  decodeSubscribeResponse(payload: Uint8Array): SubscribeResponse {
    if (payload.length < 2) {
      throw new Error("SUBSCRIBE response too short");
    }

    const reader = new BufferReader(payload);
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
    const writer = new BufferWriter(64);
    writer.writeU64BE(subId);
    return writer.getBufferView();
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
    const reader = new BufferReader(payload);
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
