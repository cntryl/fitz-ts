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
import { ProtocolError } from "../../core/errors";

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
   * Encode UNSUBSCRIBE request
   * Payload: [u64 subscription_id]
   */
  encodeUnsubscribe(subId: bigint): Uint8Array {
    const buffer = new Uint8Array(8);
    writeU64BEAt(buffer, 0, subId);
    return buffer;
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
      throw new ProtocolError("NOTICE_NOTIFY payload has trailing bytes", undefined, {
        operation: "NOTICE_NOTIFY",
      });
    }

    return { subId, route, body };
  },
};
