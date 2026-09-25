/**
 * Schedule domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/schedule
 */

import {
  createBufferWriter,
  createBufferReader,
  getRouteEncoding,
  utf8Encoder,
  writeU32BEAt,
  writeU64BEAt,
} from "../../core/buffer";
import {
  DecodedScheduleNotification,
  ScheduleEntry,
  ScheduleDeliveryMode,
  ScheduleCreateResponse,
  ScheduleCancelResponse,
  ScheduleListPage,
  ScheduleCursorPage,
  ScheduleSubscribeResponse,
  ScheduleUnsubscribeResponse,
} from "./types";

export const ScheduleCodec = {
  encodeCreateBatch(entries: readonly ScheduleEntry[]): Uint8Array {
    if (entries.length > 0xffffffff) throw new Error("schedule batch has too many entries");
    const writer = createBufferWriter(128);
    writer.writeU32BE(entries.length);
    for (const entry of entries) {
      writer.writeBytes(
        this.encodeCreate(entry.route, entry.cron, entry.deliveryMode, entry.payload),
      );
    }
    return writer.getBuffer();
  },

  encodeListV2(cursor?: string, limit?: bigint): Uint8Array {
    if (limit !== undefined && (limit < 0n || limit > 1000n)) {
      throw new Error("schedule LIST_V2 limit must be between 0 and 1000");
    }
    const writer = createBufferWriter(64);
    writer.writeU8(cursor === undefined ? 0 : 1);
    if (cursor !== undefined) writer.writeString(cursor);
    writer.writeOptionalU64(limit);
    return writer.getBuffer();
  },

  decodeListV2(data: Uint8Array): ScheduleCursorPage {
    const reader = createBufferReader(data);
    if (reader.readU8() !== 1) throw new Error("LIST_V2 response has an invalid version");
    const hasMoreByte = reader.readU8();
    if (hasMoreByte !== 0 && hasMoreByte !== 1) {
      throw new Error("LIST_V2 response has an invalid has_more flag");
    }
    const cursorFlag = reader.readU8();
    if (cursorFlag !== 0 && cursorFlag !== 1) {
      throw new Error("LIST_V2 response has an invalid cursor flag");
    }
    const continuation = cursorFlag === 1 ? reader.readString() : undefined;
    if (hasMoreByte === 1 && continuation === undefined) {
      throw new Error("LIST_V2 response is missing continuation");
    }
    const entries: ScheduleEntry[] = [];
    while (true) {
      const marker = reader.readU8();
      if (marker === 0) break;
      if (marker !== 1) throw new Error("LIST_V2 response has an invalid entry marker");
      entries.push({
        route: reader.readString(),
        cron: reader.readString(),
        deliveryMode: decodeDeliveryMode(reader.readU8()),
        payload: reader.readBytes(reader.readU32BE()),
      });
    }
    if (!reader.isEOF()) throw new Error("LIST_V2 response has trailing bytes");
    return { entries, hasMore: hasMoreByte === 1, continuation };
  },

  /**
   * Encode CREATE request
   * Payload: [route: string][cron: string][delivery_mode: u8][payload: bytes]
   */
  encodeCreate(
    route: string,
    cronExpr: string,
    deliveryMode: ScheduleDeliveryMode,
    payload: Uint8Array,
  ): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const cronBytes = utf8Encoder.encode(cronExpr);
    const mode = encodeDeliveryMode(deliveryMode);
    const buffer = new Uint8Array(
      routeBytes.length + 4 + cronBytes.length + 1 + 4 + payload.length,
    );
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    offset = writeU32BEAt(buffer, offset, cronBytes.length);
    buffer.set(cronBytes, offset);
    offset += cronBytes.length;
    buffer[offset++] = mode;
    offset = writeU32BEAt(buffer, offset, payload.length);
    buffer.set(payload, offset);
    return buffer;
  },

  /**
   * Decode CREATE response
   * Success payload: [optional has_schedule_id: u8][schedule_id: string if has=1]
   */
  decodeCreateResponse(data: Uint8Array): ScheduleCreateResponse {
    const reader = createBufferReader(data);
    let scheduleId: string | undefined;
    if (!reader.isEOF()) {
      if (reader.readU8() !== 1) {
        throw new Error("CREATE response has invalid schedule_id flag");
      }
      scheduleId = reader.readString();
      if (!reader.isEOF()) throw new Error("CREATE response has trailing bytes");
    }

    return { scheduleId };
  },

  /**
   * Encode CANCEL request
   * Payload: [route: string]
   */
  encodeCancel(route: string): Uint8Array {
    return getRouteEncoding(route).slice();
  },

  /**
   * Decode CANCEL response
   * Success payload: empty
   */
  decodeCancelResponse(data: Uint8Array): ScheduleCancelResponse {
    if (data.length !== 0) throw new Error("CANCEL response has trailing bytes");
    return {};
  },

  encodeListPage(offsetValue?: bigint, limit?: bigint): Uint8Array {
    if (offsetValue !== undefined && offsetValue < 0n)
      throw new Error("schedule LIST offset must be non-negative");
    if (limit !== undefined && (limit < 0n || limit > 1000n))
      throw new Error("schedule LIST limit must be between 0 and 1000");
    const buffer = new Uint8Array(
      1 + (offsetValue === undefined ? 0 : 8) + 1 + (limit === undefined ? 0 : 8),
    );
    let offset = 0;
    buffer[offset++] = offsetValue === undefined ? 0 : 1;
    if (offsetValue !== undefined) offset = writeU64BEAt(buffer, offset, offsetValue);
    buffer[offset++] = limit === undefined ? 0 : 1;
    if (limit !== undefined) writeU64BEAt(buffer, offset, limit);
    return buffer;
  },

  decodeListPage(data: Uint8Array): ScheduleListPage {
    const reader = createBufferReader(data);
    const totalCount = reader.readU64BE();
    const entries: ScheduleEntry[] = [];
    while (true) {
      const sentinel = reader.readU8();
      if (sentinel === 0) break;
      if (sentinel !== 1) throw new Error("LIST_PAGE response has invalid entry sentinel");
      const route = reader.readString();
      const cron = reader.readString();
      const deliveryMode = decodeDeliveryMode(reader.readU8());
      const payload = reader.readBytes(reader.readU32BE());
      entries.push({ route, cron, deliveryMode, payload });
    }
    if (!reader.isEOF()) throw new Error("LIST_PAGE response has trailing bytes");
    return { entries, totalCount };
  },

  /**
   * Encode SUBSCRIBE request
   * Payload: [pattern: string]
   */
  encodeSubscribe(pattern: string): Uint8Array {
    return getRouteEncoding(pattern).slice();
  },

  /**
   * Decode SUBSCRIBE response
   * Success payload: [has_sub_id: u8][sub_id: u64 if has=1]
   */
  decodeSubscribeResponse(data: Uint8Array): ScheduleSubscribeResponse {
    const reader = createBufferReader(data);
    if (reader.isEOF()) {
      throw new Error("SUBSCRIBE response missing subscription_id");
    }

    if (reader.readU8() !== 1) {
      throw new Error("SUBSCRIBE response missing subscription_id");
    }

    if (reader.remainingBytes() < 8) {
      throw new Error("SUBSCRIBE response too short for subscription_id");
    }

    const subId = reader.readU64BE();
    if (!reader.isEOF()) {
      throw new Error("SUBSCRIBE response has trailing bytes");
    }
    return { subId };
  },

  /**
   * Encode UNSUBSCRIBE request
   * Payload: [pattern: string]
   */
  encodeUnsubscribe(pattern: string): Uint8Array {
    return getRouteEncoding(pattern).slice();
  },

  /**
   * Decode UNSUBSCRIBE response
   * Success payload: empty
   */
  decodeUnsubscribeResponse(data: Uint8Array): ScheduleUnsubscribeResponse {
    if (data.length !== 0) throw new Error("UNSUBSCRIBE response has trailing bytes");
    return {};
  },

  /**
   * Decode NOTIFY notification (MSG_SCHEDULE_NOTIFY 705)
   * Payload: [subscription_id: u64][exact_route: string][payload: bytes]
   */
  decodeNotification(payload: Uint8Array): DecodedScheduleNotification {
    if (payload.length < 12) {
      throw new Error("SCHEDULE_NOTIFY payload too short");
    }

    const reader = createBufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readString();
    const payloadLength = reader.readU32BE();
    if (reader.remainingBytes() < payloadLength) {
      throw new Error("SCHEDULE_NOTIFY payload truncated");
    }

    const notificationPayload = reader.readBytes(payloadLength);
    if (!reader.isEOF()) {
      throw new Error("SCHEDULE_NOTIFY payload has trailing bytes");
    }

    return { subId, route, payload: notificationPayload };
  },
};

function encodeDeliveryMode(deliveryMode: ScheduleDeliveryMode): number {
  if (deliveryMode === "Broadcast") return 0;
  if (deliveryMode === "Single") return 1;
  throw new Error(`Invalid schedule delivery mode: ${String(deliveryMode)}`);
}

function decodeDeliveryMode(value: number): ScheduleDeliveryMode {
  if (value === 0) return "Broadcast";
  if (value === 1) return "Single";
  throw new Error(`Invalid schedule delivery mode byte: ${value}`);
}
