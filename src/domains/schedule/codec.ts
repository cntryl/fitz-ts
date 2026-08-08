/**
 * Schedule domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/schedule
 */

import {
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
  ScheduleSubscribeResponse,
  ScheduleUnsubscribeResponse,
} from "./types";

export const ScheduleCodec = {
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
      entries.push({ id: route, route, cron, deliveryMode, payload });
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
