/**
 * Schedule domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/schedule
 */

import {
  BufferReader,
  getRouteEncoding,
  utf8Encoder,
  writeU32BEAt,
  writeU64BEAt,
} from "../../core/buffer";
import {
  DecodedScheduleNotification,
  ScheduleEntry,
  ScheduleCreateResponse,
  ScheduleCancelResponse,
  ScheduleListResponse,
  ScheduleSubscribeResponse,
  ScheduleUnsubscribeResponse,
} from "./types";

export const ScheduleCodec = {
  /**
   * Encode CREATE request
   * Payload: [route: string][cron: string][payload: bytes]
   */
  encodeCreate(route: string, cronExpr: string, payload: Uint8Array): Uint8Array {
    const routeBytes = getRouteEncoding(route);
    const cronBytes = utf8Encoder.encode(cronExpr);
    const buffer = new Uint8Array(routeBytes.length + 4 + cronBytes.length + 4 + payload.length);
    let offset = 0;

    buffer.set(routeBytes, offset);
    offset += routeBytes.length;
    offset = writeU32BEAt(buffer, offset, cronBytes.length);
    buffer.set(cronBytes, offset);
    offset += cronBytes.length;
    offset = writeU32BEAt(buffer, offset, payload.length);
    buffer.set(payload, offset);
    return buffer;
  },

  /**
   * Decode CREATE response
   * Success payload: [optional has_schedule_id: u8][schedule_id: string if has=1]
   */
  decodeCreateResponse(data: Uint8Array): ScheduleCreateResponse {
    const reader = new BufferReader(data);
    let scheduleId: string | undefined;
    if (!reader.isEOF() && reader.readU8() === 1) {
      scheduleId = reader.readString();
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
  decodeCancelResponse(_data: Uint8Array): ScheduleCancelResponse {
    return {};
  },

  /**
   * Encode LIST request
   * Payload: [optional offset: u64][optional limit: u64]
   */
  encodeList(offset: bigint = 0n, limit: bigint = 0n): Uint8Array {
    const buffer = new Uint8Array(18);
    let bufferOffset = 0;

    buffer[bufferOffset++] = 1;
    bufferOffset = writeU64BEAt(buffer, bufferOffset, offset);
    buffer[bufferOffset++] = 1;
    writeU64BEAt(buffer, bufferOffset, limit);
    return buffer;
  },

  /**
   * Decode LIST response
   * Success payload: [total_count: u64][has_entry: u8]...[route: string][cron: string][payload: bytes when has_entry=1]
   */
  decodeListResponse(data: Uint8Array): ScheduleListResponse {
    const reader = new BufferReader(data);
    if (reader.remainingBytes() < 8) {
      throw new Error("LIST response missing total_count");
    }
    const totalCount = reader.readU64BE();
    const entries: ScheduleEntry[] = [];

    while (!reader.isEOF()) {
      const hasEntry = reader.readU8();
      if (hasEntry === 0) {
        break;
      }

      const route = reader.readString();
      const cron = reader.readString();
      const payloadBytes = reader.readBytes(reader.readU32BE());

      entries.push({
        id: route, // Route as identity
        route,
        cron,
        payload: payloadBytes,
      });
    }

    return { totalCount, entries };
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
    const reader = new BufferReader(data);
    if (reader.isEOF()) {
      throw new Error("SUBSCRIBE response missing subscription_id");
    }

    if (reader.readU8() !== 1) {
      throw new Error("SUBSCRIBE response missing subscription_id");
    }

    if (reader.remainingBytes() < 8) {
      throw new Error("SUBSCRIBE response too short for subscription_id");
    }

    return { subId: reader.readU64BE() };
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
  decodeUnsubscribeResponse(_data: Uint8Array): ScheduleUnsubscribeResponse {
    return {};
  },

  /**
   * Decode NOTIFY notification (MSG_SCHEDULE_NOTIFY 705)
   * Payload: [subscription_id: u64][payload: bytes]
   */
  decodeNotification(payload: Uint8Array): DecodedScheduleNotification {
    if (payload.length < 12) {
      throw new Error("SCHEDULE_NOTIFY payload too short");
    }

    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const payloadLength = reader.readU32BE();
    if (reader.remainingBytes() < payloadLength) {
      throw new Error("SCHEDULE_NOTIFY payload truncated");
    }

    const notificationPayload = reader.readBytes(payloadLength);
    if (!reader.isEOF()) {
      throw new Error("SCHEDULE_NOTIFY payload has trailing bytes");
    }

    return { subId, payload: notificationPayload };
  },
};
