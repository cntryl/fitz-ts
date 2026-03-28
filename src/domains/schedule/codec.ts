/**
 * Schedule domain codec for encoding/decoding messages
 * Per fitz-go/internal/domains/schedule
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import {
  DecodedScheduleNotification,
  ScheduleEntry,
  ScheduleCreateResponse,
  ScheduleCancelResponse,
  ScheduleListResponse,
  ScheduleSubscribeResponse,
  ScheduleUnsubscribeResponse,
} from "./types";

export class ScheduleCodec {
  /**
   * Encode CREATE request
   * Payload: [route: string][cron: string][payload: bytes]
   */
  static encodeCreate(route: string, cronExpr: string, payload: Uint8Array): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeRoute(route);
    writer.writeString(cronExpr);
    writer.writeU32BE(payload.length);
    writer.writeBytes(payload);
    return writer.getBuffer();
  }

  /**
   * Decode CREATE response
   * Success payload: [optional has_schedule_id: u8][schedule_id: string if has=1]
   */
  static decodeCreateResponse(data: Uint8Array): ScheduleCreateResponse {
    const reader = new BufferReader(data);
    let scheduleId: string | undefined;
    if (!reader.isEOF() && reader.readU8() === 1) {
      scheduleId = reader.readString();
    }

    return { scheduleId };
  }

  /**
   * Encode CANCEL request
   * Payload: [route: string]
   */
  static encodeCancel(route: string): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    return writer.getBuffer();
  }

  /**
   * Decode CANCEL response
   * Success payload: empty
   */
  static decodeCancelResponse(_data: Uint8Array): ScheduleCancelResponse {
    return {};
  }

  /**
   * Encode LIST request
   * Payload: [optional offset: u64][optional limit: u64]
   */
  static encodeList(offset: bigint = 0n, limit: bigint = 0n): Uint8Array {
    const writer = new BufferWriter(32);
    writer.writeOptionalU64(offset);
    writer.writeOptionalU64(limit);
    return writer.getBuffer();
  }

  /**
   * Decode LIST response
   * Success payload: [total_count: u64][has_entry: u8]...[route: string][cron: string][payload: bytes when has_entry=1]
   */
  static decodeListResponse(data: Uint8Array): ScheduleListResponse {
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
  }

  /**
   * Encode SUBSCRIBE request
   * Payload: [pattern: string]
   */
  static encodeSubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeString(pattern);
    return writer.getBuffer();
  }

  /**
   * Decode SUBSCRIBE response
   * Success payload: [has_sub_id: u8][sub_id: u64 if has=1]
   */
  static decodeSubscribeResponse(data: Uint8Array): ScheduleSubscribeResponse {
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
  }

  /**
   * Encode UNSUBSCRIBE request
   * Payload: [pattern: string]
   */
  static encodeUnsubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeString(pattern);
    return writer.getBuffer();
  }

  /**
   * Decode UNSUBSCRIBE response
   * Success payload: empty
   */
  static decodeUnsubscribeResponse(_data: Uint8Array): ScheduleUnsubscribeResponse {
    return {};
  }

  /**
   * Decode NOTIFY notification (MSG_SCHEDULE_NOTIFY 705)
   * Payload: [subscription_id: u64][payload: bytes]
   */
  static decodeNotification(payload: Uint8Array): DecodedScheduleNotification {
    if (payload.length < 12) {
      throw new Error("SCHEDULE_NOTIFY payload too short");
    }

    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const payloadLength = reader.readU32BE();
    if (reader.remainingBytes() < payloadLength) {
      throw new Error("SCHEDULE_NOTIFY payload truncated");
    }

    return { subId, payload: reader.readBytes(payloadLength) };
  }
}
