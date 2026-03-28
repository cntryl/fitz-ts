/**
 * Queue domain codec for encoding and decoding protocol messages.
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import {
  QueueEnqueueResponse,
  QueueReserveResponse,
  QueueCompleteResponse,
  QueueExtendResponse,
  QueueSubscribeResponse,
  QueueUnsubscribeResponse,
  EnqueueOptions,
} from "./types";

export class QueueCodec {
  /**
   * Encode ENQUEUE request.
   * Payload: [route: string][body_len: u32][body: bytes][has_delay: u8][delay_seconds: u64 if has_delay]
   */
  static encodeEnqueue(route: string, body: Uint8Array, options?: EnqueueOptions): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeRoute(route);
    writer.writeU32BE(body.length);
    writer.writeBytes(body);

    const delaySeconds = options?.delayMs ? Math.floor(options.delayMs / 1000) : 0;
    const hasDelay = delaySeconds > 0 ? 1 : 0;
    writer.writeU8(hasDelay);
    if (hasDelay) {
      writer.writeU64BE(BigInt(delaySeconds));
    }

    return writer.getBuffer();
  }

  /**
   * Decode ENQUEUE response.
   * Payload: [status: u8][message_id: u64]
   */
  static decodeEnqueueResponse(payload: Uint8Array): QueueEnqueueResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    let messageId: bigint | undefined;
    if (!reader.isEOF()) {
      messageId = reader.readU64BE();
    }
    return { status, messageId };
  }

  /**
   * Encode RESERVE request.
   * Payload: [route: string][lease_seconds: u64][has_batch_size: u8][batch_size: u32][has_wait_seconds: u8][wait_seconds: u64]
   */
  static encodeReserve(
    route: string,
    leaseSeconds: number,
    batchSize?: number,
    waitSeconds?: number,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU64BE(BigInt(leaseSeconds));

    const hasBatchSize = batchSize !== undefined && batchSize > 0 ? 1 : 0;
    writer.writeU8(hasBatchSize);
    if (hasBatchSize && batchSize !== undefined) {
      writer.writeU32BE(batchSize);
    }

    const hasWaitSeconds = waitSeconds !== undefined && waitSeconds > 0 ? 1 : 0;
    writer.writeU8(hasWaitSeconds);
    if (hasWaitSeconds && waitSeconds !== undefined) {
      writer.writeU64BE(BigInt(waitSeconds));
    }

    return writer.getBuffer();
  }

  /**
   * Decode RESERVE response.
   * Payload: [status: u8][lease_count: u32]([message_id: u64][lease_token: u64][body_len: u32][body: bytes] ...)
   */
  static decodeReserveResponse(payload: Uint8Array): QueueReserveResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    if (reader.isEOF()) {
      return { status, items: [] };
    }

    const leaseCount = reader.readU32BE();
    const items: Array<{ id: bigint; token: bigint; body: Uint8Array }> = [];

    for (let i = 0; i < leaseCount; i++) {
      const id = reader.readU64BE();
      const token = reader.readU64BE();
      const bodyLen = reader.readU32BE();
      const body = reader.readBytes(bodyLen);

      items.push({ id, token, body });
    }

    return { status, items };
  }

  /**
   * Encode COMPLETE request.
   * Payload: [route: string][message_id: u64][lease_token: u64]
   */
  static encodeComplete(route: string, messageId: bigint, leaseToken: bigint): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    writer.writeU64BE(messageId);
    writer.writeU64BE(leaseToken);
    return writer.getBuffer();
  }

  /**
   * Decode COMPLETE response.
   * Payload: [status: u8]
   */
  static decodeCompleteResponse(payload: Uint8Array): QueueCompleteResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode EXTEND request.
   * Payload: [route: string][message_id: u64][lease_token: u64][lease_seconds: u64]
   */
  static encodeExtend(
    route: string,
    messageId: bigint,
    leaseToken: bigint,
    leaseSeconds: number,
  ): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(route);
    writer.writeU64BE(messageId);
    writer.writeU64BE(leaseToken);
    writer.writeU64BE(BigInt(leaseSeconds));
    return writer.getBuffer();
  }

  /**
   * Decode EXTEND response.
   * Payload: [status: u8]
   */
  static decodeExtendResponse(payload: Uint8Array): QueueExtendResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Encode SUBSCRIBE request.
   * Payload: [pattern: string]
   */
  static encodeSubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(pattern);
    return writer.getBuffer();
  }

  /**
   * Decode SUBSCRIBE response.
   * Payload: [status: u8][sub_id: u64]
   */
  static decodeSubscribeResponse(payload: Uint8Array): QueueSubscribeResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    if (status !== 0 || reader.isEOF()) {
      return { status };
    }

    const hasSubId = reader.readU8();
    if (hasSubId !== 1 || reader.isEOF()) {
      return { status };
    }

    return { status, subId: reader.readU64BE() };
  }

  /**
   * Encode UNSUBSCRIBE request.
   * Payload: [pattern: string]
   */
  static encodeUnsubscribe(pattern: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeRoute(pattern);
    return writer.getBuffer();
  }

  /**
   * Decode UNSUBSCRIBE response.
   * Payload: [status: u8]
   */
  static decodeUnsubscribeResponse(payload: Uint8Array): QueueUnsubscribeResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    return { status };
  }

  /**
   * Decode notification payload.
   * Payload: [sub_id: u64][route: string]
   */
  static decodeNotification(payload: Uint8Array): {
    subId: bigint;
    route: string;
  } {
    const reader = new BufferReader(payload);
    const subId = reader.readU64BE();
    const route = reader.readRoute();
    if (!reader.isEOF()) {
      const payloadLen = reader.readU32BE();
      reader.readBytes(payloadLen);
    }
    return { subId, route };
  }
}
