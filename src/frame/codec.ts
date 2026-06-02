/**
 * Frame encoding/decoding for Fitz protocol
 * Format: [MessageType (variable)][Length (u16 BE)][Payload]
 * MessageType: 0-254 = single byte, 255+ = escape 0xFF + u16 BE
 */

import { CodecError } from "../core/errors";
import { BufferWriter, BufferReader } from "../core/buffer";

export interface Frame {
  messageType: number;
  payload: Uint8Array;
}

const encodeMessageType = (messageType: number, writer: BufferWriter): void => {
  if (messageType < 0) {
    throw new CodecError(`Invalid message type: ${messageType}`);
  }
  if (messageType <= 254) {
    writer.writeU8(messageType);
  } else {
    writer.writeU8(0xff);
    writer.writeU16BE(messageType);
  }
};

const decodeMessageType = (reader: BufferReader): number => {
  const firstByte = reader.readU8();
  if (firstByte === 0xff) {
    return reader.readU16BE();
  }
  return firstByte;
};

const encodeFrame = (messageType: number, payload: Uint8Array): Uint8Array => {
  const writer = BufferWriter(payload.length + 10);

  encodeMessageType(messageType, writer);
  writer.writeU16BE(payload.length);
  writer.writeBytes(payload);

  return writer.getBuffer();
};

const decodeFrame = (buffer: Uint8Array): Frame => {
  const reader = BufferReader(buffer);

  try {
    const messageType = decodeMessageType(reader);
    const length = reader.readU16BE();

    if (reader.remainingBytes() < length) {
      throw new CodecError(
        `Frame incomplete: expected ${length} bytes, got ${reader.remainingBytes()}`,
      );
    }

    const payload = reader.readBytes(length);

    return { messageType, payload };
  } catch (err) {
    if (err instanceof CodecError) {
      throw err;
    }
    throw new CodecError(
      `Failed to decode frame: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
};

const getMessageTypeSize = (messageType: number): number => {
  if (messageType <= 254) {
    return 1;
  }
  return 3;
};

const calculateFrameSize = (messageType: number, payloadLength: number): number => {
  return getMessageTypeSize(messageType) + 2 + payloadLength;
};

export const FrameCodec = {
  encodeFrame,
  decodeFrame,
  getMessageTypeSize,
  calculateFrameSize,
};

/**
 * Helper for streaming frame parsing
 */
export type FrameParser = ReturnType<typeof createFrameParser>;

export function createFrameParser() {
  let buffer: Uint8Array = new Uint8Array(0);
  let offset = 0;
  let messageType: number | null = null;
  let payloadLength: number | null = null;
  let state: "reading_type" | "reading_length" | "reading_payload" = "reading_type";

  const appendData = (data: Uint8Array): void => {
    if (data.length === 0) {
      return;
    }

    if (buffer.length === 0) {
      buffer = data;
      offset = 0;
      return;
    }

    const newBuffer = new Uint8Array(buffer.length + data.length);
    newBuffer.set(buffer);
    newBuffer.set(data, buffer.length);
    buffer = newBuffer;
    offset = 0;
  };

  const tryReadMessageType = (): boolean => {
    if (offset >= buffer.length) return false;

    const firstByte = buffer[offset];

    if (firstByte === 0xff) {
      if (offset + 3 > buffer.length) return false;

      const high = buffer[offset + 1];
      const low = buffer[offset + 2];
      messageType = (high << 8) | low;
      offset += 3;
    } else {
      messageType = firstByte;
      offset += 1;
    }

    return true;
  };

  const tryReadLength = (): boolean => {
    if (offset + 2 > buffer.length) return false;

    const high = buffer[offset];
    const low = buffer[offset + 1];
    payloadLength = (high << 8) | low;
    offset += 2;

    return true;
  };

  const tryReadPayload = (frames: Frame[]): boolean => {
    if (messageType === null || payloadLength === null) return false;

    if (offset + payloadLength > buffer.length) return false;

    const payload = buffer.slice(offset, offset + payloadLength);
    offset += payloadLength;

    frames.push({
      messageType,
      payload,
    });

    messageType = null;
    payloadLength = null;

    return true;
  };

  const compactBuffer = (): void => {
    if (offset > 0) {
      buffer = buffer.slice(offset);
      offset = 0;
    }
  };

  const parseFrames = (data: Uint8Array): Frame[] => {
    appendData(data);

    const frames: Frame[] = [];

    while (true) {
      if (state === "reading_type") {
        if (!tryReadMessageType()) break;
        state = "reading_length";
      }

      if (state === "reading_length") {
        if (!tryReadLength()) break;
        state = "reading_payload";
      }

      if (state === "reading_payload") {
        if (!tryReadPayload(frames)) break;
        state = "reading_type";
      }
    }

    compactBuffer();

    return frames;
  };

  const hasCompleteFrame = (): boolean => {
    if (state === "reading_type") {
      return false;
    }
    if (state === "reading_length") {
      return false;
    }
    if (state === "reading_payload" && payloadLength !== null) {
      return offset + payloadLength <= buffer.length;
    }
    return false;
  };

  return {
    parseFrames,
    hasCompleteFrame,
  };
}

type FrameParserConstructor = {
  new (): FrameParser;
};

export const FrameParser: FrameParserConstructor = function () {
  return createFrameParser();
} as unknown as FrameParserConstructor;
