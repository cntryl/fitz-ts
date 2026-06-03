/**
 * Frame encoding/decoding for Fitz protocol
 * Format: [MessageType (variable)][Length (u16 BE)][Payload]
 * MessageType: 0-254 = single byte, 255+ = escape 0xFF + u16 BE
 */

import { CodecError } from "../core/errors";

export interface Frame {
  messageType: number;
  payload: Uint8Array;
}

const encodeFrame = (messageType: number, payload: Uint8Array): Uint8Array => {
  if (messageType < 0) {
    throw new CodecError(`Invalid message type: ${messageType}`);
  }

  const payloadLength = payload.length;
  const typeSize = messageType <= 254 ? 1 : 3;
  const buffer = new Uint8Array(typeSize + 2 + payloadLength);
  let offset = 0;

  if (messageType <= 254) {
    buffer[offset++] = messageType;
  } else {
    buffer[offset++] = 0xff;
    buffer[offset++] = (messageType >> 8) & 0xff;
    buffer[offset++] = messageType & 0xff;
  }

  buffer[offset++] = (payloadLength >> 8) & 0xff;
  buffer[offset++] = payloadLength & 0xff;
  buffer.set(payload, offset);

  return buffer;
};

const decodeFrame = (buffer: Uint8Array): Frame => {
  let offset = 0;

  const firstByte = buffer[offset++];
  let messageType: number;

  if (firstByte === 0xff) {
    if (offset + 2 > buffer.length) {
      throw new CodecError("Frame incomplete: invalid message type header");
    }
    messageType = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;
  } else {
    messageType = firstByte;
  }

  if (offset + 2 > buffer.length) {
    throw new CodecError("Frame incomplete: missing length header");
  }

  const length = (buffer[offset] << 8) | buffer[offset + 1];
  offset += 2;

  const remaining = buffer.length - offset;
  if (remaining < length) {
    throw new CodecError(`Frame incomplete: expected ${length} bytes, got ${remaining}`);
  }

  const payload = buffer.subarray(offset, offset + length);
  return { messageType, payload };
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
  let buffer: Uint8Array = new Uint8Array(1024);
  let offset = 0;
  let end = 0;
  let messageType = -1;
  let payloadLength = -1;
  let state = 0; // 0=reading_type, 1=reading_length, 2=reading_payload

  const ensureCapacity = (needed: number): void => {
    if (buffer.length >= needed) {
      return;
    }

    const newCapacity = Math.max(buffer.length * 2, needed, 1024);
    const newBuffer = new Uint8Array(newCapacity);
    if (end > offset) {
      newBuffer.set(buffer.subarray(offset, end));
      end -= offset;
      offset = 0;
    } else {
      end = 0;
      offset = 0;
    }
    buffer = newBuffer;
  };

  const appendData = (data: Uint8Array): void => {
    if (data.length === 0) {
      return;
    }

    const remaining = end - offset;
    if (remaining === 0) {
      offset = 0;
      end = 0;
    } else if (buffer.length - end < data.length) {
      if (offset > 0) {
        buffer.copyWithin(0, offset, end);
        end = remaining;
        offset = 0;
      }
    }

    ensureCapacity(end + data.length);
    buffer.set(data, end);
    end += data.length;
  };

  const parseFrames = (data: Uint8Array): Frame[] => {
    appendData(data);

    const frames: Frame[] = [];

    while (true) {
      if (state === 0) {
        if (offset >= end) break;

        const firstByte = buffer[offset];
        if (firstByte === 0xff) {
          if (offset + 3 > end) break;
          messageType = (buffer[offset + 1] << 8) | buffer[offset + 2];
          offset += 3;
        } else {
          messageType = firstByte;
          offset += 1;
        }

        state = 1;
      }

      if (state === 1) {
        if (offset + 2 > end) break;
        payloadLength = (buffer[offset] << 8) | buffer[offset + 1];
        offset += 2;
        state = 2;
      }

      if (state === 2) {
        if (offset + payloadLength > end) break;
        frames.push({
          messageType,
          payload: buffer.slice(offset, offset + payloadLength),
        });
        offset += payloadLength;
        messageType = -1;
        payloadLength = -1;
        state = 0;
      }
    }

    if (offset === end) {
      offset = 0;
      end = 0;
    }

    return frames;
  };

  const hasCompleteFrame = (): boolean => {
    return state === 2 && payloadLength >= 0 && offset + payloadLength <= end;
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
