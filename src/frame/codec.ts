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

export interface PendingFrameInfo {
  hasPending: boolean;
  bufferedBytes: number;
  state: "type" | "length" | "payload";
  messageType?: number;
  payloadLength?: number;
  payloadBytesRead?: number;
  payloadBytesRemaining?: number;
}

const encodeFrame = (messageType: number, payload: Uint8Array): Uint8Array => {
  if (messageType < 0) {
    throw new CodecError(`Invalid message type: ${messageType}`);
  }
  if (messageType > 0xffff) {
    throw new CodecError(`Invalid message type: ${messageType}`);
  }

  const payloadLength = payload.length;
  if (payloadLength > 0xffff) {
    throw new CodecError(`Frame payload too large: ${payloadLength} bytes`);
  }

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

  const firstByte = buffer[offset++]!;
  let messageType: number;

  if (firstByte === 0xff) {
    if (offset + 2 > buffer.length) {
      throw new CodecError("Frame incomplete: invalid message type header");
    }
    messageType = (buffer[offset]! << 8) | buffer[offset + 1]!;
    offset += 2;
  } else {
    messageType = firstByte;
  }

  if (offset + 2 > buffer.length) {
    throw new CodecError("Frame incomplete: missing length header");
  }

  const length = (buffer[offset]! << 8) | buffer[offset + 1]!;
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

  const parseCompleteInputFrames = (data: Uint8Array): { frames: Frame[]; consumed: number } => {
    const frames: Frame[] = [];
    let cursor = 0;

    while (cursor < data.length) {
      const frameStart = cursor;
      const firstByte = data[cursor++]!;
      let parsedMessageType: number;

      if (firstByte === 0xff) {
        if (cursor + 2 > data.length) {
          return { frames, consumed: frameStart };
        }
        parsedMessageType = (data[cursor]! << 8) | data[cursor + 1]!;
        cursor += 2;
      } else {
        parsedMessageType = firstByte;
      }

      if (cursor + 2 > data.length) {
        return { frames, consumed: frameStart };
      }

      const parsedPayloadLength = (data[cursor]! << 8) | data[cursor + 1]!;
      cursor += 2;
      if (cursor + parsedPayloadLength > data.length) {
        return { frames, consumed: frameStart };
      }

      frames.push({
        messageType: parsedMessageType,
        payload: data.subarray(cursor, cursor + parsedPayloadLength),
      });
      cursor += parsedPayloadLength;
    }

    return { frames, consumed: cursor };
  };

  const parseFrames = (data: Uint8Array): Frame[] => {
    if (state === 0 && offset === end && data.length > 0) {
      const parsed = parseCompleteInputFrames(data);
      if (parsed.consumed === data.length) {
        return parsed.frames;
      }

      if (parsed.consumed > 0) {
        appendData(data.subarray(parsed.consumed));
        return parsed.frames;
      }
    }

    appendData(data);

    const frames: Frame[] = [];

    while (true) {
      if (state === 0) {
        if (offset >= end) break;

        const firstByte = buffer[offset]!;
        if (firstByte === 0xff) {
          if (offset + 3 > end) break;
          messageType = (buffer[offset + 1]! << 8) | buffer[offset + 2]!;
          offset += 3;
        } else {
          messageType = firstByte;
          offset += 1;
        }

        state = 1;
      }

      if (state === 1) {
        if (offset + 2 > end) break;
        payloadLength = (buffer[offset]! << 8) | buffer[offset + 1]!;
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

  const getPendingFrameInfo = (): PendingFrameInfo => {
    const bufferedBytes = end - offset;

    if (state === 2 && payloadLength >= 0) {
      const payloadBytesRead = Math.max(0, bufferedBytes);
      return {
        hasPending: true,
        bufferedBytes,
        state: "payload",
        messageType,
        payloadLength,
        payloadBytesRead,
        payloadBytesRemaining: Math.max(0, payloadLength - payloadBytesRead),
      };
    }

    if (state === 1) {
      return {
        hasPending: true,
        bufferedBytes,
        state: "length",
        messageType,
      };
    }

    return {
      hasPending: bufferedBytes > 0,
      bufferedBytes,
      state: "type",
    };
  };

  const reset = (): void => {
    offset = 0;
    end = 0;
    messageType = -1;
    payloadLength = -1;
    state = 0;
  };

  return {
    parseFrames,
    hasCompleteFrame,
    getPendingFrameInfo,
    reset,
  };
}

export const FrameParser = createFrameParser;
