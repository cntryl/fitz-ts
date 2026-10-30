/** Wire format 2 framing and control-message codecs. */

import { createBufferReader, createBufferWriter } from "../core/buffer";
import { CodecError } from "../core/errors";

export const HEADER_LENGTH = 15;
export const DEFAULT_MAX_MESSAGE_BYTES = 1024 * 1024;
export const FLAG_ACK_REQUESTED = 0x01;
export const FLAG_RESPONSE_FINAL = 0x02;
const RESERVED_FLAGS = 0xfc;
const MAX_U64 = 0xffffffffffffffffn;

export interface Frame {
  messageType: number;
  flags: number;
  correlation: bigint;
  payload: Uint8Array;
}

export interface FrameOptions {
  flags?: number;
  correlation?: bigint;
}

export interface Hello {
  version: number;
  brokerCapabilities: number;
  maxMessageBytes: number;
  maxInFlight: number;
  sessionId: bigint;
}

export interface ConnectRejected {
  code: number;
  message: string;
}

export interface GoAway {
  code: number;
  graceMs: number;
  reason: string;
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

const encodeFrame = (
  messageType: number,
  payload: Uint8Array,
  options: FrameOptions = {},
): Uint8Array => {
  if (!Number.isInteger(messageType) || messageType < 0 || messageType > 0xffff) {
    throw new CodecError(`Invalid message type: ${messageType}`);
  }
  const flags = options.flags ?? 0;
  const correlation = options.correlation ?? 0n;
  if (!Number.isInteger(flags) || flags < 0 || flags > 0xff || (flags & RESERVED_FLAGS) !== 0) {
    throw new CodecError(`Invalid message flags: ${flags}`);
  }
  if (correlation < 0n || correlation > MAX_U64) {
    throw new CodecError(`Invalid correlation id: ${correlation}`);
  }
  if (payload.length + HEADER_LENGTH > DEFAULT_MAX_MESSAGE_BYTES) {
    throw new CodecError(`Message exceeds ${DEFAULT_MAX_MESSAGE_BYTES} bytes`);
  }

  const frame = new Uint8Array(HEADER_LENGTH + payload.length);
  const view = new DataView(frame.buffer);
  view.setUint16(0, messageType, false);
  view.setUint8(2, flags);
  view.setBigUint64(3, correlation, false);
  view.setUint32(11, payload.length, false);
  frame.set(payload, HEADER_LENGTH);
  return frame;
};

const decodeFrame = (frame: Uint8Array, maxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES): Frame => {
  if (frame.length < HEADER_LENGTH) {
    throw new CodecError(`Message header is truncated: got ${frame.length} bytes`);
  }
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  const messageType = view.getUint16(0, false);
  const flags = view.getUint8(2);
  if ((flags & RESERVED_FLAGS) !== 0) {
    throw new CodecError(`Reserved message flags are set: ${flags}`);
  }
  const correlation = view.getBigUint64(3, false);
  const payloadLength = view.getUint32(11, false);
  if (payloadLength + HEADER_LENGTH > maxMessageBytes) {
    throw new CodecError(`Message exceeds negotiated limit of ${maxMessageBytes} bytes`);
  }
  const actualPayloadLength = frame.length - HEADER_LENGTH;
  if (actualPayloadLength !== payloadLength) {
    throw new CodecError(
      `Message length mismatch: declared ${payloadLength}, got ${actualPayloadLength}`,
    );
  }
  return {
    messageType,
    flags,
    correlation,
    payload: frame.subarray(HEADER_LENGTH),
  };
};

const withCorrelation = (frame: Uint8Array, correlation: bigint): Uint8Array => {
  if (correlation <= 0n || correlation > MAX_U64) {
    throw new CodecError(`Invalid request correlation: ${correlation}`);
  }
  if (frame.length < HEADER_LENGTH) {
    throw new CodecError("Cannot label a truncated message");
  }
  const labelled = frame.slice();
  new DataView(labelled.buffer).setBigUint64(3, correlation, false);
  return labelled;
};

const encodeConnect = (input: {
  jwt: string;
  serviceName: string;
  clientBuild: string;
}): Uint8Array => {
  const writer = createBufferWriter();
  writer.writeU16BE(2);
  writer.writeU16BE(2);
  writer.writeU32BE(0);
  writer.writeString(input.jwt);
  writer.writeString(input.serviceName);
  writer.writeString(input.clientBuild);
  return writer.getBuffer();
};

const decodeHello = (payload: Uint8Array): Hello => {
  const minimumLength = 2 + 4 + 4 + 4 + 8;
  if (payload.length < minimumLength) {
    throw new CodecError(`HELLO needs at least ${minimumLength} bytes`);
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  return {
    version: view.getUint16(0, false),
    brokerCapabilities: view.getUint32(2, false),
    maxMessageBytes: view.getUint32(6, false),
    maxInFlight: view.getUint32(10, false),
    sessionId: view.getBigUint64(14, false),
  };
};

const decodeConnectRejected = (payload: Uint8Array): ConnectRejected => {
  const reader = createBufferReader(payload);
  const code = reader.readU16BE();
  const message = reader.readString();
  if (!reader.isEOF()) {
    throw new CodecError("CONNECT_REJECTED has trailing data");
  }
  return { code, message };
};

const decodeGoAway = (payload: Uint8Array): GoAway => {
  const reader = createBufferReader(payload);
  const code = reader.readU16BE();
  const graceMs = reader.readU32BE();
  const reason = reader.readString();
  if (!reader.isEOF()) {
    throw new CodecError("GOAWAY has trailing data");
  }
  return { code, graceMs, reason };
};

export const FrameCodec = {
  encodeFrame,
  decodeFrame,
  withCorrelation,
  encodeConnect,
  decodeHello,
  decodeConnectRejected,
  decodeGoAway,
  calculateFrameSize: (_messageType: number, payloadLength: number): number =>
    HEADER_LENGTH + payloadLength,
};

export { decodeConnectRejected, decodeGoAway, decodeHello, encodeConnect, withCorrelation };

export type FrameParser = ReturnType<typeof createFrameParser>;

export function createFrameParser(initialMaxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES) {
  let buffer = new Uint8Array(0);
  let maxMessageBytes = initialMaxMessageBytes;

  const parseFrames = (data: Uint8Array): Frame[] => {
    if (data.length > 0) {
      if (buffer.length + data.length > maxMessageBytes) {
        throw new CodecError(`Message exceeds negotiated limit of ${maxMessageBytes} bytes`);
      }
      const combined = new Uint8Array(buffer.length + data.length);
      combined.set(buffer);
      combined.set(data, buffer.length);
      buffer = combined;
    }
    if (buffer.length < HEADER_LENGTH) {
      return [];
    }

    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    const payloadLength = view.getUint32(11, false);
    const frameLength = HEADER_LENGTH + payloadLength;
    if (frameLength > maxMessageBytes) {
      throw new CodecError(`Message exceeds negotiated limit of ${maxMessageBytes} bytes`);
    }
    if (buffer.length < frameLength) {
      return [];
    }
    if (buffer.length > frameLength) {
      throw new CodecError("Transport frame contains more than one message");
    }

    const complete = buffer;
    buffer = new Uint8Array(0);
    return [decodeFrame(complete, maxMessageBytes)];
  };

  const getPendingFrameInfo = (): PendingFrameInfo => {
    if (buffer.length === 0) {
      return { hasPending: false, bufferedBytes: 0, state: "type" };
    }
    if (buffer.length < HEADER_LENGTH) {
      return {
        hasPending: true,
        bufferedBytes: buffer.length,
        state: buffer.length < 2 ? "type" : "length",
        ...(buffer.length >= 2 ? { messageType: (buffer[0]! << 8) | buffer[1]! } : {}),
      };
    }
    const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    const payloadLength = view.getUint32(11, false);
    const payloadBytesRead = buffer.length - HEADER_LENGTH;
    return {
      hasPending: true,
      bufferedBytes: buffer.length,
      state: "payload",
      messageType: view.getUint16(0, false),
      payloadLength,
      payloadBytesRead,
      payloadBytesRemaining: Math.max(0, payloadLength - payloadBytesRead),
    };
  };

  const reset = (): void => {
    buffer = new Uint8Array(0);
  };

  return {
    parseFrames,
    getPendingFrameInfo,
    reset,
    setMaxMessageBytes: (value: number): void => {
      if (!Number.isInteger(value) || value < HEADER_LENGTH) {
        throw new CodecError(`Invalid max_message_bytes value: ${value}`);
      }
      maxMessageBytes = value;
    },
  };
}
