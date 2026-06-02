/**
 * Buffer utilities for big-endian encoding
 */

export const utf8Encoder = new TextEncoder();
export const utf8Decoder = new TextDecoder();

let utf8Scratch = new Uint8Array(256);

function ensureUtf8ScratchCapacity(minCapacity: number): Uint8Array {
  if (utf8Scratch.length < minCapacity) {
    utf8Scratch = new Uint8Array(Math.max(utf8Scratch.length * 2, minCapacity));
  }

  return utf8Scratch;
}

export type BufferWriter = ReturnType<typeof createBufferWriter>;

export function createBufferWriter(capacity: number = 4096) {
  let buffer = new Uint8Array(capacity);
  let offset = 0;

  const ensureCapacity = (needed: number) => {
    if (offset + needed > buffer.length) {
      const newCapacity = Math.max(buffer.length * 2, offset + needed);
      const newBuffer = new Uint8Array(newCapacity);
      newBuffer.set(buffer);
      buffer = newBuffer;
    }
  };

  const writeU8 = (value: number): void => {
    ensureCapacity(1);
    buffer[offset++] = value & 0xff;
  };

  const writeU16BE = (value: number): void => {
    ensureCapacity(2);
    buffer[offset++] = (value >> 8) & 0xff;
    buffer[offset++] = value & 0xff;
  };

  const writeU32BE = (value: number): void => {
    ensureCapacity(4);
    buffer[offset++] = (value >> 24) & 0xff;
    buffer[offset++] = (value >> 16) & 0xff;
    buffer[offset++] = (value >> 8) & 0xff;
    buffer[offset++] = value & 0xff;
  };

  const writeU64BE = (value: bigint): void => {
    ensureCapacity(8);
    buffer[offset++] = Number((value >> 56n) & 0xffn);
    buffer[offset++] = Number((value >> 48n) & 0xffn);
    buffer[offset++] = Number((value >> 40n) & 0xffn);
    buffer[offset++] = Number((value >> 32n) & 0xffn);
    buffer[offset++] = Number((value >> 24n) & 0xffn);
    buffer[offset++] = Number((value >> 16n) & 0xffn);
    buffer[offset++] = Number((value >> 8n) & 0xffn);
    buffer[offset++] = Number(value & 0xffn);
  };

  const writeBytes = (data: Uint8Array): void => {
    ensureCapacity(data.length);
    buffer.set(data, offset);
    offset += data.length;
  };

  const writeString = (str: string): void => {
    if (str.length === 0) {
      writeU32BE(0);
      return;
    }

    const scratch = ensureUtf8ScratchCapacity(str.length * 4);
    const { written } = utf8Encoder.encodeInto(str, scratch);
    writeU32BE(written);
    writeBytes(scratch.subarray(0, written));
  };

  const writeRoute = (route: string): void => {
    writeString(route);
  };

  const getBuffer = (): Uint8Array => buffer.slice(0, offset);

  const getLength = (): number => offset;

  const reset = (): void => {
    offset = 0;
  };

  const writeOptionalU64 = (value: bigint | undefined): void => {
    if (value === undefined) {
      writeU8(0);
    } else {
      writeU8(1);
      writeU64BE(value);
    }
  };

  const writeOptionalString = (value: string | undefined): void => {
    if (value === undefined) {
      writeU8(0);
    } else {
      writeU8(1);
      writeString(value);
    }
  };

  const writeOptionalBytes = (value: Uint8Array | undefined): void => {
    if (value === undefined) {
      writeU8(0);
    } else {
      writeU8(1);
      writeU32BE(value.length);
      writeBytes(value);
    }
  };

  return {
    writeU8,
    writeU16BE,
    writeU32BE,
    writeU64BE,
    writeBytes,
    writeString,
    writeRoute,
    getBuffer,
    getLength,
    reset,
    writeOptionalU64,
    writeOptionalString,
    writeOptionalBytes,
  };
}

type BufferWriterConstructor = {
  new (capacity?: number): BufferWriter;
  (capacity?: number): BufferWriter;
};

export const BufferWriter: BufferWriterConstructor = function (capacity = 4096) {
  return createBufferWriter(capacity);
} as unknown as BufferWriterConstructor;

export type BufferReader = ReturnType<typeof createBufferReader>;

export function createBufferReader(buffer: Uint8Array) {
  let internalBuffer = buffer;
  let offset = 0;

  const readU8 = (): number => {
    if (offset >= internalBuffer.length) {
      throw new Error("Buffer overflow: cannot read U8");
    }
    return internalBuffer[offset++];
  };

  const readU16BE = (): number => {
    if (offset + 2 > internalBuffer.length) {
      throw new Error("Buffer overflow: cannot read U16BE");
    }
    const value = (internalBuffer[offset] << 8) | internalBuffer[offset + 1];
    offset += 2;
    return value;
  };

  const readU32BE = (): number => {
    if (offset + 4 > internalBuffer.length) {
      throw new Error("Buffer overflow: cannot read U32BE");
    }
    const value =
      (internalBuffer[offset] << 24) |
      (internalBuffer[offset + 1] << 16) |
      (internalBuffer[offset + 2] << 8) |
      internalBuffer[offset + 3];
    offset += 4;
    return value >>> 0;
  };

  const readU64BE = (): bigint => {
    if (offset + 8 > internalBuffer.length) {
      throw new Error("Buffer overflow: cannot read U64BE");
    }
    const high =
      ((BigInt(internalBuffer[offset]) << 24n) |
        (BigInt(internalBuffer[offset + 1]) << 16n) |
        (BigInt(internalBuffer[offset + 2]) << 8n) |
        BigInt(internalBuffer[offset + 3])) &
      0xffffffffn;

    const low =
      ((BigInt(internalBuffer[offset + 4]) << 24n) |
        (BigInt(internalBuffer[offset + 5]) << 16n) |
        (BigInt(internalBuffer[offset + 6]) << 8n) |
        BigInt(internalBuffer[offset + 7])) &
      0xffffffffn;

    offset += 8;
    return (high << 32n) | low;
  };

  const readBytes = (count: number): Uint8Array => {
    if (offset + count > internalBuffer.length) {
      throw new Error("Buffer overflow: cannot read bytes");
    }
    const data = internalBuffer.slice(offset, offset + count);
    offset += count;
    return data;
  };

  const readString = (): string => {
    const length = readU32BE();
    if (length === 0) {
      return "";
    }

    const bytes = readBytes(length);
    return utf8Decoder.decode(bytes);
  };

  const readRoute = (): string => readString();

  const remainingBytes = (): number => internalBuffer.length - offset;

  const isEOF = (): boolean => offset >= internalBuffer.length;

  const getOffset = (): number => offset;

  const setOffset = (value: number): void => {
    if (value < 0 || value > internalBuffer.length) {
      throw new Error("Invalid offset");
    }
    offset = value;
  };

  const peekU8 = (): number => {
    if (offset >= internalBuffer.length) {
      throw new Error("Buffer overflow: cannot peek U8");
    }
    return internalBuffer[offset];
  };

  const readOptionalU64 = (): bigint | undefined => {
    const hasValue = readU8();
    if (hasValue === 0) {
      return undefined;
    }
    return readU64BE();
  };

  const readOptionalString = (): string | undefined => {
    const hasValue = readU8();
    if (hasValue === 0) {
      return undefined;
    }
    return readString();
  };

  const readOptionalBytes = (): Uint8Array | undefined => {
    const hasValue = readU8();
    if (hasValue === 0) {
      return undefined;
    }
    const length = readU32BE();
    return readBytes(length);
  };

  const remaining = (): Uint8Array => internalBuffer.slice(offset);

  return {
    readU8,
    readU16BE,
    readU32BE,
    readU64BE,
    readBytes,
    readString,
    readRoute,
    remainingBytes,
    isEOF,
    getOffset,
    setOffset,
    peekU8,
    readOptionalU64,
    readOptionalString,
    readOptionalBytes,
    remaining,
  };
}

type BufferReaderConstructor = {
  new (buffer: Uint8Array): BufferReader;
  (buffer: Uint8Array): BufferReader;
};

export const BufferReader: BufferReaderConstructor = function (buffer: Uint8Array) {
  return createBufferReader(buffer);
} as unknown as BufferReaderConstructor;
