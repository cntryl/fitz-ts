/**
 * Buffer utilities for big-endian encoding
 */

export const utf8Encoder = new TextEncoder();
export const utf8Decoder = new TextDecoder();

const routeEncodingCache = new Map<string, Uint8Array>();
const routeEncodingCacheMaxEntries = 256;

export function getRouteEncoding(route: string): Uint8Array {
  const cached = routeEncodingCache.get(route);
  if (cached) {
    return cached;
  }

  const encoded = utf8Encoder.encode(route);
  const routePayload = new Uint8Array(4 + encoded.length);
  const len = encoded.length;
  routePayload[0] = (len >> 24) & 0xff;
  routePayload[1] = (len >> 16) & 0xff;
  routePayload[2] = (len >> 8) & 0xff;
  routePayload[3] = len & 0xff;
  routePayload.set(encoded, 4);

  if (routeEncodingCache.size >= routeEncodingCacheMaxEntries) {
    const firstKey = routeEncodingCache.keys().next().value;
    if (firstKey !== undefined) {
      routeEncodingCache.delete(firstKey);
    }
  }
  routeEncodingCache.set(route, routePayload);
  return routePayload;
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

  const writeU32LE = (value: number): void => {
    ensureCapacity(4);
    buffer[offset++] = value & 0xff;
    buffer[offset++] = (value >> 8) & 0xff;
    buffer[offset++] = (value >> 16) & 0xff;
    buffer[offset++] = (value >> 24) & 0xff;
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

  const writeU64LE = (value: bigint): void => {
    ensureCapacity(8);
    buffer[offset++] = Number(value & 0xffn);
    buffer[offset++] = Number((value >> 8n) & 0xffn);
    buffer[offset++] = Number((value >> 16n) & 0xffn);
    buffer[offset++] = Number((value >> 24n) & 0xffn);
    buffer[offset++] = Number((value >> 32n) & 0xffn);
    buffer[offset++] = Number((value >> 40n) & 0xffn);
    buffer[offset++] = Number((value >> 48n) & 0xffn);
    buffer[offset++] = Number((value >> 56n) & 0xffn);
  };

  const writeBytes = (data: Uint8Array): void => {
    ensureCapacity(data.length);
    buffer.set(data, offset);
    offset += data.length;
  };

  const overwriteU32BE = (position: number, value: number): void => {
    if (position + 4 > buffer.length) {
      throw new Error("Buffer overwrite out of bounds");
    }
    buffer[position] = (value >> 24) & 0xff;
    buffer[position + 1] = (value >> 16) & 0xff;
    buffer[position + 2] = (value >> 8) & 0xff;
    buffer[position + 3] = value & 0xff;
  };

  const writeString = (str: string): void => {
    if (str.length === 0) {
      writeU32BE(0);
      return;
    }

    const maxBytes = str.length * 4;
    ensureCapacity(4 + maxBytes);
    const lengthOffset = offset;
    offset += 4;

    const slice = buffer.subarray(offset, offset + maxBytes);
    const { written } = utf8Encoder.encodeInto(str, slice);
    offset += written;

    buffer[lengthOffset] = (written >> 24) & 0xff;
    buffer[lengthOffset + 1] = (written >> 16) & 0xff;
    buffer[lengthOffset + 2] = (written >> 8) & 0xff;
    buffer[lengthOffset + 3] = written & 0xff;
  };

  const writeStringU64 = (str: string): void => {
    if (str.length === 0) {
      writeU64BE(0n);
      return;
    }

    const maxBytes = str.length * 4;
    ensureCapacity(8 + maxBytes);
    const lengthOffset = offset;
    offset += 8;

    const slice = buffer.subarray(offset, offset + maxBytes);
    const { written } = utf8Encoder.encodeInto(str, slice);
    offset += written;

    const length = BigInt(written);
    buffer[lengthOffset] = Number((length >> 56n) & 0xffn);
    buffer[lengthOffset + 1] = Number((length >> 48n) & 0xffn);
    buffer[lengthOffset + 2] = Number((length >> 40n) & 0xffn);
    buffer[lengthOffset + 3] = Number((length >> 32n) & 0xffn);
    buffer[lengthOffset + 4] = Number((length >> 24n) & 0xffn);
    buffer[lengthOffset + 5] = Number((length >> 16n) & 0xffn);
    buffer[lengthOffset + 6] = Number((length >> 8n) & 0xffn);
    buffer[lengthOffset + 7] = Number(length & 0xffn);
  };

  const writeStringU64LE = (str: string): void => {
    if (str.length === 0) {
      writeU64LE(0n);
      return;
    }

    const maxBytes = str.length * 4;
    ensureCapacity(8 + maxBytes);
    const lengthOffset = offset;
    offset += 8;

    const slice = buffer.subarray(offset, offset + maxBytes);
    const { written } = utf8Encoder.encodeInto(str, slice);
    offset += written;

    const length = BigInt(written);
    buffer[lengthOffset] = Number(length & 0xffn);
    buffer[lengthOffset + 1] = Number((length >> 8n) & 0xffn);
    buffer[lengthOffset + 2] = Number((length >> 16n) & 0xffn);
    buffer[lengthOffset + 3] = Number((length >> 24n) & 0xffn);
    buffer[lengthOffset + 4] = Number((length >> 32n) & 0xffn);
    buffer[lengthOffset + 5] = Number((length >> 40n) & 0xffn);
    buffer[lengthOffset + 6] = Number((length >> 48n) & 0xffn);
    buffer[lengthOffset + 7] = Number((length >> 56n) & 0xffn);
  };

  const writeRoute = (route: string): void => {
    const encoded = getRouteEncoding(route);
    writeBytes(encoded);
  };

  const getBuffer = (): Uint8Array => {
    return offset === buffer.length ? buffer : buffer.slice(0, offset);
  };

  const getBufferView = (): Uint8Array => {
    return buffer.subarray(0, offset);
  };

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
    writeU32LE,
    writeU64BE,
    writeU64LE,
    writeBytes,
    writeString,
    writeStringU64,
    writeStringU64LE,
    writeRoute,
    getBuffer,
    getBufferView,
    getLength,
    reset,
    writeOptionalU64,
    writeOptionalString,
    writeOptionalBytes,
    overwriteU32BE,
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
    const data = internalBuffer.subarray(offset, offset + count);
    offset += count;
    return data;
  };

  const readString = (): string => {
    const length = readU32BE();
    if (length === 0) {
      return "";
    }

    const bytes = internalBuffer.subarray(offset, offset + length);
    offset += length;
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
