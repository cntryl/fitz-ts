import { CodecError } from "../core/errors";

export const FLAG_EXTENSIONS_PRESENT = 4;
export const CAP_QUEUE_MESSAGE_TTL = 1;
export const CAP_QUEUE_NACK = 2;
export const CLIENT_CAPABILITY_IDS: readonly number[] = [1, 2];

export function encodeCapabilities(ids: readonly number[]): Uint8Array {
  if (ids.length > 0xffff) throw new CodecError("Too many capabilities");
  const result = new Uint8Array(2 + 4 * ids.length);
  const view = new DataView(result.buffer);
  view.setUint16(0, ids.length);
  let previous = 0;
  ids.forEach((id, index) => {
    if (!Number.isInteger(id) || id <= previous || id > 0xffffffff) {
      throw new CodecError("Capabilities must be nonzero ascending u32 IDs");
    }
    view.setUint32(2 + 4 * index, id);
    previous = id;
  });
  return result;
}

export function encodeExtensions(
  core: Uint8Array,
  fields: readonly (readonly [number, Uint8Array])[],
): Uint8Array {
  if (fields.length === 0) return core;
  const size = 4 + core.length + fields.reduce((sum, [, value]) => sum + 6 + value.length, 0);
  if (size + 15 > 1024 * 1024) throw new CodecError("Extensions exceed message size limit");
  const result = new Uint8Array(size);
  const view = new DataView(result.buffer);
  view.setUint32(0, core.length);
  result.set(core, 4);
  let cursor = 4 + core.length;
  let previous = 0;
  for (const [id, value] of fields) {
    if (!Number.isInteger(id) || id <= previous || id > 0xffff)
      throw new CodecError("Extension IDs must be nonzero and ascending");
    view.setUint16(cursor, id);
    view.setUint32(cursor + 2, value.length);
    result.set(value, cursor + 6);
    cursor += 6 + value.length;
    previous = id;
  }
  return result;
}

export function decodeExtensions(
  payload: Uint8Array,
  messageType: number,
  flags: number,
): { core: Uint8Array; capabilityIds: readonly number[] } {
  const handshake = messageType === 2;
  if ((flags & FLAG_EXTENSIONS_PRESENT) === 0) {
    if (handshake) throw new CodecError("HELLO requires capability extensions");
    return { core: payload, capabilityIds: [] };
  }
  if (payload.length < 4) throw new CodecError("Truncated extension core length");
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const coreEnd = 4 + view.getUint32(0);
  if (coreEnd >= payload.length)
    throw new CodecError("Core exceeds bounds or extensions are missing");
  let cursor = coreEnd;
  let capabilityIds: number[] | undefined;
  while (cursor < payload.length) {
    if (payload.length - cursor < 6) throw new CodecError("Truncated extension header");
    const id = view.getUint16(cursor);
    const size = view.getUint32(cursor + 2);
    cursor += 6;
    if (id === 0 || size > payload.length - cursor)
      throw new CodecError("Invalid extension ID or length");
    if (handshake && id === 1) {
      if (capabilityIds !== undefined || size < 2)
        throw new CodecError("Duplicate or truncated capabilities");
      const count = view.getUint16(cursor);
      if (size !== 2 + count * 4) throw new CodecError("Capability length mismatch");
      capabilityIds = [];
      let previous = 0;
      for (let index = 0; index < count; index++) {
        const value = view.getUint32(cursor + 2 + index * 4);
        if (value <= previous) throw new CodecError("Capabilities must be nonzero and ascending");
        capabilityIds.push(value);
        previous = value;
      }
    }
    cursor += size;
  }
  if (handshake && capabilityIds === undefined)
    throw new CodecError("HELLO capability list is missing");
  return { core: payload.subarray(4, coreEnd), capabilityIds: capabilityIds ?? [] };
}
