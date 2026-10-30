import { createBufferWriter, type BufferWriter } from "../../../src/core/buffer";

export function writeErrorCode(writer: BufferWriter, code: number): void {
  const category = code % 1000;
  writer.writeU16BE(code);
  writer.writeU8(category <= 10 ? category : 0);
  writer.writeU8(category === 6 || category === 7 ? 1 : 0);
}
export function errorPayload(code: number, message: string): Uint8Array {
  const writer = createBufferWriter();
  writer.writeU8(1);
  writeErrorCode(writer, code);
  writer.writeString(message);
  return writer.getBuffer();
}
