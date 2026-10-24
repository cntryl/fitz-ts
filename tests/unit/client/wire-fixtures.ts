import { FrameCodec } from "../../../src/frame/codec";
import { MSG_HELLO } from "../../../src/frame/types";

export function wire2HelloPayload(brokerCapabilities = 0): Uint8Array {
  const payload = new Uint8Array(22);
  const view = new DataView(payload.buffer);
  view.setUint16(0, 2, false);
  view.setUint32(2, brokerCapabilities, false);
  view.setUint32(6, 1024 * 1024, false);
  view.setUint32(10, 256, false);
  view.setBigUint64(14, 1n, false);
  return payload;
}

export function wire2HelloFrame(brokerCapabilities = 0): Uint8Array {
  return FrameCodec.encodeFrame(MSG_HELLO, wire2HelloPayload(brokerCapabilities));
}
