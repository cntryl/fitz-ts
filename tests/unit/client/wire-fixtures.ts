import { encodeExtensions, encodeCapabilities } from "../../../src/frame/extensions";
import { FrameCodec } from "../../../src/frame/codec";
import { MSG_HELLO } from "../../../src/frame/types";

export function wire3HelloPayload(
  brokerCapabilities = 0,
  maxMessageBytes = 1024 * 1024,
): Uint8Array {
  const payload = new Uint8Array(22);
  const view = new DataView(payload.buffer);
  view.setUint16(0, 3, false);
  view.setUint32(2, brokerCapabilities, false);
  view.setUint32(6, maxMessageBytes, false);
  view.setUint32(10, 256, false);
  view.setBigUint64(14, 1n, false);
  return payload;
}

export function wire3HelloFrame(brokerCapabilities = 0, maxMessageBytes = 1024 * 1024): Uint8Array {
  return FrameCodec.encodeFrame(
    MSG_HELLO,
    encodeExtensions(wire3HelloPayload(brokerCapabilities, maxMessageBytes), [
      [1, encodeCapabilities([1, 2])],
    ]),
    { flags: 4 },
  );
}
