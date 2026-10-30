/** Shared wire format 2 response envelope. */

import { createBufferReader } from "../core/buffer";
import { ProtocolError } from "../core/errors";

export interface ParsedResponse {
  success: boolean;
  data: Uint8Array;
  error?: string;
  errorCode?: number;
  errorClass?: number;
  effect?: number;
}

export function parseStandardResponse(payload: Uint8Array): ParsedResponse {
  if (payload.length === 0) {
    throw new ProtocolError("Response payload is empty", undefined, { payloadLength: 0 });
  }

  const reader = createBufferReader(payload);
  const status = reader.readU8();
  if (status === 0) {
    return { success: true, data: reader.remaining() };
  }
  if (status !== 1) {
    throw new ProtocolError(`Unknown response status: ${status}`, status, { status });
  }

  const errorCode = reader.readU16BE();
  const errorClass = reader.readU8();
  const effect = reader.readU8();
  const error = reader.readString();
  if (errorClass > 10) {
    throw new ProtocolError(`Unknown error class: ${errorClass}`, errorCode);
  }
  if (effect > 2) {
    throw new ProtocolError(`Unknown error effect: ${effect}`, errorCode);
  }
  if (!reader.isEOF()) {
    throw new ProtocolError("Error response has trailing data", errorCode);
  }
  return { success: false, data: new Uint8Array(0), error, errorCode, errorClass, effect };
}

/** All domain errors now use the same envelope. Kept as a compatibility name. */
export const parsePlainResponse = parseStandardResponse;

export function assertSuccess(payload: Uint8Array, operation: string): Uint8Array {
  const result = parseStandardResponse(payload);
  if (!result.success) {
    throw new ProtocolError(
      `${operation} failed: ${result.error || "Unknown error"}`,
      result.errorCode,
      {
        operation,
        error: result.error || "Unknown error",
        errorCode: result.errorCode,
        errorClass: result.errorClass,
        effect: result.effect,
      },
    );
  }
  return result.data;
}

export const assertPlainSuccess = assertSuccess;
