/**
 * Standard response parsing utilities
 * Per CLIENT_SPEC.md and fitz-go/internal/core/connection/connection.go
 *
 * Standard Response Format:
 * [u8 status][payload...]
 *
 * status=0: success, remaining payload follows
 * status=1: error, followed by [u32 code][u32 len][UTF-8 error message]
 */

import { createBufferReader } from "../core/buffer";
import { ProtocolError } from "../core/errors";

export interface ParsedResponse {
  success: boolean;
  data: Uint8Array;
  error?: string;
  errorCode?: number;
}

/**
 * Parse standard response format
 * Returns {success: true, data: remaining} on status=0
 * Returns {success: false, error: message} on status=1
 */
export function parseStandardResponse(payload: Uint8Array): ParsedResponse {
  if (payload.length === 0) {
    throw new ProtocolError("Response payload is empty", undefined, {
      payloadLength: 0,
    });
  }

  const reader = createBufferReader(payload);
  const status = reader.readU8();

  if (status === 0) {
    // Success - return remaining payload
    const remaining = reader.remaining();
    return { success: true, data: remaining };
  }

  if (status === 1) {
    // Error codes are intentionally not validated here. A newer broker may
    // allocate a code before this client knows its symbolic name.
    const errorCode = reader.readU32BE();
    const errorMsg = reader.readString();
    if (!reader.isEOF()) {
      throw new ProtocolError("Error response has trailing data", errorCode);
    }
    return { success: false, data: new Uint8Array(0), error: errorMsg, errorCode };
  }

  // Unknown status code
  throw new ProtocolError(`Unknown response status: ${status}`, status, {
    status,
  });
}

/**
 * Check if response is success, throw error if not
 * Returns remaining data on success
 */
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
      },
    );
  }
  return result.data;
}
