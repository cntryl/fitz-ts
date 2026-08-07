/**
 * Shared AbortError helpers.
 *
 * Historically this exact pair of functions was independently copy-pasted
 * into src/client/internal/async.ts, src/client/multiplexer.ts,
 * src/transport/tcp.ts, src/transport/websocket.node.ts, and
 * src/transport/websocket.browser.ts. `core/` has no reverse dependency on
 * `client/` or `transport/` anywhere in this package, so it's the one
 * layering-safe home both sides can import from.
 */
export function abortError(): Error {
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

export function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}
