/**
 * Shared WebSocket URL scheme normalization.
 *
 * Historically this logic (ws/wss passthrough, https->wss, http->ws, and a
 * bare-host fallback to ws://) was independently reimplemented with subtly
 * different coverage in factory.node.ts, factory.browser.ts, and inline in
 * both websocket.node.ts and websocket.browser.ts's connect(). This is the
 * one shared, most-complete implementation all four now use.
 *
 * tcp.ts's URL handling is a different shape entirely (host:port extraction
 * via `new URL()`, not scheme rewriting) and intentionally isn't folded in
 * here.
 */
export function normalizeWebSocketUrl(url: string): string {
  if (url.startsWith("ws://") || url.startsWith("wss://")) {
    return url;
  }

  if (url.startsWith("https://")) {
    return url.replace(/^https:\/\//, "wss://");
  }

  if (url.startsWith("http://")) {
    return url.replace(/^http:\/\//, "ws://");
  }

  return `ws://${url}`;
}
