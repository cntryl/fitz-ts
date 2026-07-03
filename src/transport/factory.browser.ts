/**
 * Browser transport factory.
 */

import { Transport, TransportOptions } from "./types";
import { createWebSocketTransport } from "./websocket.browser";
import { TransportError } from "../core/errors";

export type BrowserTransportType = "ws" | "auto";

function normalizeWebSocketUrl(url: string): string {
  if (url.startsWith("https://")) {
    return url.replace(/^https:\/\//, "wss://");
  }

  if (url.startsWith("http://")) {
    return url.replace(/^http:\/\//, "ws://");
  }

  return url;
}

export function createBrowserTransport(
  url: string,
  transportType: BrowserTransportType | "tcp" = "auto",
  options: TransportOptions = {},
): Transport {
  if (transportType === "tcp" || url.startsWith("tcp://")) {
    throw new TransportError("TCP transport is not available in browsers. Use WebSocket (ws://).");
  }

  if (transportType === "auto" || transportType === "ws") {
    return createWebSocketTransport(normalizeWebSocketUrl(url), options);
  }

  throw new TransportError("Unknown transport type");
}
