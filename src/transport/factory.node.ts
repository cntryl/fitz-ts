/**
 * Transport factory to create appropriate transport based on URL
 */

import { Transport, TransportOptions } from "./types";
import { createWebSocketTransport } from "./websocket.node";
import { createTcpTransport } from "./tcp";
import { normalizeWebSocketUrl } from "./url";
import { TransportError } from "../core/errors";

export function createNodeTransport(
  url: string,
  transportType: "ws" | "tcp" | "auto" = "auto",
  options: TransportOptions = {},
): Transport {
  if (transportType === "auto") {
    // Detect from URL
    if (url.startsWith("ws://") || url.startsWith("wss://")) {
      return createWebSocketTransport(url, options);
    }
    if (url.startsWith("tcp://")) {
      return createTcpTransport(url, options);
    }
    if (url.startsWith("http://") || url.startsWith("https://")) {
      return createWebSocketTransport(normalizeWebSocketUrl(url), options);
    }

    // Default to WebSocket
    return createWebSocketTransport(url, options);
  }

  if (transportType === "ws") {
    return createWebSocketTransport(url, options);
  }

  if (transportType === "tcp") {
    return createTcpTransport(url, options);
  }

  throw new TransportError("Unknown transport type");
}

export const createTransport = createNodeTransport;
