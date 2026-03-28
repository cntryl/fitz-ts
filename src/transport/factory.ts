/**
 * Transport factory to create appropriate transport based on URL
 */

import { Transport, TransportOptions } from "./types";
import { WebSocketTransport } from "./websocket";
import { TcpTransport } from "./tcp";
import { TransportError } from "../core/errors";

type NodeLikeProcess = {
  versions?: {
    node?: string;
  };
};

const isNode = (): boolean => {
  try {
    const candidate = globalThis as typeof globalThis & {
      process?: NodeLikeProcess;
    };
    return (
      typeof candidate.process !== "undefined" &&
      typeof candidate.process?.versions?.node === "string"
    );
  } catch {
    return false;
  }
};

export function createTransport(
  url: string,
  transportType: "ws" | "tcp" | "auto" = "auto",
  options: TransportOptions = {},
): Transport {
  if (transportType === "auto") {
    // Detect from URL
    if (url.startsWith("ws://") || url.startsWith("wss://")) {
      return new WebSocketTransport(url, options);
    }
    if (url.startsWith("tcp://")) {
      if (!isNode()) {
        throw new TransportError(
          "TCP transport requires Node.js. Use WebSocket (ws://) for browser",
        );
      }
      return new TcpTransport(url, options);
    }
    if (url.startsWith("http://") || url.startsWith("https://")) {
      // Convert HTTP to WebSocket
      const wsUrl = url.replace(/^https?:\/\//, (match) => {
        return match === "https://" ? "wss://" : "ws://";
      });
      return new WebSocketTransport(wsUrl, options);
    }

    // Default to WebSocket
    return new WebSocketTransport(url, options);
  }

  if (transportType === "ws") {
    return new WebSocketTransport(url, options);
  }

  if (transportType === "tcp") {
    if (!isNode()) {
      throw new TransportError("TCP transport requires Node.js. Use WebSocket (ws://) for browser");
    }
    return new TcpTransport(url, options);
  }

  throw new TransportError("Unknown transport type");
}
