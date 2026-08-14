/**
 * Browser Fitz client facade.
 */

import type { ClientConfig } from "../core/types";
import { createBrowserTransport } from "../transport/factory.browser";
import type { Client as CoreClient } from "./client-core";
import { createClientWithTransport } from "./client-core";

/** Browser-safe transport selection. Both values use the browser WebSocket implementation. */
export type BrowserTransportType = "ws" | "auto";

/** Browser WebSocket options; custom upgrade headers are intentionally unavailable. */
export interface BrowserWebSocketOptions {
  /**
   * Browser WebSocket implementations do not allow callers to set upgrade headers.
   */
  headers?: never;
}

/** Browser client configuration with TCP and upgrade-header options removed. */
export interface BrowserClientConfig extends Omit<ClientConfig, "transport" | "webSocket"> {
  /** Browser transport selection. Defaults to `auto`. */
  transport?: BrowserTransportType;
  /** Browser WebSocket options. Intentionally empty; browsers cannot set upgrade headers. */
  webSocket?: BrowserWebSocketOptions;
}

type ValidateBrowserClient<T extends CoreClient<BrowserClientConfig>> = [
  T["config"]["transport"],
] extends [BrowserTransportType]
  ? [BrowserTransportType] extends [T["config"]["transport"]]
    ? [T["config"]["webSocket"]] extends [BrowserWebSocketOptions]
      ? [BrowserWebSocketOptions] extends [T["config"]["webSocket"]]
        ? T
        : never
      : never
    : never
  : never;

/** Fitz client constrained to browser-safe configuration and WebSocket transport. */
export type BrowserClient = ValidateBrowserClient<CoreClient<BrowserClientConfig>>;

/** Creates a lazy browser Fitz client. Call `connect()` before domain operations. */
export function createClient(config: BrowserClientConfig): BrowserClient {
  return createClientWithTransport(config, createBrowserTransport);
}
