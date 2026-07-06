/**
 * Browser Fitz client facade.
 */

import type { ClientConfig } from "../core/types";
import { createBrowserTransport } from "../transport/factory.browser";
import type { Client as CoreClient } from "./client-core";
import { createClientConstructor, createClientWithTransport } from "./client-core";

export type BrowserTransportType = "ws" | "auto";

export interface BrowserWebSocketOptions {
  /**
   * Browser WebSocket implementations do not allow callers to set upgrade headers.
   */
  headers?: never;
}

export interface BrowserClientConfig extends Omit<ClientConfig, "transport" | "webSocket"> {
  transport?: BrowserTransportType;
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

export type BrowserClient = ValidateBrowserClient<CoreClient<BrowserClientConfig>>;

export function createClient(config: BrowserClientConfig): BrowserClient {
  return createClientWithTransport(config, createBrowserTransport);
}

export const Client = createClientConstructor<BrowserClientConfig>(createClient);
