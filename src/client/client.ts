/**
 * Node.js Fitz client facade.
 */

import type { ClientConfig } from "../core/types";
import { createNodeTransport } from "../transport/factory.node";
import type { Client as CoreClient } from "./client-core";
import { createClientWithTransport } from "./client-core";

/** Node.js Fitz client, including TCP and WebSocket transport support. */
export type Client = CoreClient<ClientConfig>;

/**
 * Creates a lazy Node.js Fitz client. Call {@link Client.connect} or
 * {@link Client.connectWhenReady} before issuing domain operations, and call
 * {@link Client.close} during shutdown.
 */
export function createClient(config: ClientConfig): Client {
  return createClientWithTransport(config, createNodeTransport);
}
