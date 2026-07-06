/**
 * Node.js Fitz client facade.
 */

import type { ClientConfig } from "../core/types";
import { createNodeTransport } from "../transport/factory.node";
import type { Client as CoreClient } from "./client-core";
import { createClientConstructor, createClientWithTransport } from "./client-core";

export type Client = CoreClient<ClientConfig>;

export function createClient(config: ClientConfig): Client {
  return createClientWithTransport(config, createNodeTransport);
}

export const Client = createClientConstructor<ClientConfig>(createClient);
