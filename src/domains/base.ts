/**
 * Shared utilities for domain clients
 */

import { Connection } from "../client/connection";

export class DomainClient {
  protected connection: Connection;

  constructor(connection: Connection) {
    this.connection = connection;
  }

  protected async requestFrame(
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    return this.connection.request(messageType, payload, signal);
  }

  protected async sendFrame(messageType: number, payload: Uint8Array): Promise<void> {
    return this.connection.send(messageType, payload);
  }
}

export function createDomainClient(connection: Connection) {
  const requestFrame = async (
    messageType: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<Uint8Array> => connection.request(messageType, payload, signal);

  const sendFrame = async (messageType: number, payload: Uint8Array): Promise<void> =>
    connection.send(messageType, payload);

  return {
    connection,
    requestFrame,
    sendFrame,
  };
}
