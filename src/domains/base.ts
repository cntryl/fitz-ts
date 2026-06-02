/**
 * Shared utilities for domain clients
 */

import { Connection } from "../client/connection";

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
