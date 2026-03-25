import { describe, expect, it } from "vitest";

import { ConnectionState } from "../../../src/core/types";
import { ConnectionError } from "../../../src/core/errors";
import { MSG_RPC_REQUEST } from "../../../src/frame/types";
import { RpcClient } from "../../../src/domains/rpc/client";
import { RpcCodec } from "../../../src/domains/rpc/codec";
import type { Connection } from "../../../src/client/connection";

class FakeRpcConnection {
  public readonly notificationHandlers = new Map<
    number,
    (payload: Uint8Array) => void
  >();
  public sendCalls: Array<{ messageType: number; payload: Uint8Array }> = [];
  private state = ConnectionState.Authenticated;

  async request(messageType: number): Promise<Uint8Array> {
    if (messageType === 300 || messageType === 301 || messageType === 302) {
      return new Uint8Array([0]);
    }
    throw new Error(`Unexpected request message type ${messageType}`);
  }

  async send(messageType: number, payload: Uint8Array): Promise<void> {
    this.sendCalls.push({ messageType, payload });
    if (this.state !== ConnectionState.Authenticated) {
      throw new ConnectionError(
        `Cannot use connection while state is ${this.state}`,
      );
    }
  }

  registerNotificationHandler(
    messageType: number,
    handler: (payload: Uint8Array) => void,
  ): void {
    this.notificationHandlers.set(messageType, handler);
  }

  unregisterNotificationHandler(messageType: number): void {
    this.notificationHandlers.delete(messageType);
  }

  onReconnect(): () => void {
    return () => undefined;
  }

  onDisconnect(): () => void {
    return () => undefined;
  }

  dispatchAsyncHandler(task: () => void | Promise<void>): void {
    void Promise.resolve().then(task);
  }

  getState(): ConnectionState {
    return this.state;
  }

  setState(state: ConnectionState): void {
    this.state = state;
  }
}

describe("RpcClient", () => {
  it("swallows worker response sends after the connection closes", async () => {
    const connection = new FakeRpcConnection();
    const client = new RpcClient(connection as unknown as Connection);
    const route = "rpc://realm/area/method";

    await client.registerWorker(route, async (_req, writer) => {
      await writer.send(new Uint8Array([1]), true);
    });

    connection.setState(ConnectionState.Closed);

    const handler = connection.notificationHandlers.get(MSG_RPC_REQUEST);
    expect(handler).toBeTypeOf("function");

    handler!(
      RpcCodec.encodeRequest(
        new Uint8Array(16),
        route,
        "",
        new Uint8Array([9]),
      ),
    );

    await Promise.resolve();
    await Promise.resolve();

    expect(connection.sendCalls).toHaveLength(1);
    expect(connection.sendCalls[0].messageType).toBe(303);
  });
});
