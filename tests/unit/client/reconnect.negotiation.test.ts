import { describe, expect, it } from "vite-plus/test";
import { createReconnectScheduler } from "../../../src/client/internal/reconnect.js";
import { ProtocolError } from "../../../src/core/errors.js";
import { ConnectionState } from "../../../src/core/types.js";

describe("reconnect negotiation", () => {
  it("stops after the broker rejects the wire version", async () => {
    let attempts = 0;
    let state = ConnectionState.Disconnected;
    const scheduler = createReconnectScheduler({
      maxAttempts: 3,
      backoffMs: 1,
      maxBackoffMs: 1,
      closeSignal: new AbortController().signal,
      isCloseRequested: () => false,
      setState: (next) => {
        state = next;
      },
      openAndAuthenticate: async () => {
        attempts++;
        throw new ProtocolError("no common wire version", 50);
      },
      emitLifecycleEvent: () => undefined,
      logRetry: () => undefined,
    });

    const result = await scheduler.runLoop();

    expect(result).toBe("exhausted");
    expect(state).toBe(ConnectionState.Disconnected);
    expect(attempts).toBe(1);
  });
});
