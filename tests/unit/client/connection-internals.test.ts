import { describe, expect, it, vi } from "vite-plus/test";

import { createAsyncHandlerDispatcher } from "../../../src/client/internal/async-handler-dispatcher";
import { createHeartbeatLoop } from "../../../src/client/internal/heartbeat";
import { createReadinessWaiter } from "../../../src/client/internal/readiness";
import { createRequestGate } from "../../../src/client/internal/request-gate";
import { ConnectionError, RequestQueueFullError, TransportError } from "../../../src/core/errors";
import { ConnectionState } from "../../../src/core/types";
import type { Transport, TransportHeartbeatOptions } from "../../../src/transport/types";

class HeartbeatTransport implements Transport {
  public closeCalls = 0;
  public heartbeatCalls = 0;
  public heartbeatError: Error | null = null;

  async connect(): Promise<void> {
    return;
  }

  async send(_data: Uint8Array): Promise<void> {
    return;
  }

  async receive(): Promise<Uint8Array> {
    return new Uint8Array();
  }

  async sendHeartbeat(_options: TransportHeartbeatOptions): Promise<void> {
    this.heartbeatCalls += 1;
    if (this.heartbeatError) {
      throw this.heartbeatError;
    }
  }

  supportsHeartbeat(): boolean {
    return true;
  }

  async close(): Promise<void> {
    this.closeCalls += 1;
  }

  getUrl(): string {
    return "memory://heartbeat";
  }

  isConnected(): boolean {
    return true;
  }
}

describe("connection internals", () => {
  it("bounds, aborts, and closes queued request gate waiters", async () => {
    const gate = createRequestGate(1, 1);
    const firstRelease = await gate.acquire();
    const queuedAbort = new AbortController();
    const queued = gate.acquire(queuedAbort.signal);

    await expect(gate.acquire()).rejects.toBeInstanceOf(RequestQueueFullError);

    queuedAbort.abort();
    await expect(queued).rejects.toMatchObject({ name: "AbortError" });

    const queuedForClose = gate.acquire();
    gate.close();
    await expect(queuedForClose).rejects.toBeInstanceOf(ConnectionError);

    firstRelease();
  });

  it("runs async handlers with concurrency limits, drains active tasks, and reports timeouts", async () => {
    vi.useFakeTimers();
    try {
      const errors: unknown[] = [];
      const dispatcher = createAsyncHandlerDispatcher(1, 25, (error) => {
        errors.push(error);
      });
      const events: string[] = [];
      let releaseFirst: () => void = () => undefined;

      dispatcher.dispatch(async () => {
        events.push("first:start");
        await new Promise<void>((resolve) => {
          releaseFirst = resolve;
        });
        events.push("first:end");
      });
      dispatcher.dispatch(() => {
        events.push("second");
      });

      await Promise.resolve();
      expect(events).toEqual(["first:start"]);

      await vi.advanceTimersByTimeAsync(25);
      expect(errors).toHaveLength(1);
      expect(errors[0]).toBeInstanceOf(Error);
      expect((errors[0] as Error).message).toContain("Async handler timeout");

      releaseFirst();
      await dispatcher.drain();
      expect(events).toEqual(["first:start", "first:end", "second"]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("bounds queued async handlers and reports saturation without freeing active slots", async () => {
    vi.useFakeTimers();
    try {
      const errors: unknown[] = [];
      const saturated: Array<{ activeCount: number; queuedCount: number }> = [];
      const dispatcher = createAsyncHandlerDispatcher(
        1,
        25,
        (error) => {
          errors.push(error);
        },
        {
          queueCapacity: 1,
          onSaturated: (metrics) => {
            saturated.push({
              activeCount: metrics.activeCount,
              queuedCount: metrics.queuedCount,
            });
          },
        },
      );
      const events: string[] = [];
      let releaseFirst: () => void = () => undefined;

      expect(
        dispatcher.dispatch(async () => {
          events.push("first:start");
          await new Promise<void>((resolve) => {
            releaseFirst = resolve;
          });
          events.push("first:end");
        }),
      ).toBe(true);
      expect(
        dispatcher.dispatch(() => {
          events.push("second");
        }),
      ).toBe(true);
      expect(
        dispatcher.dispatch(() => {
          events.push("dropped");
        }),
      ).toBe(false);

      expect(dispatcher.getMetrics()).toMatchObject({
        activeCount: 1,
        queuedCount: 1,
        saturationCount: 1,
      });
      expect(saturated).toEqual([{ activeCount: 1, queuedCount: 1 }]);

      await Promise.resolve();
      await vi.advanceTimersByTimeAsync(25);

      expect(errors).toHaveLength(1);
      expect(events).toEqual(["first:start"]);

      releaseFirst();
      await dispatcher.drain();

      expect(events).toEqual(["first:start", "first:end", "second"]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("drops queued async handlers on close but drains active handlers", async () => {
    const dispatcher = createAsyncHandlerDispatcher(1, 1_000, () => undefined, {
      queueCapacity: 1,
    });
    const events: string[] = [];
    let releaseFirst: () => void = () => undefined;

    expect(
      dispatcher.dispatch(async () => {
        events.push("first:start");
        await new Promise<void>((resolve) => {
          releaseFirst = resolve;
        });
        events.push("first:end");
      }),
    ).toBe(true);
    expect(
      dispatcher.dispatch(() => {
        events.push("queued");
      }),
    ).toBe(true);

    await Promise.resolve();
    dispatcher.close();
    releaseFirst();
    await dispatcher.drain();

    expect(events).toEqual(["first:start", "first:end"]);
    expect(dispatcher.getMetrics()).toMatchObject({
      activeCount: 0,
      queuedCount: 0,
    });
  });

  it("bounds readiness waiters and resolves waiters on notification", async () => {
    let state = ConnectionState.Disconnected;
    const waiter = createReadinessWaiter({
      maxWaiters: 1,
      getState: () => state,
      getFailure: () => null,
      createTimeoutError: () => new ConnectionError("ready timeout"),
    });

    const release = waiter.acquireWaitSlot();
    expect(release).toBeTypeOf("function");
    expect(() => waiter.acquireWaitSlot()).toThrow(RequestQueueFullError);
    release?.();

    const ready = waiter.waitForReady(undefined, 1000);
    state = ConnectionState.Authenticated;
    waiter.notify();

    await expect(ready).resolves.toBeUndefined();
  });

  it("reports heartbeat failures and closes the active transport", async () => {
    vi.useFakeTimers();
    try {
      const transport = new HeartbeatTransport();
      transport.heartbeatError = new Error("boom");
      const failures: TransportError[] = [];
      const loop = createHeartbeatLoop({
        enabled: true,
        intervalMs: 10,
        timeoutMs: 20,
        isStopped: () => false,
        sendHeartbeat: async (activeTransport, heartbeat) => {
          await activeTransport.sendHeartbeat!(heartbeat);
        },
        onFailure: (error) => {
          failures.push(error);
        },
        describeError: (error) => (error instanceof Error ? error.message : String(error)),
      });

      loop.start(transport);
      await vi.advanceTimersByTimeAsync(10);

      expect(transport.heartbeatCalls).toBe(1);
      expect(transport.closeCalls).toBe(1);
      expect(failures).toHaveLength(1);
      expect(failures[0]).toBeInstanceOf(TransportError);
      expect(failures[0].message).toContain("Heartbeat failed: boom");

      loop.stop();
    } finally {
      vi.useRealTimers();
    }
  });
});
