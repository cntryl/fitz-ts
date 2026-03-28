import { describe, expect, it, vi } from "vitest";

import { Connection } from "../../../src/client/connection";
import type {
  FitzLifecycleEvent,
  FitzLogger,
  FitzMeter,
  FitzSpan,
  FitzTracer,
} from "../../../src/core/types";
import { AuthenticationError } from "../../../src/core/errors";
import { MSG_CONNECT } from "../../../src/frame/types";
import type { Transport } from "../../../src/transport/types";

class FakeTransport implements Transport {
  public sent: Uint8Array[] = [];
  public connected = false;
  public concurrentSends = 0;
  public maxConcurrentSends = 0;
  public sendGate: Promise<void> | null = null;
  public gateAfterSends = 0;
  private reads: Array<Uint8Array | Error> = [];
  private pendingRead: {
    resolve: (value: Uint8Array) => void;
    reject: (error: Error) => void;
  } | null = null;

  constructor(reads: Array<Uint8Array | Error> = []) {
    this.reads = reads;
  }

  async connect(): Promise<void> {
    this.connected = true;
  }

  async send(data: Uint8Array): Promise<void> {
    this.concurrentSends += 1;
    this.maxConcurrentSends = Math.max(this.maxConcurrentSends, this.concurrentSends);
    this.sent.push(data);
    if (this.sendGate && this.sent.length > this.gateAfterSends) {
      await this.sendGate;
    }
    this.concurrentSends -= 1;
  }

  async receive(): Promise<Uint8Array> {
    const next = this.reads.shift();
    if (next instanceof Error) {
      this.connected = false;
      throw next;
    }

    if (next) {
      return next;
    }

    return await new Promise<Uint8Array>((resolve, reject) => {
      this.pendingRead = {
        resolve,
        reject: (error: Error) => {
          this.connected = false;
          reject(error);
        },
      };
    });
  }

  async close(): Promise<void> {
    this.connected = false;
    this.pendingRead?.reject(new Error("closed"));
    this.pendingRead = null;
  }

  getUrl(): string {
    return "ws://example.test";
  }

  isConnected(): boolean {
    return this.connected;
  }

  fail(error: Error): void {
    this.pendingRead?.reject(error);
    this.pendingRead = null;
  }
}

class FailingSendTransport extends FakeTransport {
  private sendCount = 0;
  constructor(private readonly failure: Error) {
    super();
  }

  async send(data: Uint8Array): Promise<void> {
    this.sendCount += 1;
    this.sent.push(data);
    if (this.sendCount > 1) {
      throw this.failure;
    }
  }
}

class FakeSpan implements FitzSpan {
  public attributes: Record<string, unknown> = {};
  public exceptions: unknown[] = [];
  public ended = false;

  setAttribute(key: string, value: unknown): void {
    this.attributes[key] = value;
  }

  recordException(error: unknown): void {
    this.exceptions.push(error);
  }

  end(): void {
    this.ended = true;
  }
}

class FakeTracer implements FitzTracer {
  public spans: FakeSpan[] = [];

  startSpan(): FitzSpan {
    const span = new FakeSpan();
    this.spans.push(span);
    return span;
  }
}

class FakeMeter implements FitzMeter {
  public counters: Array<{
    name: string;
    value: number;
    attributes?: Record<string, unknown>;
  }> = [];
  public histograms: Array<{
    name: string;
    value: number;
    attributes?: Record<string, unknown>;
  }> = [];
  public gauges: Array<{
    name: string;
    value: number;
    attributes?: Record<string, unknown>;
  }> = [];

  counter(name: string, value: number, attributes?: Record<string, unknown>): void {
    this.counters.push({ name, value, attributes });
  }

  histogram(name: string, value: number, attributes?: Record<string, unknown>): void {
    this.histograms.push({ name, value, attributes });
  }

  gauge(name: string, value: number, attributes?: Record<string, unknown>): void {
    this.gauges.push({ name, value, attributes });
  }
}

describe("Connection", () => {
  it("authenticates using the token provider and sends CONNECT first", async () => {
    const tokenProvider = vi.fn(async () => "jwt-token");
    const transport = new FakeTransport();
    const connection = new Connection(() => transport, tokenProvider, {
      authSettleDelayMs: 0,
    });

    await connection.connect();

    expect(connection.isConnected()).toBe(true);
    expect(tokenProvider).toHaveBeenCalledTimes(1);
    expect(transport.sent).toHaveLength(1);
    expect(transport.sent[0][0]).toBe(MSG_CONNECT);

    await connection.close();
  });

  it("supports anonymous mode with an empty token", async () => {
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
      },
    );

    await connection.connect();

    expect(connection.isConnected()).toBe(true);
    expect(transport.sent).toHaveLength(1);

    await connection.close();
  });

  it("reconnects and replays reconnect listeners after transport loss", async () => {
    const first = new FakeTransport();
    const second = new FakeTransport();
    const factory = vi.fn<() => Transport>().mockReturnValueOnce(first).mockReturnValueOnce(second);
    const tokenProvider = vi.fn(async () => "jwt-token");
    const restore = vi.fn(async () => undefined);

    const connection = new Connection(factory, tokenProvider, {
      authSettleDelayMs: 0,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });
    connection.onReconnect(restore);

    await connection.connect();
    first.fail(new Error("boom"));
    await vi.waitFor(() => {
      expect(connection.isConnected()).toBe(true);
      expect(factory).toHaveBeenCalledTimes(2);
      expect(restore).toHaveBeenCalledTimes(1);
    });

    await connection.close();
  });

  it("exposes the CONNECTED to AUTHENTICATING to AUTHENTICATED transition sequence", async () => {
    const transport = new FakeTransport();
    const events: FitzLifecycleEvent[] = [];
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        observability: {
          onLifecycleEvent: (event) => {
            events.push(event);
          },
        },
      },
    );

    await connection.connect();

    expect(events.map((event) => [event.event, event.state])).toEqual([
      ["connect_start", "CONNECTING"],
      ["auth_start", "AUTHENTICATING"],
      ["connect_succeeded", "AUTHENTICATED"],
    ]);

    await connection.close();
  });

  it("waits for reconnect restoration before reporting authenticated state", async () => {
    const first = new FakeTransport();
    const second = new FakeTransport();
    const factory = vi.fn<() => Transport>().mockReturnValueOnce(first).mockReturnValueOnce(second);
    let restoreResolved = false;
    let stateDuringRestore: string | null = null;

    const connection = new Connection(factory, async () => "jwt-token", {
      authSettleDelayMs: 0,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });
    connection.onReconnect(async () => {
      stateDuringRestore = connection.getState();
      await Promise.resolve();
      restoreResolved = true;
    });

    await connection.connect();
    first.fail(new Error("boom"));

    await vi.waitFor(() => {
      expect(restoreResolved).toBe(true);
      expect(connection.isConnected()).toBe(true);
    });

    expect(stateDuringRestore).toBe("AUTHENTICATING");

    await connection.close();
  });

  it("transitions to CLOSED and does not reconnect after auth rejection", async () => {
    const transport = new FakeTransport([new Error("connect failed: invalid jwt")]);
    const factory = vi.fn<() => Transport>().mockReturnValue(transport);
    const connection = new Connection(factory, async () => "bad-token", {
      authSettleDelayMs: 50,
      reconnect: {
        enabled: true,
        maxAttempts: 3,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });

    await expect(connection.connect()).rejects.toBeInstanceOf(AuthenticationError);
    expect(connection.getState()).toBe("CLOSED");
    expect(factory).toHaveBeenCalledTimes(1);
  });

  it("emits lifecycle events and logs for connect and close", async () => {
    const transport = new FakeTransport();
    const events: FitzLifecycleEvent[] = [];
    const log = vi.fn();
    const logger: FitzLogger = { log };

    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        observability: {
          logger,
          onLifecycleEvent: (event) => {
            events.push(event);
          },
        },
      },
    );

    await connection.connect();
    await connection.close();

    expect(events.map((event) => event.event)).toEqual([
      "connect_start",
      "auth_start",
      "connect_succeeded",
      "closed",
    ]);
    expect(log).toHaveBeenCalledWith(
      "info",
      "fitz.connection.connect_succeeded",
      expect.objectContaining({ event: "connect_succeeded" }),
    );
  });

  it("emits reconnect lifecycle signals and metrics after transport loss", async () => {
    const first = new FakeTransport();
    const second = new FakeTransport();
    const factory = vi.fn<() => Transport>().mockReturnValueOnce(first).mockReturnValueOnce(second);
    const meter = new FakeMeter();
    const events: FitzLifecycleEvent[] = [];

    const connection = new Connection(factory, async () => "jwt-token", {
      authSettleDelayMs: 0,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
      observability: {
        meter,
        onLifecycleEvent: (event) => {
          events.push(event);
        },
      },
    });

    await connection.connect();
    first.fail(new Error("boom"));

    await vi.waitFor(() => {
      expect(connection.isConnected()).toBe(true);
      expect(events.some((event) => event.event === "connection_lost")).toBe(true);
      expect(events.some((event) => event.event === "reconnect_succeeded")).toBe(true);
    });

    expect(
      meter.counters.some(
        (entry) =>
          entry.name === "fitz.connection.lifecycle" &&
          entry.attributes?.event === "reconnect_succeeded",
      ),
    ).toBe(true);

    await connection.close();
  });

  it("records request tracing and metrics through the multiplexer path", async () => {
    const transport = new FakeTransport();
    const tracer = new FakeTracer();
    const meter = new FakeMeter();
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        observability: {
          tracer,
          meter,
        },
      },
    );

    await connection.connect();
    const pending = connection.request(77, new Uint8Array([1, 2, 3]));
    await Promise.resolve();

    expect(transport.sent).toHaveLength(2);
    connection.getMultiplexer().dispatch(77, new Uint8Array([9]));

    await expect(pending).resolves.toEqual(new Uint8Array([9]));
    expect(tracer.spans).toHaveLength(1);
    expect(tracer.spans[0].ended).toBe(true);
    expect(meter.counters.some((entry) => entry.name === "fitz.request.started")).toBe(true);
    expect(meter.histograms.some((entry) => entry.name === "fitz.request.duration")).toBe(true);

    await connection.close();
  });

  it("logs structured fields for request and send failures", async () => {
    const failure = new Error("write failed") as Error & {
      code?: string;
      domainCode?: number;
    };
    failure.code = "WRITE_FAILED";
    failure.domainCode = 9;

    const transport = new FailingSendTransport(failure);
    const log = vi.fn();
    const logger: FitzLogger = { log };
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        observability: {
          logger,
        },
      },
    );

    await connection.connect();

    await expect(connection.request(93, new Uint8Array([1]))).rejects.toThrow("write failed");
    await expect(connection.send(94, new Uint8Array([2]))).rejects.toThrow("write failed");

    expect(log).toHaveBeenCalledWith(
      "error",
      "fitz.connection.request_failed",
      expect.objectContaining({
        operation: "request",
        state: "AUTHENTICATED",
        messageType: 93,
        latencyMs: expect.any(Number),
        error: "write failed",
        errorName: "Error",
        code: "WRITE_FAILED",
        domainCode: 9,
      }),
    );
    expect(log).toHaveBeenCalledWith(
      "error",
      "fitz.connection.send_failed",
      expect.objectContaining({
        operation: "send",
        state: "AUTHENTICATED",
        messageType: 94,
        latencyMs: expect.any(Number),
        error: "write failed",
        errorName: "Error",
        code: "WRITE_FAILED",
        domainCode: 9,
      }),
    );

    await connection.close();
  });

  it("serializes outbound writes through the connection", async () => {
    let releaseSend: () => void = () => undefined;
    const sendGate = new Promise<void>((resolve) => {
      releaseSend = () => resolve();
    });
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
      },
    );

    await connection.connect();
    transport.sendGate = sendGate;
    transport.gateAfterSends = 1;

    const first = connection.send(90, new Uint8Array([1]));
    const second = connection.send(91, new Uint8Array([2]));
    await Promise.resolve();

    expect(transport.maxConcurrentSends).toBe(1);

    releaseSend();
    await Promise.all([first, second]);
    await connection.close();
  });

  it("rejects an in-flight request when the caller aborts", async () => {
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
      },
    );
    const controller = new AbortController();

    await connection.connect();

    const pending = connection.request(92, new Uint8Array([1, 2, 3]), controller.signal);
    await Promise.resolve();
    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(connection.getMultiplexer().getInFlightCount()).toBe(0);

    await connection.close();
  });

  it("aborts connect when the signal is canceled during auth settle", async () => {
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 5000,
      },
    );
    const controller = new AbortController();

    const pendingConnect = connection.connect({ signal: controller.signal });
    await Promise.resolve();
    controller.abort();

    await expect(pendingConnect).rejects.toMatchObject({ name: "AbortError" });
    expect(connection.isConnected()).toBe(false);
    expect(connection.getState()).toBe("DISCONNECTED");
  });

  it("bounds async handler concurrency", async () => {
    vi.useFakeTimers();
    const transport = new FakeTransport();
    const log = vi.fn();
    const logger: FitzLogger = { log };
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
        asyncHandlers: {
          maxConcurrency: 1,
          timeoutMs: 1000,
        },
        observability: {
          logger,
        },
      },
    );

    let active = 0;
    let maxActive = 0;
    let releaseFirst: () => void = () => undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = () => resolve();
    });

    connection.dispatchAsyncHandler(async () => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await firstGate;
      active -= 1;
    });
    connection.dispatchAsyncHandler(async () => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      active -= 1;
    });

    await Promise.resolve();
    expect(maxActive).toBe(1);

    releaseFirst();
    await vi.runAllTimersAsync();
    await Promise.resolve();
    expect(maxActive).toBe(1);

    vi.useRealTimers();
  });

  it("fires disconnect listeners when close is called", async () => {
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      () => "",
      {
        authSettleDelayMs: 0,
      },
    );
    const onDisconnect = vi.fn();
    connection.onDisconnect(onDisconnect);

    await connection.connect();
    await connection.close();

    expect(onDisconnect).toHaveBeenCalledTimes(1);
  });
});
