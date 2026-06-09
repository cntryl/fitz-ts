import { describe, expect, it, vi } from "vite-plus/test";

import { Connection } from "../../../src/client/connection";
import type {
  FitzLifecycleEvent,
  FitzLogger,
  FitzMeter,
  FitzSpan,
  FitzTracer,
} from "../../../src/core/types";
import { BufferWriter } from "../../../src/core/buffer";
import { AuthenticationError } from "../../../src/core/errors";
import { NoticeClient } from "../../../src/domains/notice/client";
import { FrameCodec } from "../../../src/frame/codec";
import { MSG_CONNECT, MSG_NOTICE_NOTIFY, MSG_NOTICE_SUBSCRIBE } from "../../../src/frame/types";
import type { Transport } from "../../../src/transport/types";

class FakeTransport implements Transport {
  public sent: Uint8Array[] = [];
  public connected = false;
  public connectStarted = false;
  public connectGate: Promise<void> | null = null;
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
    this.connectStarted = true;
    if (this.connectGate) {
      await this.connectGate;
    }
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

  pushRead(data: Uint8Array): void {
    if (this.pendingRead) {
      this.pendingRead.resolve(data);
      this.pendingRead = null;
      return;
    }

    this.reads.push(data);
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

function encodeNoticeSubscribeResponse(subId: bigint): Uint8Array {
  const payload = new Uint8Array(10);
  payload[0] = 0;
  payload[1] = 1;
  const view = new DataView(payload.buffer);
  view.setBigUint64(2, subId, false);
  return payload;
}

function encodeNoticeNotification(subId: bigint, route: string, body: Uint8Array): Uint8Array {
  const writer = new BufferWriter(128);
  writer.writeU64BE(subId);
  writer.writeString(route);
  writer.writeU32BE(body.length);
  writer.writeBytes(body);
  return writer.getBuffer();
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

  it("coalesces concurrent initial connect calls onto one transport dial", async () => {
    const transport = new FakeTransport();
    let releaseConnect: () => void = () => undefined;
    transport.connectGate = new Promise<void>((resolve) => {
      releaseConnect = resolve;
    });
    const factory = vi.fn<() => Transport>().mockReturnValue(transport);
    const connection = new Connection(factory, async () => "jwt-token", {
      authSettleDelayMs: 0,
    });

    const firstConnect = connection.connect();
    const secondConnect = connection.connect();

    expect(factory).toHaveBeenCalledTimes(1);
    expect(transport.connectStarted).toBe(true);

    releaseConnect();
    await Promise.all([firstConnect, secondConnect]);

    expect(connection.isConnected()).toBe(true);

    await connection.close();
  });

  it("does not let an aborted secondary waiter cancel a shared initial connect", async () => {
    const transport = new FakeTransport();
    let releaseConnect: () => void = () => undefined;
    transport.connectGate = new Promise<void>((resolve) => {
      releaseConnect = resolve;
    });
    const factory = vi.fn<() => Transport>().mockReturnValue(transport);
    const connection = new Connection(factory, async () => "jwt-token", {
      authSettleDelayMs: 0,
    });

    const firstConnect = connection.connect();
    const controller = new AbortController();
    const secondConnect = connection.connect({ signal: controller.signal });

    controller.abort();
    await expect(secondConnect).rejects.toHaveProperty("name", "AbortError");

    releaseConnect();
    await expect(firstConnect).resolves.toBeUndefined();
    expect(factory).toHaveBeenCalledTimes(1);
    expect(connection.isConnected()).toBe(true);

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

  it("does not reconnect after close during reconnect backoff", async () => {
    const first = new FakeTransport();
    const second = new FakeTransport();
    const factory = vi.fn<() => Transport>().mockReturnValueOnce(first).mockReturnValueOnce(second);

    const connection = new Connection(factory, async () => "jwt-token", {
      authSettleDelayMs: 1,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 50,
        maxBackoffMs: 50,
      },
    });

    await connection.connect();
    first.fail(new Error("boom"));

    await vi.waitFor(() => {
      expect(connection.getState()).toBe("RECONNECTING");
    });

    await connection.close();
    await new Promise((resolve) => setTimeout(resolve, 75));

    expect(factory).toHaveBeenCalledTimes(1);
    expect(second.connected).toBe(false);
    expect(connection.getState()).toBe("CLOSED");
  });

  it("does not continue reconnecting if close wins during transport connect", async () => {
    const first = new FakeTransport();
    const second = new FakeTransport();
    const factory = vi.fn<() => Transport>().mockReturnValueOnce(first).mockReturnValueOnce(second);
    let releaseConnect: () => void = () => undefined;

    second.connectGate = new Promise<void>((resolve) => {
      releaseConnect = resolve;
    });

    const connection = new Connection(factory, async () => "jwt-token", {
      authSettleDelayMs: 0,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });

    await connection.connect();
    first.fail(new Error("boom"));

    await vi.waitFor(() => {
      expect(second.connectStarted).toBe(true);
      expect(connection.getState()).toBe("RECONNECTING");
    });

    await connection.close();
    releaseConnect();
    await Promise.resolve();
    await Promise.resolve();

    expect(second.sent).toHaveLength(0);
    expect(second.connected).toBe(false);
    expect(connection.getState()).toBe("CLOSED");
    expect(factory).toHaveBeenCalledTimes(2);
  });

  it("waits on the active reconnect instead of starting a foreground reconnecting connect", async () => {
    const first = new FakeTransport();
    const second = new FakeTransport();
    const third = new FakeTransport();
    const factory = vi
      .fn<() => Transport>()
      .mockReturnValueOnce(first)
      .mockReturnValueOnce(second)
      .mockReturnValueOnce(third);
    let releaseReconnectConnect: () => void = () => undefined;

    second.connectGate = new Promise<void>((resolve) => {
      releaseReconnectConnect = resolve;
    });

    const connection = new Connection(factory, async () => "jwt-token", {
      authSettleDelayMs: 0,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });

    await connection.connect();
    first.fail(new Error("boom"));

    await vi.waitFor(() => {
      expect(second.connectStarted).toBe(true);
      expect(connection.getState()).toBe("RECONNECTING");
    });

    const waitingConnect = connection.connect();
    await Promise.resolve();

    expect(factory).toHaveBeenCalledTimes(2);
    expect(third.connectStarted).toBe(false);

    releaseReconnectConnect();
    await expect(waitingConnect).resolves.toBeUndefined();
    expect(connection.isConnected()).toBe(true);

    await connection.close();
  });

  it("replays reconnect listeners that issue request/response traffic before reporting authenticated", async () => {
    const first = new FakeTransport();
    const second = new FakeTransport();
    const factory = vi.fn<() => Transport>().mockReturnValueOnce(first).mockReturnValueOnce(second);

    const connection = new Connection(factory, async () => "", {
      authSettleDelayMs: 0,
      reconnect: {
        enabled: true,
        maxAttempts: 1,
        backoffMs: 0,
        maxBackoffMs: 0,
      },
    });
    const notice = new NoticeClient(connection);
    const received: string[] = [];

    await connection.connect();

    const subscriptionPromise = notice.subscribe("notice://realm/area/resource", async (msg) => {
      received.push(Buffer.from(msg.body).toString());
    });

    await vi.waitFor(() => {
      expect(first.sent).toHaveLength(2);
      expect(FrameCodec.decodeFrame(first.sent[1]).messageType).toBe(MSG_NOTICE_SUBSCRIBE);
    });
    first.pushRead(FrameCodec.encodeFrame(MSG_NOTICE_SUBSCRIBE, encodeNoticeSubscribeResponse(1n)));

    await subscriptionPromise;

    first.fail(new Error("boom"));

    await vi.waitFor(() => {
      expect(second.connectStarted).toBe(true);
    });

    await vi.waitFor(() => {
      expect(second.sent).toHaveLength(2);
      expect(FrameCodec.decodeFrame(second.sent[1]).messageType).toBe(MSG_NOTICE_SUBSCRIBE);
    });
    second.pushRead(
      FrameCodec.encodeFrame(MSG_NOTICE_SUBSCRIBE, encodeNoticeSubscribeResponse(2n)),
    );

    await vi.waitFor(() => {
      expect(connection.isConnected()).toBe(true);
    });

    second.pushRead(
      FrameCodec.encodeFrame(
        MSG_NOTICE_NOTIFY,
        encodeNoticeNotification(
          2n,
          "notice://realm/area/resource",
          Buffer.from("after-reconnect"),
        ),
      ),
    );

    await vi.waitFor(() => {
      expect(received).toEqual(["after-reconnect"]);
    });

    await connection.close();
  });

  it("bounds concurrent outbound requests to the configured limit", async () => {
    const transport = new FakeTransport();
    const connection = new Connection(
      () => transport,
      async () => "",
      {
        authSettleDelayMs: 0,
        maxInFlightRequests: 1,
      },
    );

    await connection.connect();

    const firstRequest = connection.request(77, new Uint8Array([1]));
    await vi.waitFor(() => {
      expect(transport.sent).toHaveLength(2);
    });

    const controller = new AbortController();
    const secondRequest = connection.request(77, new Uint8Array([2]), controller.signal);
    const abortTimer = setTimeout(() => controller.abort(), 20);

    await expect(secondRequest).rejects.toThrow(/aborted/i);
    clearTimeout(abortTimer);
    expect(transport.sent).toHaveLength(2);

    transport.pushRead(FrameCodec.encodeFrame(77, new Uint8Array([9])));
    await expect(firstRequest).resolves.toEqual(new Uint8Array([9]));

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
    await vi.waitFor(() => {
      expect(transport.sent).toHaveLength(2);
    });
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
    await vi.waitFor(() => {
      expect(transport.sent).toHaveLength(2);
    });
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

  it("keeps timed-out async handlers in the concurrency slot until they finish", async () => {
    vi.useFakeTimers();
    try {
      const transport = new FakeTransport();
      const log = vi.fn();
      const logger: FitzLogger = { log };
      const connection = new Connection(
        () => transport,
        () => "",
        {
          asyncHandlers: {
            maxConcurrency: 1,
            timeoutMs: 1000,
          },
          observability: {
            logger,
          },
        },
      );

      let releaseFirst: () => void = () => undefined;
      let secondStarted = false;
      const firstGate = new Promise<void>((resolve) => {
        releaseFirst = resolve;
      });

      connection.dispatchAsyncHandler(async () => {
        await firstGate;
      });
      connection.dispatchAsyncHandler(() => {
        secondStarted = true;
      });

      await Promise.resolve();
      expect(secondStarted).toBe(false);

      await vi.advanceTimersByTimeAsync(1000);
      await Promise.resolve();
      expect(secondStarted).toBe(false);
      expect(log).toHaveBeenCalledWith(
        "warn",
        "fitz.connection.handler_failed",
        expect.objectContaining({
          error: "Async handler timeout after 1000ms",
        }),
      );

      releaseFirst();
      await vi.runAllTimersAsync();
      await Promise.resolve();
      expect(secondStarted).toBe(true);
    } finally {
      vi.useRealTimers();
    }
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
