import { describe, expect, it, vi } from "vite-plus/test";

import { Multiplexer } from "../../../src/client/multiplexer";
import { ConnectionError, TimeoutError } from "../../../src/core/errors";
import { MSG_RPC_REQUEST, MSG_RPC_RESPONSE } from "../../../src/frame/types";
import { RpcCodec } from "../../../src/domains/rpc/codec";
import type { FitzMeter, FitzSpan, FitzTracer } from "../../../src/core/types";

class FakeSpan implements FitzSpan {
  public readonly attributes: Record<string, unknown> = {};
  public readonly startedAttributes: Record<string, unknown>;
  public ended = false;
  public exceptions: unknown[] = [];

  constructor(startedAttributes: Record<string, unknown> = {}) {
    this.startedAttributes = startedAttributes;
  }

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

  startSpan(_name: string, attributes?: Record<string, unknown>): FitzSpan {
    const span = new FakeSpan(attributes ?? {});
    this.spans.push(span);
    return span;
  }
}

class FakeMeter implements FitzMeter {
  public counters: string[] = [];
  public histograms: string[] = [];
  public gauges: string[] = [];

  counter(name: string): void {
    this.counters.push(name);
  }

  histogram(name: string): void {
    this.histograms.push(name);
  }

  gauge(name: string): void {
    this.gauges.push(name);
  }
}

describe("Multiplexer", () => {
  it("ignores optional broker replies without treating them as dropped", () => {
    const multiplexer = new Multiplexer();
    multiplexer.setConnected();

    multiplexer.expectOptionalResponse(500);
    multiplexer.dispatch(500, new Uint8Array([0]));

    expect(multiplexer.getMetrics()).toMatchObject({
      responsesDropped: 0,
      responsesIgnored: 1,
    });
  });

  it("ignores late frames after disconnect", () => {
    const multiplexer = new Multiplexer();
    multiplexer.setConnected();
    multiplexer.setDisconnected();

    multiplexer.dispatch(500, new Uint8Array([0]));

    expect(multiplexer.getMetrics()).toMatchObject({
      responsesDropped: 0,
      responsesIgnored: 1,
    });
  });

  it("drops unexpected authenticated frames only after optional responses are exhausted", () => {
    const multiplexer = new Multiplexer();
    multiplexer.setConnected();

    const release = multiplexer.expectOptionalResponse(500);
    release();
    multiplexer.dispatch(500, new Uint8Array([0]));

    expect(multiplexer.getMetrics()).toMatchObject({
      responsesDropped: 1,
      responsesIgnored: 0,
    });
  });

  it("records tracing and metrics for successful requests", async () => {
    const tracer = new FakeTracer();
    const meter = new FakeMeter();
    const multiplexer = new Multiplexer({ tracer, meter });
    multiplexer.setConnected();

    const request = multiplexer.request(77, new Uint8Array([1]), async () => undefined, 100);

    multiplexer.dispatch(77, new Uint8Array([2]));

    await expect(request).resolves.toEqual(new Uint8Array([2]));
    expect(tracer.spans).toHaveLength(1);
    expect(tracer.spans[0].ended).toBe(true);
    expect(tracer.spans[0].startedAttributes).toMatchObject({
      messageType: 77,
    });
    expect(tracer.spans[0].attributes).toMatchObject({
      "fitz.request.duration_ms": expect.any(Number),
    });
    expect(meter.counters).toContain("fitz.request.started");
    expect(meter.counters).toContain("fitz.response.received");
    expect(meter.histograms).toContain("fitz.request.duration");
  });

  it("records timeout failures once and closes the span", async () => {
    vi.useFakeTimers();
    const tracer = new FakeTracer();
    const meter = new FakeMeter();
    const multiplexer = new Multiplexer({ tracer, meter });
    multiplexer.setConnected();

    const request = multiplexer.request(88, new Uint8Array([1]), async () => undefined, 10);

    const assertion = expect(request).rejects.toThrow(/Request timeout/);

    await vi.advanceTimersByTimeAsync(10);
    await assertion;
    expect(tracer.spans).toHaveLength(1);
    expect(tracer.spans[0].ended).toBe(true);
    expect(tracer.spans[0].exceptions).toHaveLength(1);
    expect(meter.counters).toContain("fitz.request.timeout");

    vi.useRealTimers();
  });

  it("records disconnect failures once and closes the span", async () => {
    const tracer = new FakeTracer();
    const meter = new FakeMeter();
    const multiplexer = new Multiplexer({ tracer, meter });
    const controller = new AbortController();
    const removeAbortListener = vi.spyOn(controller.signal, "removeEventListener");
    multiplexer.setConnected();

    const request = multiplexer.request(
      88,
      new Uint8Array([1]),
      async () => undefined,
      1000,
      controller.signal,
    );

    const assertion = expect(request).rejects.toBeInstanceOf(ConnectionError);

    multiplexer.setDisconnected();

    await assertion;
    expect(tracer.spans).toHaveLength(1);
    expect(tracer.spans[0].ended).toBe(true);
    expect(tracer.spans[0].exceptions[0]).toBeInstanceOf(ConnectionError);
    expect(meter.counters).toContain("fitz.request.failed");
    expect(removeAbortListener).toHaveBeenCalledWith("abort", expect.any(Function));
  });

  it("rejects all pending requests on disconnect when the FIFO contains holes", async () => {
    const multiplexer = new Multiplexer();
    const controller = new AbortController();
    multiplexer.setConnected();

    const first = multiplexer.request(77, new Uint8Array([1]), async () => undefined, 1000);
    const second = multiplexer.request(
      77,
      new Uint8Array([2]),
      async () => undefined,
      1000,
      controller.signal,
    );
    const third = multiplexer.request(77, new Uint8Array([3]), async () => undefined, 1000);
    void first.catch(() => undefined);
    void second.catch(() => undefined);
    void third.catch(() => undefined);

    controller.abort();
    await expect(second).rejects.toMatchObject({ name: "AbortError" });

    const firstAssertion = expect(first).rejects.toBeInstanceOf(ConnectionError);
    multiplexer.setDisconnected();

    const thirdResult = await Promise.race<unknown>([
      third.catch((error: unknown) => error),
      new Promise((resolve) => setTimeout(() => resolve("pending"), 0)),
    ]);

    await firstAssertion;
    expect(thirdResult).toBeInstanceOf(ConnectionError);
  });

  it("rejects timed-out requests without waiting for send to finish", async () => {
    vi.useFakeTimers();
    try {
      const multiplexer = new Multiplexer();
      let releaseSend: () => void = () => undefined;
      const sendBlocked = new Promise<void>((resolve) => {
        releaseSend = resolve;
      });
      multiplexer.setConnected();

      const request = multiplexer.request(
        77,
        new Uint8Array([1]),
        async () => {
          await sendBlocked;
        },
        10,
      );
      void request.catch(() => undefined);

      await vi.advanceTimersByTimeAsync(10);
      const resultPromise = Promise.race<unknown>([
        request.catch((error: unknown) => error),
        new Promise((resolve) => setTimeout(() => resolve("pending"), 0)),
      ]);
      await vi.advanceTimersByTimeAsync(0);

      expect(await resultPromise).toBeInstanceOf(TimeoutError);
      releaseSend();
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects aborted requests without waiting for send to finish", async () => {
    const multiplexer = new Multiplexer();
    const controller = new AbortController();
    const sendBlocked = new Promise<void>(() => undefined);
    multiplexer.setConnected();

    const request = multiplexer.request(
      77,
      new Uint8Array([1]),
      async () => {
        await sendBlocked;
      },
      1000,
      controller.signal,
    );
    void request.catch(() => undefined);

    controller.abort();
    const result = await Promise.race<unknown>([
      request.catch((error: unknown) => error),
      new Promise((resolve) => setTimeout(() => resolve("pending"), 0)),
    ]);

    expect(result).toMatchObject({ name: "AbortError" });
  });

  it("routes inbound RPC worker requests to handlers while RPC request acks are pending", async () => {
    let releaseSend: () => void = () => undefined;
    const sendBlocked = new Promise<void>((resolve) => {
      releaseSend = resolve;
    });
    const multiplexer = new Multiplexer();
    const handler = vi.fn();
    const correlationId = new Uint8Array(16).fill(0x42);
    const inboundRequest = RpcCodec.encodeRequest(
      correlationId,
      "rpc://realm/area/worker",
      "",
      new Uint8Array([1, 2, 3]),
    );

    multiplexer.setConnected();
    multiplexer.registerNotificationHandler(MSG_RPC_REQUEST, handler);
    multiplexer.registerPushFrameClassifier(MSG_RPC_REQUEST, (payload) =>
      RpcCodec.isInboundRequestPayload(payload),
    );

    const pending = multiplexer.request(
      MSG_RPC_REQUEST,
      new Uint8Array([9]),
      async () => {
        await sendBlocked;
      },
      1000,
    );

    multiplexer.dispatch(MSG_RPC_REQUEST, inboundRequest);
    expect(handler).toHaveBeenCalledWith(inboundRequest);

    releaseSend();
    await Promise.resolve();
    multiplexer.dispatch(MSG_RPC_REQUEST, new Uint8Array([0]));

    await expect(pending).resolves.toEqual(new Uint8Array([0]));
  });

  it("routes RPC stream responses to handlers while same-type requests are pending", async () => {
    let releaseSend: () => void = () => undefined;
    const sendBlocked = new Promise<void>((resolve) => {
      releaseSend = resolve;
    });
    const multiplexer = new Multiplexer();
    const handler = vi.fn();
    const streamResponse = RpcCodec.encodeResponse(
      new Uint8Array(16).fill(0x33),
      0n,
      new Uint8Array([4, 5, 6]),
      false,
    );

    multiplexer.setConnected();
    multiplexer.registerNotificationHandler(MSG_RPC_RESPONSE, handler);
    multiplexer.registerPushFrameClassifier(MSG_RPC_RESPONSE, (payload) =>
      RpcCodec.isStreamResponsePayload(payload),
    );

    const pending = multiplexer.request(
      MSG_RPC_RESPONSE,
      new Uint8Array([9]),
      async () => {
        await sendBlocked;
      },
      1000,
    );

    multiplexer.dispatch(MSG_RPC_RESPONSE, streamResponse);
    expect(handler).toHaveBeenCalledWith(streamResponse);

    releaseSend();
    await Promise.resolve();
    multiplexer.dispatch(MSG_RPC_RESPONSE, new Uint8Array([0]));

    await expect(pending).resolves.toEqual(new Uint8Array([0]));
  });

  it("preserves FIFO request matching when a registered classifier returns false", async () => {
    const multiplexer = new Multiplexer();
    const handler = vi.fn();
    const response = new Uint8Array([7]);

    multiplexer.setConnected();
    multiplexer.registerNotificationHandler(901, handler);
    multiplexer.registerPushFrameClassifier(901, () => false);

    const pending = multiplexer.request(901, new Uint8Array([1]), async () => undefined, 1000);

    multiplexer.dispatch(901, response);

    await expect(pending).resolves.toEqual(response);
    expect(handler).not.toHaveBeenCalled();
  });
});
