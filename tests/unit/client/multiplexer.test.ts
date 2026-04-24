import { describe, expect, it, vi } from "vite-plus/test";

import { Multiplexer } from "../../../src/client/multiplexer";
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
});
