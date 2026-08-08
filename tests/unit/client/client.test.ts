import { describe, expect, it } from "vite-plus/test";

import { createClient } from "../../../src/client/client";

describe("Client", () => {
  it("defaults the max in-flight request limit when omitted", () => {
    const client = createClient({ url: "ws://example.test" });

    expect(
      (client as unknown as { config: { maxInFlightRequests: number } }).config.maxInFlightRequests,
    ).toBe(256);
  });

  it("preserves the configured max in-flight request limit", () => {
    const client = createClient({ url: "ws://example.test", maxInFlightRequests: 12 });

    expect(
      (client as unknown as { config: { maxInFlightRequests: number } }).config.maxInFlightRequests,
    ).toBe(12);
  });

  it("merges a partial reconnect override with the documented defaults instead of discarding them", () => {
    const client = createClient({ url: "ws://example.test", reconnect: { backoffMs: 500 } });

    expect(
      (
        client as unknown as {
          config: {
            reconnect: {
              enabled: boolean;
              maxAttempts: number;
              backoffMs: number;
              maxBackoffMs: number;
            };
          };
        }
      ).config.reconnect,
    ).toEqual({ enabled: true, maxAttempts: Infinity, backoffMs: 500, maxBackoffMs: 5000 });
  });

  it("merges a partial retry override with the documented defaults instead of discarding them", () => {
    const client = createClient({ url: "ws://example.test", retry: { maxAttempts: 5 } });

    expect(
      (
        client as unknown as {
          config: {
            retry: {
              enabled: boolean;
              maxAttempts: number;
              backoffMs: number;
              maxBackoffMs: number;
            };
          };
        }
      ).config.retry,
    ).toEqual({ enabled: true, maxAttempts: 5, backoffMs: 100, maxBackoffMs: 1000 });
  });

  it("merges a partial heartbeat override with the documented defaults instead of discarding them", () => {
    const client = createClient({ url: "ws://example.test", heartbeat: { intervalMs: 5000 } });

    expect(
      (
        client as unknown as {
          config: { heartbeat: { enabled: boolean; intervalMs: number; timeoutMs: number } };
        }
      ).config.heartbeat,
    ).toEqual({ enabled: true, intervalMs: 5000, timeoutMs: 30000 });
  });

  it("merges a partial asyncHandlers override with the documented defaults, independent of a custom timeout", () => {
    const client = createClient({
      url: "ws://example.test",
      timeout: 5000,
      asyncHandlers: { maxConcurrency: 10 },
    });

    expect(
      (
        client as unknown as {
          config: { asyncHandlers: { maxConcurrency: number; timeoutMs: number } };
        }
      ).config.asyncHandlers,
    ).toEqual({ maxConcurrency: 10, timeoutMs: 30000 });
  });

  it("defaults observability to an empty object when omitted", () => {
    const client = createClient({ url: "ws://example.test" });

    expect(
      (client as unknown as { config: { observability: unknown } }).config.observability,
    ).toEqual({});
  });
});
