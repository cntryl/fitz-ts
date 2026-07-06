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
});
