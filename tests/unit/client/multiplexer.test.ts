import { describe, expect, it } from "vitest";

import { Multiplexer } from "../../../src/client/multiplexer";

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
});
