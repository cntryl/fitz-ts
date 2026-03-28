import { describe, expect, it } from "vitest";

import { Multiplexer } from "../../../src/client/multiplexer";
import { ConnectionError } from "../../../src/core/errors";

describe("Multiplexer shutdown errors", () => {
  it("rejects pending requests with ConnectionError on disconnect", async () => {
    const multiplexer = new Multiplexer();
    multiplexer.setConnected();

    const pending = multiplexer.request(77, new Uint8Array([1]), async () => undefined, 1000);

    multiplexer.setDisconnected();

    await expect(pending).rejects.toBeInstanceOf(ConnectionError);
  });
});
