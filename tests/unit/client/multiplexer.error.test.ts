import { describe, expect, it } from "vite-plus/test";

import { Multiplexer } from "../../../src/client/multiplexer";
import { ConnectionError } from "../../../src/core/errors";

describe("Multiplexer shutdown errors", () => {
  it("rejects pending requests with ConnectionError on disconnect", async () => {
    const multiplexer = new Multiplexer();
    multiplexer.setConnected();

    const pending = multiplexer.request(77, new Uint8Array([1]), async () => undefined, 1000);
    void pending.catch(() => undefined);
    const pendingExpectation = expect(pending).rejects.toBeInstanceOf(ConnectionError);

    multiplexer.setDisconnected();

    await pendingExpectation;
  });
});
