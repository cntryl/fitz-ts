import { describe, expect, it } from "vite-plus/test";

import "../../../src/core/async-dispose";

describe("async-dispose runtime polyfill", () => {
  it("ensures Symbol.asyncDispose is a real runtime symbol, not just a type augmentation", () => {
    // On a native-ESM runtime this may already be a real symbol; the point
    // of the polyfill is that it's guaranteed to be one either way, so
    // `[Symbol.asyncDispose]`-keyed object literals elsewhere in the SDK
    // (kv transactions, stream sessions) register under the real well-known
    // symbol rather than the string "undefined".
    expect(typeof Symbol.asyncDispose).toBe("symbol");
  });

  it("is safe to import more than once without throwing", async () => {
    await expect(import("../../../src/core/async-dispose")).resolves.toBeDefined();
  });
});
