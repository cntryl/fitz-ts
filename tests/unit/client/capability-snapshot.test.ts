import { expect, it } from "vite-plus/test";
import { createMultiplexer } from "../../../src/client/multiplexer";

it("prevents a capability snapshot from changing negotiated behavior", () => {
  const mux = createMultiplexer();
  mux.setCapabilities(3, 0, [1, 2]);
  const snapshot = mux.getCapabilities();
  expect(Reflect.set(snapshot.capabilityIds, "0", 99)).toBe(false);
  expect(mux.getCapabilities().capabilityIds).toEqual([1, 2]);
});
