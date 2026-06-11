import { describe, expect, it, vi } from "vite-plus/test";

import { createWakeGate } from "../../../src/core/wake-gate";

describe("createWakeGate", () => {
  it("increments the version and wakes all current waiters", async () => {
    const gate = createWakeGate();
    const first = gate.waitAfter(gate.version);
    const second = gate.waitAfter(gate.version);

    const version = gate.wake();

    await expect(first).resolves.toBe(version);
    await expect(second).resolves.toBe(version);
    expect(gate.version).toBe(version);
  });

  it("resolves immediately when waiting after an old version", async () => {
    const gate = createWakeGate();
    const observed = gate.version;
    const version = gate.wake();

    await expect(gate.waitAfter(observed)).resolves.toBe(version);
  });

  it("does not lose a wake between observing and waiting", async () => {
    const gate = createWakeGate();
    const observed = gate.version;

    gate.wake();

    await expect(gate.waitAfter(observed)).resolves.toBe(gate.version);
  });

  it("rejects aborted waits and detaches abort listeners", async () => {
    const gate = createWakeGate();
    const controller = new AbortController();
    const addSpy = vi.spyOn(controller.signal, "addEventListener");
    const removeSpy = vi.spyOn(controller.signal, "removeEventListener");
    const pending = gate.waitAfter(gate.version, { signal: controller.signal });

    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(addSpy).toHaveBeenCalledWith("abort", expect.any(Function), { once: true });
    expect(removeSpy).toHaveBeenCalledWith("abort", expect.any(Function));
  });
});
