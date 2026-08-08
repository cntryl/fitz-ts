import { describe, expect, it } from "vite-plus/test";

import {
  awaitPendingUnsubscribe,
  createGenerationCounter,
  createLiveSubIdGetter,
  isCurrentEmptyState,
} from "../../../src/domains/internal/subscription-handle";

describe("createGenerationCounter", () => {
  it("returns a strictly increasing sequence starting at 1", () => {
    const counter = createGenerationCounter();

    expect(counter.next()).toBe(1);
    expect(counter.next()).toBe(2);
    expect(counter.next()).toBe(3);
  });
});

describe("isCurrentEmptyState", () => {
  it("is true only when the map's current entry is the same object and has no handlers", () => {
    const state = { handlers: new Map() };
    const map = new Map([["pattern", state]]);

    expect(isCurrentEmptyState(map, "pattern", state)).toBe(true);
  });

  it("is false when the state still has handlers", () => {
    const state = { handlers: new Map([[1, () => undefined]]) };
    const map = new Map([["pattern", state]]);

    expect(isCurrentEmptyState(map, "pattern", state)).toBe(false);
  });

  it("is false when a concurrent subscribe() replaced the map entry with a different object", () => {
    const staleState = { handlers: new Map() };
    const currentState = { handlers: new Map([[2, () => undefined]]) };
    const map = new Map([["pattern", currentState]]);

    expect(isCurrentEmptyState(map, "pattern", staleState)).toBe(false);
  });

  it("is false when the map no longer has an entry for the key", () => {
    const state = { handlers: new Map() };
    const map = new Map<string, typeof state>();

    expect(isCurrentEmptyState(map, "pattern", state)).toBe(false);
  });
});

describe("createLiveSubIdGetter", () => {
  it("returns the captured subId when no entry has replaced it in the map", () => {
    const map = new Map<string, { subId: bigint; generation: number }>();
    const getSubId = createLiveSubIdGetter(map, "pattern", 1n, 1);

    expect(getSubId()).toBe(1n);
  });

  it("returns the live subId when the map entry survived with the same generation (e.g. reconnect)", () => {
    const map = new Map<string, { subId: bigint; generation: number }>([
      ["pattern", { subId: 99n, generation: 1 }],
    ]);
    const getSubId = createLiveSubIdGetter(map, "pattern", 1n, 1);

    expect(getSubId()).toBe(99n);
  });

  it("falls back to the originally captured subId once a genuinely new subscription (different generation) replaces the entry", () => {
    const map = new Map<string, { subId: bigint; generation: number }>([
      ["pattern", { subId: 42n, generation: 2 }],
    ]);
    const getSubId = createLiveSubIdGetter(map, "pattern", 1n, 1);

    // Generation 2 belongs to an unrelated later subscription that reused
    // the same pattern key after this handle's own subscription was fully
    // torn down — this handle must report its own last-known value, not
    // the new occupant's subId.
    expect(getSubId()).toBe(1n);
  });
});

describe("awaitPendingUnsubscribe", () => {
  it("resolves immediately when no unsubscribe is in flight", async () => {
    await expect(awaitPendingUnsubscribe({})).resolves.toBeUndefined();
  });

  it("waits for a pending unsubscribe to settle successfully", async () => {
    let resolvePending: () => void = () => undefined;
    const pending = new Promise<void>((resolve) => {
      resolvePending = resolve;
    });
    const state = { pendingUnsubscribe: pending };

    let settled = false;
    const waiting = awaitPendingUnsubscribe(state).then(() => {
      settled = true;
    });

    await Promise.resolve();
    expect(settled).toBe(false);

    resolvePending();
    await waiting;
    expect(settled).toBe(true);
  });

  it("does not throw when the pending unsubscribe rejects", async () => {
    const state = { pendingUnsubscribe: Promise.reject(new Error("boom")) };

    await expect(awaitPendingUnsubscribe(state)).resolves.toBeUndefined();
  });
});
