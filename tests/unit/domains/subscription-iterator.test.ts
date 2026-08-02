import { describe, expect, it, vi } from "vite-plus/test";

import { createSubscriptionIterator } from "../../../src/domains/internal/subscription-iterator";

describe("Subscription iterator", () => {
  it("should yield pushed values and unsubscribe given cancellation", async () => {
    // Arrange
    let push: ((value: string) => void) | undefined;
    const unsubscribe = vi.fn(async () => undefined);
    const controller = new AbortController();
    const iterable = createSubscriptionIterator<string>(
      async (handler) => {
        push = handler;
        return { unsubscribe };
      },
      { signal: controller.signal },
    );
    const iterator = iterable[Symbol.asyncIterator]();

    // Act
    const first = iterator.next();
    await vi.waitFor(() => expect(push).toBeTypeOf("function"));
    push!("value");

    // Assert
    await expect(first).resolves.toEqual({ done: false, value: "value" });
    controller.abort();
    await expect(iterator.next()).resolves.toEqual({ done: true, value: undefined });
    expect(unsubscribe).toHaveBeenCalledOnce();
  });
});
