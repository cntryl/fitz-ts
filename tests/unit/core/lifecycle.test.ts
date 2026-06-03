import { describe, expect, it } from "vite-plus/test";

import { createScope } from "../../../src/core/lifecycle";

describe("core Scope", () => {
  it("should dispose sync resources given scope disposal", async () => {
    const scope = createScope("scope");
    let cleaned = false;

    scope.add(() => {
      cleaned = true;
    });

    await scope.dispose();

    expect(cleaned).toBe(true);
    expect(scope.disposed).toBe(true);
  });

  it("should dispose async resources given scope disposal", async () => {
    const scope = createScope("scope");
    let cleaned = false;

    scope.add(async () => {
      await Promise.resolve();
      cleaned = true;
    });

    await scope.dispose();

    expect(cleaned).toBe(true);
    expect(scope.disposed).toBe(true);
  });

  it("should cancel the scope signal given scope disposal", async () => {
    const scope = createScope("scope");
    const aborted = new Promise<boolean>((resolve) => {
      scope.signal.addEventListener("abort", () => resolve(true), { once: true });
    });

    await scope.dispose();

    expect(await aborted).toBe(true);
  });

  it("should unsubscribe a subscription group given scope disposal", async () => {
    const scope = createScope("scope");
    const group = scope.createSubscriptionGroup("group");
    let unsubscribed = false;

    const subscription = {
      closed: false,
      unsubscribe: async () => {
        unsubscribed = true;
      },
    };

    group.add(subscription);
    expect(group.size).toBe(1);

    await scope.dispose();

    expect(unsubscribed).toBe(true);
    expect(group.closed).toBe(true);
  });

  it("should dispose child scope and resources given parent scope disposal", async () => {
    const parent = createScope("parent");
    const child = createScope("child", parent);
    let childDisposed = false;

    child.add(() => {
      childDisposed = true;
    });

    await parent.dispose();

    expect(childDisposed).toBe(true);
    expect(child.disposed).toBe(true);
    expect(parent.disposed).toBe(true);
  });
});
