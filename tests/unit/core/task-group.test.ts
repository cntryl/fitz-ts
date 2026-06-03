import { describe, expect, it } from "vite-plus/test";

import { createTaskGroup } from "../../../src/core/task-group";

describe("core TaskGroup", () => {
  it("should run tasks with bounded concurrency given start", async () => {
    let active = 0;
    let maxActive = 0;
    let stoppedCount = 0;

    const group = createTaskGroup({
      name: "test-group",
      concurrency: 2,
      run: async (ctx) => {
        active += 1;
        maxActive = Math.max(maxActive, active);

        await new Promise<void>((resolve) => {
          ctx.signal.addEventListener(
            "abort",
            () => {
              stoppedCount += 1;
              active -= 1;
              resolve();
            },
            { once: true },
          );
        });
      },
    });

    expect(group.status).toBe("idle");

    const joinPromise = group.join();
    void group.start();

    await Promise.resolve();
    expect(group.status).toBe("running");
    expect(maxActive).toBe(2);

    const stopPromise = group.stop("test-stop");
    await stopPromise;
    await joinPromise;

    expect(group.status).toBe("stopped");
    expect(active).toBe(0);
    expect(stoppedCount).toBe(2);
  });

  it("should allow restart after clean stop given a stopped TaskGroup", async () => {
    let runs = 0;

    const group = createTaskGroup({
      name: "restart-group",
      concurrency: 1,
      run: async (ctx) => {
        runs += 1;
        await new Promise<void>((resolve) => {
          const listener = () => resolve();
          ctx.signal.addEventListener("abort", listener, { once: true });
        });
      },
    });

    await group.start();
    await Promise.resolve();
    await group.stop();
    await group.join();

    expect(group.status).toBe("stopped");
    expect(runs).toBe(1);

    await group.start();
    await Promise.resolve();
    await group.stop();
    await group.join();

    expect(group.status).toBe("stopped");
    expect(runs).toBe(2);
  });

  it("should fail the group given stop-group policy and a task throws", async () => {
    const error = new Error("boom");
    const group = createTaskGroup({
      name: "fail-group",
      concurrency: 1,
      errorPolicy: "stop-group",
      run: async () => {
        throw error;
      },
    });

    await group.start();
    await expect(group.join()).rejects.toThrow(error);
    expect(group.status).toBe("failed");
  });

  it("should restart failed tasks given restart-task policy", async () => {
    let attempts = 0;
    let startedCount = 0;
    const resumed = [] as Array<() => void>;

    const group = createTaskGroup({
      name: "restart-task-group",
      concurrency: 1,
      errorPolicy: "restart-task",
      run: async (ctx) => {
        attempts += 1;
        startedCount += 1;

        if (attempts === 1) {
          throw new Error("transient");
        }

        await new Promise<void>((resolve) => {
          resumed.push(resolve);
          const listener = () => resolve();
          ctx.signal.addEventListener("abort", listener, { once: true });
        });
      },
    });

    await group.start();
    await Promise.resolve();

    expect(attempts).toBe(2);
    expect(group.status).toBe("running");

    resumed.forEach((resolve) => resolve());
    await group.stop();
    await group.join();

    expect(group.status).toBe("stopped");
    expect(attempts).toBe(2);
    expect(startedCount).toBe(2);
  });
});
