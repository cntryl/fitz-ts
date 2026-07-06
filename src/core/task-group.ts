export type TaskGroupStatus = "idle" | "running" | "stopping" | "stopped" | "failed";
export type TaskGroupErrorPolicy = "stop-group" | "restart-task" | "ignore";

export type TaskContext = {
  readonly name: string;
  readonly index: number;
  readonly signal: AbortSignal;
};

export type TaskGroupOptions = {
  readonly name: string;
  readonly concurrency: number;
  readonly errorPolicy?: TaskGroupErrorPolicy;
  readonly run: (ctx: TaskContext) => Promise<void>;
};

import { createDeferred, type Deferred } from "./types";

export type TaskGroup = ReturnType<typeof createTaskGroup>;

type InternalState = {
  status: TaskGroupStatus;
  controller: AbortController | null;
  completion: Deferred<void> | null;
  error: unknown;
};

export function createTaskGroup(options: TaskGroupOptions) {
  const name = options.name;
  const concurrency = options.concurrency;
  const errorPolicy = options.errorPolicy ?? "stop-group";
  const run = options.run;

  if (concurrency < 1) {
    throw new Error("TaskGroup concurrency must be at least 1");
  }

  const state: InternalState = {
    status: "idle",
    controller: null,
    completion: null,
    error: null,
  };

  const tasks = new Set<Promise<void>>();

  const ensureCompletion = (): Deferred<void> => {
    if (!state.completion) {
      state.completion = createDeferred<void>();
    }
    return state.completion;
  };

  const complete = (error?: unknown): void => {
    const completion = ensureCompletion();

    if (error !== undefined) {
      state.status = "failed";
      state.error = error;
      completion.reject(error);
      return;
    }

    state.status = "stopped";
    completion.resolve();
  };

  const maybeFinish = () => {
    if (tasks.size > 0) {
      return;
    }

    if (state.error) {
      complete(state.error);
      return;
    }

    if (state.status === "running" || state.status === "stopping") {
      complete();
      return;
    }
  };

  const track = (promise: Promise<void>): void => {
    tasks.add(promise);
    void promise.finally(() => {
      tasks.delete(promise);
      maybeFinish();
    });
  };

  const shouldRestart = (error: unknown): boolean => {
    if (errorPolicy !== "restart-task") {
      return false;
    }

    if (state.status !== "running" || state.controller?.signal.aborted) {
      return false;
    }

    if (isAbortError(error)) {
      return false;
    }

    return true;
  };

  const handleWorkerError = (error: unknown) => {
    if (isAbortError(error)) {
      return;
    }

    if (state.status === "stopping") {
      return;
    }

    if (errorPolicy === "stop-group") {
      state.error = error;
      state.status = "stopping";
      state.controller?.abort();
      return;
    }
  };

  const runWorker = async (index: number): Promise<void> => {
    while (true) {
      const controller = state.controller;
      if (!controller) {
        return;
      }

      try {
        await run({ name, index, signal: controller.signal });
      } catch (error) {
        if (isAbortError(error) || state.status === "stopping") {
          return;
        }

        if (errorPolicy === "stop-group") {
          handleWorkerError(error);
          return;
        }

        if (errorPolicy === "restart-task") {
          if (shouldRestart(error)) {
            continue;
          }
          return;
        }

        if (errorPolicy === "ignore") {
          return;
        }
      }

      return;
    }
  };

  const start = async (): Promise<void> => {
    if (state.status === "running") {
      throw new Error("TaskGroup already running");
    }

    if (state.status === "stopping") {
      throw new Error("TaskGroup is stopping");
    }

    if (state.status === "failed") {
      throw new Error("TaskGroup has failed and cannot be restarted");
    }

    const previousStatus = state.status;
    state.controller = new AbortController();
    if (previousStatus === "stopped") {
      state.completion = createDeferred<void>();
    } else {
      ensureCompletion();
    }
    state.error = null;
    state.status = "running";

    for (let index = 0; index < concurrency; index += 1) {
      track(runWorker(index));
    }
  };

  const stop = async (reason?: unknown): Promise<void> => {
    void reason;
    if (state.status === "idle") {
      state.status = "stopped";
      ensureCompletion().resolve();
      return;
    }

    if (state.status === "stopped") {
      return ensureCompletion().promise;
    }

    if (state.status === "failed") {
      return ensureCompletion().promise;
    }

    if (state.status === "running") {
      state.status = "stopping";
      state.controller?.abort();
      return ensureCompletion().promise;
    }

    return ensureCompletion().promise;
  };

  const join = async (): Promise<void> => {
    const completion = ensureCompletion();
    return completion.promise;
  };

  const dispose = async (reason?: unknown): Promise<void> => {
    return stop(reason);
  };

  return {
    get name() {
      return name;
    },
    get status() {
      return state.status;
    },
    start,
    stop,
    join,
    dispose,
  };
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}
