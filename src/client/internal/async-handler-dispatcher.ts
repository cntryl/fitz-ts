export function createAsyncHandlerDispatcher(
  maxConcurrency: number,
  timeoutMs: number,
  onError: (error: unknown) => void,
) {
  let activeCount = 0;
  let closed = false;
  const queue: Array<() => void> = [];
  const activeTasks = new Set<Promise<void>>();

  const dispatch = (task: () => void | Promise<void>): void => {
    if (closed) {
      return;
    }

    const run = () => {
      if (closed) {
        return;
      }

      activeCount += 1;
      const activeTask = runTask(task).finally(() => {
        activeTasks.delete(activeTask);
        activeCount -= 1;
        flush();
      });
      activeTasks.add(activeTask);
      void activeTask;
    };

    if (activeCount < maxConcurrency) {
      run();
      return;
    }

    queue.push(run);
  };

  const flush = (): void => {
    if (closed) {
      queue.length = 0;
      return;
    }

    if (activeCount >= maxConcurrency) {
      return;
    }

    const next = queue.shift();
    next?.();
  };

  const runTask = async (task: () => void | Promise<void>): Promise<void> => {
    let reported = false;
    const reportOnce = (error: unknown): void => {
      if (reported) {
        return;
      }
      reported = true;
      onError(error);
    };

    const timeoutId = setTimeout(() => {
      reportOnce(new Error(`Async handler timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    try {
      await Promise.resolve().then(task);
    } catch (error) {
      reportOnce(error);
    } finally {
      clearTimeout(timeoutId);
    }
  };

  const close = (): void => {
    closed = true;
    queue.length = 0;
  };

  const drain = async (): Promise<void> => {
    await Promise.allSettled(Array.from(activeTasks));
  };

  return {
    dispatch,
    close,
    drain,
  };
}
