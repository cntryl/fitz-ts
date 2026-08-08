export interface AsyncHandlerDispatcherMetrics {
  activeCount: number;
  queuedCount: number;
  saturationCount: number;
}

export interface AsyncHandlerDispatcherOptions {
  queueCapacity?: number;
  onSaturated?: (metrics: AsyncHandlerDispatcherMetrics) => void;
  onMetricsChange?: (metrics: AsyncHandlerDispatcherMetrics) => void;
}

export function createAsyncHandlerDispatcher(
  maxConcurrency: number,
  timeoutMs: number,
  onError: (error: unknown) => void,
  options: AsyncHandlerDispatcherOptions = {},
) {
  const concurrency = Number.isFinite(maxConcurrency)
    ? Math.max(1, Math.floor(maxConcurrency))
    : Infinity;
  const queueCapacity =
    options.queueCapacity === undefined || !Number.isFinite(options.queueCapacity)
      ? Infinity
      : Math.max(0, Math.floor(options.queueCapacity));
  let activeCount = 0;
  let saturationCount = 0;
  let closed = false;
  const queue: Array<() => void> = [];
  const activeTasks = new Set<Promise<void>>();

  const getMetrics = (): AsyncHandlerDispatcherMetrics => ({
    activeCount,
    queuedCount: queue.length,
    saturationCount,
  });

  const emitMetrics = (): void => {
    options.onMetricsChange?.(getMetrics());
  };

  const dispatch = (task: () => void | Promise<void>): boolean => {
    if (closed) {
      return false;
    }

    const run = () => {
      if (closed) {
        return;
      }

      activeCount += 1;
      emitMetrics();
      const activeTask = runTask(task).finally(() => {
        activeTasks.delete(activeTask);
        activeCount -= 1;
        flush();
        emitMetrics();
      });
      activeTasks.add(activeTask);
      void activeTask;
    };

    if (activeCount < concurrency) {
      run();
      return true;
    }

    if (queue.length >= queueCapacity) {
      saturationCount += 1;
      options.onSaturated?.(getMetrics());
      emitMetrics();
      return false;
    }

    queue.push(run);
    emitMetrics();
    return true;
  };

  const flush = (): void => {
    if (closed) {
      if (queue.length > 0) {
        queue.length = 0;
        emitMetrics();
      }
      return;
    }

    while (activeCount < concurrency) {
      const next = queue.shift();
      if (!next) {
        return;
      }

      next();
    }
  };

  const runTask = (task: () => void | Promise<void>): Promise<void> => {
    let reported = false;
    const reportOnce = (error: unknown): void => {
      if (reported) {
        return;
      }
      reported = true;
      onError(error);
    };

    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    const taskPromise = Promise.resolve()
      .then(task)
      .catch((error: unknown) => {
        reportOnce(error);
      })
      .finally(() => {
        clearTimeout(timeoutId);
      });

    // Report a timeout promptly, but do NOT let it settle the promise this
    // function returns — dispatch()'s caller frees the concurrency slot and
    // (via drain()) considers the handler finished as soon as that promise
    // settles. `asyncHandlers.maxConcurrency` is a hard bound
    // (PUBLIC_CONTRACT.md's "Async handler fan-out is bounded by..."), so a
    // slot must stay occupied for as long as the real handler invocation is
    // still running, even after it has already been reported as timed out —
    // otherwise repeated hung handlers could run unboundedly past the
    // configured concurrency limit. reportOnce's dedup guard ensures the
    // task's own eventual failure (if any) doesn't double-report.
    timeoutId = setTimeout(() => {
      reportOnce(new Error(`Async handler timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    return taskPromise;
  };

  const close = (): void => {
    closed = true;
    if (queue.length > 0) {
      queue.length = 0;
      emitMetrics();
    }
  };

  const drain = async (): Promise<void> => {
    while (activeTasks.size > 0) {
      await Promise.allSettled(Array.from(activeTasks));
    }
  };

  return {
    dispatch,
    close,
    drain,
    getMetrics,
  };
}
