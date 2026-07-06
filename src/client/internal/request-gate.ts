import { RequestQueueFullError } from "../../core/errors";
import { abortError, connectionClosedError } from "./async";

type RequestWaiter = {
  resolve: (release: () => void) => void;
  reject: (error: Error) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
};

export function createRequestGate(maxConcurrency: number, maxQueueSize: number) {
  let activeCount = 0;
  let closed = false;
  const queue: RequestWaiter[] = [];

  const acquire = async (signal?: AbortSignal): Promise<() => void> => {
    if (signal?.aborted) {
      throw abortError();
    }

    if (closed) {
      throw connectionClosedError();
    }

    return await new Promise<() => void>((resolve, reject) => {
      const grant = () => {
        if (closed) {
          reject(connectionClosedError());
          return;
        }

        activeCount += 1;
        resolve(() => release());
      };

      const waiter: RequestWaiter = {
        resolve,
        reject,
        signal,
        onAbort: undefined,
      };

      const cleanup = () => {
        if (signal && waiter.onAbort) {
          signal.removeEventListener("abort", waiter.onAbort);
        }
      };

      waiter.onAbort = () => {
        removeWaiter(waiter);
        cleanup();
        reject(abortError());
      };

      if (signal) {
        signal.addEventListener("abort", waiter.onAbort, { once: true });
      }

      if (activeCount < maxConcurrency) {
        cleanup();
        grant();
        return;
      }

      if (queue.length >= maxQueueSize) {
        cleanup();
        reject(new RequestQueueFullError());
        return;
      }

      queue.push(waiter);
    });
  };

  const close = (): void => {
    if (closed) {
      return;
    }

    closed = true;
    const error = connectionClosedError();
    for (const waiter of queue.splice(0)) {
      if (waiter.signal && waiter.onAbort) {
        waiter.signal.removeEventListener("abort", waiter.onAbort);
      }
      waiter.reject(error);
    }
  };

  const release = (): void => {
    if (activeCount > 0) {
      activeCount -= 1;
    }

    while (!closed && activeCount < maxConcurrency) {
      const waiter = queue.shift();
      if (!waiter) {
        return;
      }

      if (waiter.signal?.aborted) {
        if (waiter.signal && waiter.onAbort) {
          waiter.signal.removeEventListener("abort", waiter.onAbort);
        }
        continue;
      }

      if (waiter.signal && waiter.onAbort) {
        waiter.signal.removeEventListener("abort", waiter.onAbort);
      }

      activeCount += 1;
      waiter.resolve(() => release());
      return;
    }
  };

  const removeWaiter = (waiter: RequestWaiter): void => {
    const index = queue.indexOf(waiter);
    if (index >= 0) {
      queue.splice(index, 1);
    }
  };

  return {
    acquire,
    close,
  };
}
