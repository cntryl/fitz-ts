import { ConnectionState } from "../../core/types";
import { RequestQueueFullError } from "../../core/errors";
import { abortError, throwIfAborted } from "./async";

export interface ReadinessWaiterOptions {
  maxWaiters: number;
  getState: () => ConnectionState;
  getFailure: () => Error | null;
  createTimeoutError: () => Error;
}

export function createReadinessWaiter(options: ReadinessWaiterOptions) {
  const readyListeners = new Set<() => void>();
  let readyWaiterCount = 0;

  const notify = (): void => {
    for (const listener of readyListeners) {
      listener();
    }
  };

  const acquireWaitSlot = (): (() => void) | null => {
    const failure = options.getFailure();
    if (options.getState() === ConnectionState.Authenticated || failure) {
      return null;
    }

    if (readyWaiterCount >= options.maxWaiters) {
      throw new RequestQueueFullError();
    }

    readyWaiterCount += 1;
    let released = false;

    return () => {
      if (released) {
        return;
      }

      released = true;
      readyWaiterCount = Math.max(readyWaiterCount - 1, 0);
    };
  };

  const waitForReady = async (
    signal?: AbortSignal,
    waitTimeoutMs: number = 30000,
  ): Promise<void> => {
    throwIfAborted(signal);
    const immediateFailure = options.getFailure();
    if (!immediateFailure) {
      if (options.getState() === ConnectionState.Authenticated) {
        return;
      }
    } else {
      throw immediateFailure;
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      let timeoutId: ReturnType<typeof setTimeout> | null = null;

      const cleanup = () => {
        readyListeners.delete(onStateChange);
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        signal?.removeEventListener("abort", onAbort);
      };

      const settle = (cb: () => void) => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        cb();
      };

      const onAbort = () => {
        settle(() => reject(abortError()));
      };

      const onStateChange = () => {
        const failure = options.getFailure();
        if (options.getState() === ConnectionState.Authenticated) {
          settle(resolve);
          return;
        }

        if (failure) {
          settle(() => reject(failure));
        }
      };

      readyListeners.add(onStateChange);
      signal?.addEventListener("abort", onAbort, { once: true });
      timeoutId = setTimeout(() => {
        settle(() => reject(options.createTimeoutError()));
      }, waitTimeoutMs);

      onStateChange();
    });
  };

  return {
    acquireWaitSlot,
    waitForReady,
    notify,
  };
}
