import { ConnectionState } from "../../core/types";
import { ConnectionError } from "../../core/errors";

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export const sleepWithAbort = async (ms: number, signal?: AbortSignal): Promise<void> => {
  if (signal?.aborted) {
    throw abortError();
  }

  if (ms <= 0) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);

    const onAbort = () => {
      cleanup();
      reject(abortError());
    };

    const cleanup = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };

    signal?.addEventListener("abort", onAbort, { once: true });
  });
};

export function abortError(): Error {
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

export function connectionClosedError(): ConnectionError {
  return new ConnectionError("Connection closed", { state: ConnectionState.Closed });
}

export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw abortError();
  }
}

export function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

export const waitForSharedPromise = async <T>(
  promise: Promise<T>,
  signal?: AbortSignal,
): Promise<T> => {
  if (!signal) {
    return promise;
  }

  if (signal.aborted) {
    throw abortError();
  }

  return await new Promise<T>((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
      signal.removeEventListener("abort", onAbort);
    };

    const settle = (callback: () => void) => {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      callback();
    };

    const onAbort = () => {
      settle(() => reject(abortError()));
    };

    signal.addEventListener("abort", onAbort, { once: true });

    void promise.then(
      (value) => {
        settle(() => resolve(value));
      },
      (error) => {
        settle(() => reject(error));
      },
    );
  });
};
