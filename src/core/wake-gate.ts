export interface WakeWaitOptions {
  signal?: AbortSignal;
}

export interface WakeGate {
  readonly version: number;
  wake(): number;
  waitAfter(version: number, options?: WakeWaitOptions): Promise<number>;
  wait(options?: WakeWaitOptions): Promise<number>;
}

type Waiter = {
  observedVersion: number;
  resolve: (version: number) => void;
  reject: (error: Error) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
};

function abortError(): Error {
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
}

export function createWakeGate(): WakeGate {
  let currentVersion = 0;
  const waiters = new Set<Waiter>();

  const cleanup = (waiter: Waiter): void => {
    waiters.delete(waiter);
    if (waiter.signal && waiter.onAbort) {
      waiter.signal.removeEventListener("abort", waiter.onAbort);
    }
  };

  const wake = (): number => {
    currentVersion += 1;
    const version = currentVersion;

    for (const waiter of Array.from(waiters)) {
      cleanup(waiter);
      waiter.resolve(version);
    }

    return version;
  };

  const waitAfter = async (
    observedVersion: number,
    options: WakeWaitOptions = {},
  ): Promise<number> => {
    if (currentVersion > observedVersion) {
      return currentVersion;
    }

    if (options.signal?.aborted) {
      throw abortError();
    }

    return await new Promise<number>((resolve, reject) => {
      const waiter: Waiter = {
        observedVersion,
        resolve,
        reject,
        signal: options.signal,
      };

      waiter.onAbort = () => {
        cleanup(waiter);
        reject(abortError());
      };

      if (options.signal) {
        options.signal.addEventListener("abort", waiter.onAbort, { once: true });
      }

      if (currentVersion > observedVersion) {
        cleanup(waiter);
        resolve(currentVersion);
        return;
      }

      waiters.add(waiter);
    });
  };

  const wait = async (options?: WakeWaitOptions): Promise<number> => {
    return await waitAfter(currentVersion, options);
  };

  return {
    get version() {
      return currentVersion;
    },
    wake,
    waitAfter,
    wait,
  };
}
