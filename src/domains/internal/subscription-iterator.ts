/** Cancellation options shared by public notification iterators. */
export interface SubscriptionIteratorOptions {
  /** Aborts pending iteration and unsubscribes this iterator. */
  signal?: AbortSignal;
}

export function createSubscriptionIterator<T>(
  subscribe: (handler: (item: T) => void) => Promise<{
    readonly completion: Promise<void>;
    unsubscribe(): Promise<void>;
  }>,
  options: SubscriptionIteratorOptions = {},
): AsyncIterable<T> {
  return {
    async *[Symbol.asyncIterator]() {
      const values: T[] = [];
      let wake: (() => void) | undefined;
      const subscription = await subscribe((value) => {
        values.push(value);
        wake?.();
        wake = undefined;
      });
      let completionSettled = false;
      let completionFailure: unknown;
      const observeCompletion = subscription.completion.then(
        () => {
          completionSettled = true;
          wake?.();
          wake = undefined;
        },
        (error: unknown) => {
          completionFailure = error;
          wake?.();
          wake = undefined;
        },
      );

      try {
        while (!options.signal?.aborted) {
          if (completionFailure !== undefined) throw completionFailure;
          if (completionSettled) return;
          if (values.length === 0) {
            await new Promise<void>((resolve) => {
              const signal = options.signal;
              const settle = (): void => {
                signal?.removeEventListener("abort", settle);
                resolve();
              };
              wake = settle;
              signal?.addEventListener("abort", settle, { once: true });
            });
          }
          if (completionFailure !== undefined) throw completionFailure;
          if (completionSettled) return;
          while (values.length > 0) {
            yield values.shift()!;
          }
        }
      } finally {
        wake = undefined;
        await subscription.unsubscribe();
        await observeCompletion;
      }
    },
  };
}
