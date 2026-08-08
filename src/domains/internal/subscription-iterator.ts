export interface SubscriptionIteratorOptions {
  signal?: AbortSignal;
}

export function createSubscriptionIterator<T>(
  subscribe: (handler: (item: T) => void) => Promise<{ unsubscribe(): Promise<void> }>,
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

      try {
        while (!options.signal?.aborted) {
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
          while (values.length > 0) {
            yield values.shift()!;
          }
        }
      } finally {
        wake = undefined;
        await subscription.unsubscribe();
      }
    },
  };
}
