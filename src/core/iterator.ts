/**
 * Iterator[T] is a generic streaming iterator modeled after fitz-go's Iterator[T].
 *
 * Usage pattern (manual):
 * ```ts
 * const it = await tx.scan(prefix, 100);
 * try {
 *   while (it.next()) {
 *     const value = it.value();
 *     // use value
 *   }
 *   if (it.err()) throw it.err();
 * } finally {
 *   it.close();
 * }
 * ```
 *
 * Usage pattern (with forEach helper):
 * ```ts
 * const it = await tx.scan(prefix, 100);
 * return forEach(it, (value) => {
 *   // use value
 *   return Promise.resolve(undefined);
 * });
 * ```
 */
export interface Iterator<T> {
  /**
   * Advances the iterator and returns true if a value is available.
   */
  next(): boolean;

  /**
   * Returns the current item (valid only after a successful next()).
   */
  value(): T;

  /**
   * Returns the first non-EOF error encountered.
   */
  err(): Error | null;

  /**
   * Closes/releases any resources associated with the iterator.
   */
  close(): void;
}

/**
 * SliceIterator iterates over an in-memory slice/array.
 * Used for batch results like KV SCAN where all items arrive in one response.
 */
export function createSliceIterator<T>(items: T[]): Iterator<T> {
  let index = -1;

  const next = (): boolean => {
    index += 1;
    return index < items.length;
  };

  const value = (): T => {
    if (index < 0 || index >= items.length) {
      throw new Error("SliceIterator.value() called in invalid state");
    }
    return items[index]!;
  };

  const err = (): Error | null => null;

  const close = (): void => {
    // No-op: no resources to release
  };

  return {
    next,
    value,
    err,
    close,
  };
}

export type AsyncIterableIterator<T> = AsyncIterable<T>;

export function createAsyncIterableIterator<T>(iterator: Iterator<T>): AsyncIterableIterator<T> {
  return {
    async *[Symbol.asyncIterator]() {
      while (iterator.next()) {
        yield iterator.value();
      }

      const err = iterator.err();
      iterator.close();
      if (err) {
        throw err;
      }
    },
  };
}

/**
 * forEach iterates over all items in the iterator, calling fn for each.
 * Automatically handles close() and error checking.
 * Iteration stops on first error from either callback or iterator.
 *
 * Example:
 * ```ts
 * const it = await tx.scan(startKey, 100);
 * return forEach(it, async (kv) => {
 *   console.log(`${kv.key}: ${kv.value}`);
 * });
 * ```
 */
export async function forEach<T>(it: Iterator<T>, fn: (item: T) => Promise<void>): Promise<void> {
  try {
    while (it.next()) {
      await fn(it.value());
    }
    const err = it.err();
    if (err) {
      throw err;
    }
  } finally {
    it.close();
  }
}
