import { describe, expect, it } from "vite-plus/test";

import {
  createAsyncIterableIterator,
  createSliceIterator,
  forEach,
} from "../../../src/core/iterator";
import type { Iterator as FitzIterator } from "../../../src/core/iterator";

function createSpyIterator<T>(items: T[]): { iterator: FitzIterator<T>; closeCalls: number[] } {
  let index = -1;
  const closeCalls: number[] = [];
  const iterator: FitzIterator<T> = {
    next: () => {
      index += 1;
      return index < items.length;
    },
    value: () => items[index]!,
    err: () => null,
    close: () => {
      closeCalls.push(index);
    },
  };
  return { iterator, closeCalls };
}

describe("createAsyncIterableIterator", () => {
  it("calls close() when the underlying iterator is exhausted normally", async () => {
    const { iterator, closeCalls } = createSpyIterator([1, 2, 3]);
    const values: number[] = [];

    for await (const value of createAsyncIterableIterator(iterator)) {
      values.push(value);
    }

    expect(values).toEqual([1, 2, 3]);
    expect(closeCalls).toHaveLength(1);
  });

  it("calls close() when a consuming for-await loop breaks early", async () => {
    const { iterator, closeCalls } = createSpyIterator([1, 2, 3]);
    const values: number[] = [];

    for await (const value of createAsyncIterableIterator(iterator)) {
      values.push(value);
      if (value === 1) break;
    }

    expect(values).toEqual([1]);
    expect(closeCalls).toHaveLength(1);
  });

  it("calls close() when the consumer throws mid-iteration", async () => {
    const { iterator, closeCalls } = createSpyIterator([1, 2, 3]);

    await expect(
      (async () => {
        for await (const value of createAsyncIterableIterator(iterator)) {
          if (value === 1) throw new Error("consumer failure");
        }
      })(),
    ).rejects.toThrow("consumer failure");

    expect(closeCalls).toHaveLength(1);
  });

  it("still surfaces a trailing iterator error after close()", async () => {
    let index = -1;
    const items = [1, 2];
    const iterator: FitzIterator<number> = {
      next: () => {
        index += 1;
        return index < items.length;
      },
      value: () => items[index]!,
      err: () => new Error("iterator failure"),
      close: () => undefined,
    };

    await expect(
      (async () => {
        const values: number[] = [];
        for await (const value of createAsyncIterableIterator(iterator)) {
          values.push(value);
        }
        return values;
      })(),
    ).rejects.toThrow("iterator failure");
  });
});

describe("forEach", () => {
  it("calls close() after normal completion, matching createAsyncIterableIterator's behavior", async () => {
    const { iterator, closeCalls } = createSpyIterator([1, 2]);
    const seen: number[] = [];

    await forEach(iterator, async (value) => {
      seen.push(value);
    });

    expect(seen).toEqual([1, 2]);
    expect(closeCalls).toHaveLength(1);
  });

  it("calls close() even when the callback throws", async () => {
    const { iterator, closeCalls } = createSpyIterator([1, 2]);

    await expect(
      forEach(iterator, async () => {
        throw new Error("callback failure");
      }),
    ).rejects.toThrow("callback failure");

    expect(closeCalls).toHaveLength(1);
  });
});

describe("createSliceIterator", () => {
  it("iterates items in order and reports EOF via next()", () => {
    const iterator = createSliceIterator(["a", "b"]);
    expect(iterator.next()).toBe(true);
    expect(iterator.value()).toBe("a");
    expect(iterator.next()).toBe(true);
    expect(iterator.value()).toBe("b");
    expect(iterator.next()).toBe(false);
    expect(iterator.err()).toBeNull();
  });
});
