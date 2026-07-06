import { bench } from "vitest";

type BenchOptions = NonNullable<Parameters<typeof bench>[2]>;
type BenchFn = () => void | Promise<void>;
type BatchedBenchFn = (index: number) => unknown;

export const SYNC_CODEC_BATCH_SIZE = 5_000;
export const COMPOSITE_SYNC_BATCH_SIZE = 2_000;
export const ASYNC_ROUND_TRIP_BATCH_SIZE = 100;
export const FIFO_DRAIN_BATCH_SIZE = 1;

const balancedOptions = {
  time: 1_500,
  warmupTime: 500,
} satisfies BenchOptions;

const microOptions = {
  ...balancedOptions,
  iterations: 1_000,
  warmupIterations: 100,
} satisfies BenchOptions;

const macroOptions = {
  ...balancedOptions,
  iterations: 10,
  warmupIterations: 3,
} satisfies BenchOptions;

const asyncOptions = {
  ...balancedOptions,
  iterations: 10,
  warmupIterations: 3,
} satisfies BenchOptions;

let _consumeSink: unknown;

export function consume(value: unknown): void {
  _consumeSink = value;
}

export function benchMicro(name: string, fn: BenchFn, options: BenchOptions = {}): void {
  bench(name, fn, { ...microOptions, ...options });
}

export function benchMacro(name: string, fn: BenchFn, options: BenchOptions = {}): void {
  bench(name, fn, { ...macroOptions, ...options });
}

export function benchAsync(name: string, fn: BenchFn, options: BenchOptions = {}): void {
  bench(name, fn, { ...asyncOptions, ...options });
}

export function benchBatch(
  name: string,
  batchSize: number,
  fn: BatchedBenchFn,
  options: BenchOptions = {},
): void {
  if (!Number.isInteger(batchSize) || batchSize < 1) {
    throw new Error(`Invalid benchmark batch size: ${batchSize}`);
  }

  benchMicro(
    name,
    () => {
      let result: unknown;
      for (let index = 0; index < batchSize; index += 1) {
        result = fn(index);
      }
      consume(result);
    },
    options,
  );
}
