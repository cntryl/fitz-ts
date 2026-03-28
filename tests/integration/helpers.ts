export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  options: {
    timeoutMs?: number;
    intervalMs?: number;
    timeoutMessage?: string;
  } = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 5000;
  const intervalMs = options.intervalMs ?? 50;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await sleep(intervalMs);
  }

  throw new Error(options.timeoutMessage ?? `Timed out after ${timeoutMs}ms waiting for condition`);
}

export async function collectAsyncIterable<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) {
    values.push(value);
  }
  return values;
}
