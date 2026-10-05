import { RpcError, ErrCodeRpcTimeout } from "../../core/errors";
import type { ResponseFrame, RpcCallIterator, RpcCancellationOutcome } from "./types";

export type RpcIterator = RpcCallIterator & {
  push(frame: ResponseFrame): void;
  end(): void;
  fail(reason: unknown): void;
};

export function createRpcIterator(
  deadlineAt: number,
  abandon: (reason: 1 | 2) => Promise<RpcCancellationOutcome>,
  finish: () => void,
  signal?: AbortSignal,
): RpcIterator {
  const buffer: ResponseFrame[] = [];
  let done = false;
  let failureReason: unknown;
  let resolveNext: ((frame: ResponseFrame | null) => void) | null = null;
  let rejectNext: ((reason?: unknown) => void) | null = null;
  let cancellationRequested = false;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let resolveCancellation!: (outcome: RpcCancellationOutcome) => void;
  const cancellation = new Promise<RpcCancellationOutcome>((resolve) => {
    resolveCancellation = resolve;
  });
  let cancellationSettled = false;

  const settleCancellation = (outcome: RpcCancellationOutcome): void => {
    if (cancellationSettled) {
      return;
    }
    cancellationSettled = true;
    resolveCancellation(outcome);
  };

  const clearPendingWait = (): void => {
    resolveNext = null;
    rejectNext = null;
  };

  const clearDeadline = (): void => {
    if (timeout !== undefined) {
      clearTimeout(timeout);
      timeout = undefined;
    }
  };

  const detachAbortListener = (): void => {
    signal?.removeEventListener("abort", handleAbort);
  };

  const cancel = (reason: 1 | 2, error: unknown): void => {
    if (done || cancellationRequested) {
      return;
    }
    cancellationRequested = true;
    done = true;
    buffer.length = 0;
    failureReason = error;
    clearDeadline();
    const reject = rejectNext;
    clearPendingWait();
    detachAbortListener();
    void abandon(reason).then(settleCancellation, () => settleCancellation("unconfirmed"));
    reject?.(error);
  };

  function handleAbort(): void {
    cancel(1, abortError());
  }

  const push = (frame: ResponseFrame): void => {
    if (done) {
      return;
    }

    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = null;
      rejectNext = null;
      resolve(frame);
    } else {
      buffer.push(frame);
    }
  };

  const end = (): void => {
    if (!cancellationRequested) {
      settleCancellation("not_requested");
    }
    clearDeadline();
    detachAbortListener();
    done = true;
    if (resolveNext) {
      const resolve = resolveNext;
      clearPendingWait();
      resolve(null);
    }
  };

  const fail = (reason: unknown): void => {
    if (done) {
      return;
    }
    done = true;
    failureReason = reason;
    clearDeadline();
    finish();
    settleCancellation("not_requested");
    detachAbortListener();
    if (rejectNext) {
      const reject = rejectNext;
      clearPendingWait();
      const rejectWithCurrentSignalState = () => {
        reject(signal?.aborted ? abortError() : reason);
      };
      if (signal) {
        setTimeout(rejectWithCurrentSignalState, 0);
      } else {
        void Promise.resolve().then(rejectWithCurrentSignalState);
      }
      return;
    }
  };

  const next = async (): Promise<IteratorResult<ResponseFrame>> => {
    if (!done && signal?.aborted) {
      cancel(1, abortError());
      throw failureReason;
    }

    if (failureReason instanceof Error && failureReason.name === "AbortError") {
      throw failureReason;
    }

    // Frames pushed before a later failure are still deliverable and must
    // drain first — only surface the failure once the buffer is empty, or a
    // successfully received frame gets discarded in favor of the error that
    // arrived after it.
    if (buffer.length > 0) {
      const value = buffer.shift();
      if (!value) {
        return { value: undefined, done: true };
      }
      return { value, done: false };
    }

    if (failureReason !== undefined) {
      throw failureReason;
    }

    if (done) {
      return { value: undefined, done: true };
    }

    const frame = await new Promise<ResponseFrame | null>((resolve, reject) => {
      resolveNext = (f) => {
        clearPendingWait();
        resolve(f);
      };
      rejectNext = reject;
    });

    if (frame === null) {
      return { value: undefined, done: true };
    }

    return { value: frame, done: false };
  };

  const returnMethod = async (): Promise<IteratorResult<ResponseFrame>> => {
    if (!done) {
      cancel(1, abortError());
    }
    return { value: undefined, done: true };
  };

  const abortError = (): Error => {
    const error = new Error("The operation was aborted");
    error.name = "AbortError";
    return error;
  };

  const iterator: RpcIterator = {
    push,
    end,
    fail,
    cancellation,
    next,
    return: returnMethod,
    [Symbol.asyncIterator]() {
      return this;
    },
  };

  if (signal) {
    signal.addEventListener("abort", handleAbort, { once: true });
    if (signal.aborted) {
      handleAbort();
    }
  }
  if (!done) {
    timeout = setTimeout(
      () => {
        // Match the broker timeout's RPC domain error code.
        const error = new RpcError("RPC call timeout", "TIMEOUT", ErrCodeRpcTimeout);
        cancel(2, error);
      },
      Math.max(0, deadlineAt - performance.now()),
    );
  }
  return iterator;
}
