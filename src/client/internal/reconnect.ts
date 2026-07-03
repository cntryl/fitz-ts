import { ConnectionState } from "../../core/types";
import { isAbortError, sleepWithAbort } from "./async";

export type ReconnectLoopResult = "connected" | "closed" | "exhausted";

export interface ReconnectSchedulerOptions {
  maxAttempts: number;
  backoffMs: number;
  maxBackoffMs: number;
  closeSignal: AbortSignal;
  isCloseRequested: () => boolean;
  setState: (state: ConnectionState) => void;
  openAndAuthenticate: (isReconnect: boolean) => Promise<void>;
  emitLifecycleEvent: (event: string, error?: unknown, attempt?: number) => void;
  logRetry: (attempts: number, delayMs: number, baseDelayMs: number, error: unknown) => void;
}

export function createReconnectScheduler(options: ReconnectSchedulerOptions) {
  const getReconnectDelayMs = (baseDelayMs: number): number => {
    const jitter = Math.floor(Math.random() * baseDelayMs * 0.5);
    return Math.min(Math.max(baseDelayMs + jitter, 1), options.maxBackoffMs);
  };

  const runLoop = async (): Promise<ReconnectLoopResult> => {
    let attempts = 0;
    let delayMs = options.backoffMs;

    while (!options.isCloseRequested() && attempts < options.maxAttempts) {
      attempts += 1;
      options.setState(ConnectionState.Reconnecting);
      options.emitLifecycleEvent("reconnect_scheduled", undefined, attempts);

      const actualDelayMs = getReconnectDelayMs(delayMs);
      try {
        await sleepWithAbort(actualDelayMs, options.closeSignal);
        if (options.isCloseRequested()) {
          return "closed";
        }
        await options.openAndAuthenticate(true);
        return "connected";
      } catch (error) {
        if (options.isCloseRequested()) {
          return "closed";
        }
        if (isAbortError(error)) {
          return "closed";
        }
        options.logRetry(attempts, actualDelayMs, delayMs, error);
        delayMs = Math.min(delayMs * 2, options.maxBackoffMs);
      }
    }

    if (options.isCloseRequested()) {
      options.setState(ConnectionState.Closed);
      return "closed";
    }

    options.setState(ConnectionState.Disconnected);
    options.emitLifecycleEvent("reconnect_exhausted", undefined, attempts);
    return "exhausted";
  };

  const restoreState = async (
    listeners: Iterable<() => void | Promise<void>>,
    onError: (error: unknown) => void,
  ) => {
    for (const listener of listeners) {
      try {
        await listener();
      } catch (error) {
        onError(error);
      }
    }
  };

  return {
    runLoop,
    restoreState,
  };
}
