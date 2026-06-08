import {
  ConnectionError,
  FitzError,
  TimeoutError,
  TransportError,
  isRetryable,
} from "../core/errors";

export type RetryClass = "replayable_read" | "confirmed_negative_retry" | "wait_only";
export type AttemptBoundary = "pre-send" | "post-send";
export type FailureKind = "transport" | "connection" | "timeout" | "domain" | "other";

export interface RetryOperation {
  domain: string;
  operation: string;
  retryClass: RetryClass;
  signal?: AbortSignal;
  waitTimeoutMs?: number;
}

export interface ResilienceErrorMeta {
  boundary: AttemptBoundary;
  failureKind: FailureKind;
  explicitNegative: boolean;
}

const resilienceMetaSymbol = Symbol("fitz.resilience.meta");

export function attachResilienceMeta<T>(error: T, meta: ResilienceErrorMeta): T {
  if (error && (typeof error === "object" || typeof error === "function")) {
    Object.defineProperty(error, resilienceMetaSymbol, {
      value: meta,
      configurable: true,
      enumerable: false,
      writable: true,
    });
  }

  return error;
}

export function getResilienceMeta(error: unknown): ResilienceErrorMeta | undefined {
  if (!error || (typeof error !== "object" && typeof error !== "function")) {
    return undefined;
  }

  return (error as Record<PropertyKey, ResilienceErrorMeta | undefined>)[resilienceMetaSymbol];
}

export function classifyFailureKind(error: unknown): FailureKind {
  if (error instanceof TimeoutError) {
    return "timeout";
  }

  if (error instanceof TransportError) {
    return "transport";
  }

  if (error instanceof ConnectionError) {
    return "connection";
  }

  if (error instanceof FitzError) {
    return "domain";
  }

  return "other";
}

export function isTransientRetryError(error: unknown): boolean {
  return (
    error instanceof TimeoutError ||
    error instanceof TransportError ||
    error instanceof ConnectionError ||
    isRetryable(error)
  );
}

export function shouldRetryOperation(retryClass: RetryClass, error: unknown): boolean {
  switch (retryClass) {
    case "wait_only":
      return false;
    case "replayable_read":
      return isTransientRetryError(error);
    case "confirmed_negative_retry": {
      const meta = getResilienceMeta(error);
      return meta?.explicitNegative === true && meta.boundary === "post-send" && isRetryable(error);
    }
    default:
      return false;
  }
}
