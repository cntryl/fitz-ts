import type { FitzObservability } from "../../core/types";
import { ConnectionState } from "../../core/types";
import { FitzError } from "../../core/errors";
import type { Transport } from "../../transport/types";

export function createConnectionTelemetry(
  observability: FitzObservability | undefined,
  getState: () => ConnectionState,
  getTransport: () => Transport | null,
) {
  const log = (
    level: "debug" | "info" | "warn" | "error",
    event: string,
    fields?: Record<string, unknown>,
  ): void => {
    observability?.logger?.log(level, event, fields);
  };

  const describeError = (error: unknown): string => {
    if (error instanceof Error) {
      return error.message;
    }
    return String(error);
  };

  const describeErrorFields = (error: unknown): Record<string, unknown> => {
    if (error instanceof FitzError) {
      return {
        errorName: error.name,
        code: error.code,
        domainCode: error.domainCode,
      };
    }

    if (error instanceof Error) {
      return {
        errorName: error.name,
        code: (error as Error & { code?: unknown }).code,
        domainCode: (error as Error & { domainCode?: unknown }).domainCode,
      };
    }

    return {
      errorName: typeof error,
      code: undefined,
      domainCode: undefined,
    };
  };

  const describeConnectionLoss = (error: unknown): string => {
    if (error instanceof Error) {
      return error.message;
    }
    return "connection closed during CONNECT";
  };

  const emitLifecycleEvent = (event: string, error?: unknown, attempt?: number): void => {
    const state = getState();
    const transport = getTransport();
    const payload = {
      event,
      state,
      transport: transport?.constructor.name,
      url: transport?.getUrl(),
      attempt,
      error: error ? describeError(error) : undefined,
    };

    observability?.onLifecycleEvent?.(payload);
    log("info", `fitz.connection.${event}`, payload);
    observability?.meter?.counter("fitz.connection.lifecycle", 1, {
      event,
      state,
    });
  };

  return {
    log,
    describeError,
    describeErrorFields,
    describeConnectionLoss,
    emitLifecycleEvent,
  };
}
