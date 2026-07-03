import type { Transport, TransportHeartbeatOptions } from "../../transport/types";
import { TransportError } from "../../core/errors";

export interface HeartbeatLoopOptions {
  enabled: boolean;
  intervalMs: number;
  timeoutMs: number;
  isStopped: () => boolean;
  sendHeartbeat: (transport: Transport, heartbeat: TransportHeartbeatOptions) => Promise<void>;
  onFailure: (error: TransportError, transport: Transport) => void;
  describeError: (error: unknown) => string;
}

export function createHeartbeatLoop(options: HeartbeatLoopOptions) {
  let heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  let heartbeatTransport: Transport | null = null;
  let heartbeatPending = false;
  let lastActivityAt = Date.now();

  const markOutboundActivity = (): void => {
    lastActivityAt = Date.now();
  };

  const markRemoteActivity = (): void => {
    lastActivityAt = Date.now();
  };

  const stop = (): void => {
    if (heartbeatTimer) {
      clearTimeout(heartbeatTimer);
      heartbeatTimer = null;
    }
    heartbeatTransport = null;
    heartbeatPending = false;
  };

  const start = (activeTransport: Transport): void => {
    if (!options.enabled) {
      return;
    }

    stop();
    activeTransport.enableKeepAlive?.(options.intervalMs);
    heartbeatTransport = activeTransport;
    markRemoteActivity();

    const scheduleNext = (): void => {
      if (options.isStopped() || heartbeatTransport !== activeTransport) {
        return;
      }

      heartbeatTimer = setTimeout(tick, options.intervalMs);
    };

    const tick = (): void => {
      heartbeatTimer = null;
      if (options.isStopped() || heartbeatTransport !== activeTransport) {
        return;
      }

      const now = Date.now();
      if (now - lastActivityAt < options.intervalMs) {
        scheduleNext();
        return;
      }

      const supportsHeartbeat = activeTransport.supportsHeartbeat?.() ?? true;
      if (!heartbeatPending && supportsHeartbeat && activeTransport.sendHeartbeat) {
        heartbeatPending = true;
        const heartbeatSentAt = now;

        void options
          .sendHeartbeat(activeTransport, { timeoutMs: options.timeoutMs })
          .then(() => {
            if (heartbeatTransport !== activeTransport) {
              return;
            }

            heartbeatPending = false;
            markRemoteActivity();
          })
          .catch((error: unknown) => {
            if (heartbeatTransport !== activeTransport) {
              return;
            }

            heartbeatPending = false;
            if (lastActivityAt > heartbeatSentAt) {
              return;
            }

            const heartbeatError = new TransportError(
              `Heartbeat failed: ${options.describeError(error)}`,
            );
            void activeTransport.close().catch(() => undefined);
            options.onFailure(heartbeatError, activeTransport);
          });
      }

      scheduleNext();
    };

    scheduleNext();
  };

  return {
    start,
    stop,
    markOutboundActivity,
    markRemoteActivity,
  };
}
