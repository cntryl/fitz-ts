/**
 * Lease domain client.
 */

import { createDomainClient } from "../base";
import type {
  AsyncDispatchPort,
  DisconnectListenerPort,
  NotificationPort,
  ReconnectListenerPort,
  ReconnectRestoreRequestPort,
  RequestPort,
  RetryExecutionPort,
} from "../base";
import { LeaseError } from "../../core/errors";
import {
  MSG_LEASE_ACQUIRE,
  MSG_LEASE_NOTIFY,
  MSG_LEASE_QUERY,
  MSG_LEASE_SUBSCRIBE,
  MSG_LEASE_UNSUBSCRIBE,
} from "../../frame/types";
import { isRouteShape } from "../_routes";
import { restoreMapEntriesAtomically } from "../internal/restore";
import { LeaseCodec } from "./codec";
import {
  ChangeHandler,
  ChangeNotification,
  Lease,
  LeaseInfo,
  LeaseSubscription,
  LeaseLifecycleError,
  WithLeaseOptions,
  createLease,
  createLeaseSubscription,
} from "./types";

type LeaseSubscriptionState = {
  subId: bigint;
  handlers: Map<number, ChangeHandler>;
};

type LeaseConnectionPort = RequestPort &
  ReconnectListenerPort &
  DisconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  RetryExecutionPort &
  Partial<ReconnectRestoreRequestPort>;

export type LeaseClient = ReturnType<typeof createLeaseClient>;

export function createLeaseClient(connection: LeaseConnectionPort) {
  const { requestFrame, requestReconnectFrame, runWithRetry } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, LeaseSubscriptionState>();
  let initialized = false;
  let nextHandlerId = 1;

  connection.onReconnect(async () => {
    if (subscriptionsByPattern.size === 0) {
      return;
    }

    await restoreMapEntriesAtomically(subscriptionsByPattern, async (pattern, state) => {
      const subId = await subscribeWire(pattern, requestReconnectFrame);
      return {
        subId,
        handlers: new Map(state.handlers),
      };
    });
  });

  const acquire = async (route: string, ttlSecs: number): Promise<Lease> => {
    assertExactLeaseRoute(route);
    assertLeaseTtl(ttlSecs);
    const payload = LeaseCodec.encodeAcquire(route, ttlSecs);
    const response = await requestFrame(MSG_LEASE_ACQUIRE, payload);
    const decoded = LeaseCodec.decodeAcquireResponse(response);

    if (decoded.token === undefined) {
      throw new LeaseError("ACQUIRE failed", "ACQUIRE_FAILED");
    }

    const expiresAt = decoded.expiresAt ?? BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
    return createLease(decoded.token, expiresAt, route, connection);
  };

  const withLease = async <T>(
    route: string,
    ttlSecs: number,
    callback: (signal: AbortSignal) => T | Promise<T>,
    options: WithLeaseOptions = {},
  ): Promise<T> => {
    assertExactLeaseRoute(route);
    assertLeaseTtl(ttlSecs);
    if (options.signal?.aborted) {
      throw options.signal.reason ?? new Error("Lease execution canceled");
    }

    let lease: Lease;
    let delayMs = 50;
    for (;;) {
      try {
        lease = await acquire(route, ttlSecs);
        break;
      } catch (error) {
        if (!options.waitForAvailability || !isContention(error)) {
          throw error;
        }
        await abortableDelay(Math.random() * delayMs, options.signal);
        delayMs = Math.min(delayMs * 2, 1000);
      }
    }

    const lifecycle = new AbortController();
    const stopRenewal = new AbortController();
    const onParentAbort = (): void => lifecycle.abort(options.signal?.reason);
    options.signal?.addEventListener("abort", onParentAbort, { once: true });
    let timer: ReturnType<typeof setTimeout> | undefined;
    let leaseLoss: unknown;
    let callbackFailure: unknown;
    let callbackValue!: T;
    let callbackDone = false;

    const renew = async (): Promise<void> => {
      while (!callbackDone && leaseLoss === undefined) {
        await abortableDelay((ttlSecs * 1000) / 3, stopRenewal.signal).catch(() => undefined);
        if (callbackDone || stopRenewal.signal.aborted) {
          return;
        }
        try {
          await lease.extend(ttlSecs);
        } catch (error) {
          leaseLoss = error;
          lifecycle.abort(
            new LeaseError("Lease ownership was lost", "LOST", undefined, {
              cause: error,
            }),
          );
        }
      }
    };
    const renewal = renew();
    try {
      try {
        callbackValue = await callback(lifecycle.signal);
      } catch (error) {
        callbackFailure = error;
      }
      callbackDone = true;
      stopRenewal.abort();
      lifecycle.abort();
      await renewal;

      let releaseFailure: unknown;
      if (leaseLoss === undefined) {
        const cleanup = new AbortController();
        timer = setTimeout(() => cleanup.abort(), 5000);
        try {
          await lease.release(cleanup.signal);
        } catch (error) {
          releaseFailure = error;
        } finally {
          clearTimeout(timer);
        }
      }

      const failures = [leaseLoss, callbackFailure, releaseFailure].filter(
        (failure) => failure !== undefined && !isManagedCancellation(failure, lifecycle.signal),
      );
      if (failures.length > 1) {
        throw new LeaseLifecycleError("Multiple lease lifecycle operations failed", failures);
      }
      if (failures.length === 1) {
        throw failures[0];
      }
      if (options.signal?.aborted) {
        throw options.signal.reason;
      }
      return callbackValue;
    } finally {
      callbackDone = true;
      stopRenewal.abort();
      lifecycle.abort();
      if (timer !== undefined) clearTimeout(timer);
      options.signal?.removeEventListener("abort", onParentAbort);
      await renewal.catch(() => undefined);
    }
  };

  const query = async (route: string): Promise<LeaseInfo> => {
    assertExactLeaseRoute(route);
    return runWithRetry(
      {
        domain: "lease",
        operation: "query",
        retryClass: "replayable_read",
      },
      async () => {
        const payload = LeaseCodec.encodeQuery(route);
        const response = await requestFrame(MSG_LEASE_QUERY, payload);
        const decoded = LeaseCodec.decodeQueryResponse(response);
        if (decoded.status !== 0) {
          throw new LeaseError("QUERY failed", "QUERY_FAILED", decoded.status);
        }
        return {
          isHeld: decoded.isHeld ?? false,
          owner: decoded.owner,
          token: decoded.token,
          ttlRemainingSecs: decoded.ttlRemainingSecs,
          pendingWaiters: decoded.pendingWaiters ?? 0,
          expiresAt: decoded.expiresAt,
        };
      },
    );
  };

  const subscribe = async (pattern: string, handler: ChangeHandler): Promise<LeaseSubscription> => {
    assertExactLeaseRoute(pattern);
    initNotifyHandler();
    const existing = subscriptionsByPattern.get(pattern);
    if (existing) {
      return addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await subscribeWire(pattern);
    return addLocalSubscription(pattern, subId, handler);
  };

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const payload = LeaseCodec.encodeSubscribe(pattern);
    const response = await request(MSG_LEASE_SUBSCRIBE, payload);
    const decoded = LeaseCodec.decodeSubscribeResponse(response);

    if (decoded.subId === undefined) {
      throw new LeaseError("SUBSCRIBE failed", "SUBSCRIBE_FAILED");
    }

    return decoded.subId;
  };

  const addLocalSubscription = (
    pattern: string,
    subId: bigint,
    handler: ChangeHandler,
  ): LeaseSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      subscriptionsByPattern.set(pattern, subscription);
    }

    subscription.handlers.set(handlerId, handler);
    return createLeaseSubscription(subId, pattern, async () => {
      await unsubscribe(pattern, handlerId);
    });
  };

  const unsubscribe = async (pattern: string, handlerId: number): Promise<void> => {
    const subscription = subscriptionsByPattern.get(pattern);
    if (!subscription) {
      return;
    }

    subscription.handlers.delete(handlerId);
    if (subscription.handlers.size > 0) {
      return;
    }

    subscriptionsByPattern.delete(pattern);
    const payload = LeaseCodec.encodeUnsubscribe(pattern);
    await requestFrame(MSG_LEASE_UNSUBSCRIBE, payload);
  };

  const initNotifyHandler = (): void => {
    if (initialized) {
      return;
    }

    initialized = true;
    connection.registerNotificationHandler(MSG_LEASE_NOTIFY, (payload) => {
      try {
        const { route } = LeaseCodec.decodeNotification(payload);
        const subscription = subscriptionsByPattern.get(route);
        if (!subscription) {
          return;
        }

        const notification: ChangeNotification = { route };
        for (const handler of subscription.handlers.values()) {
          connection.dispatchAsyncHandler(async () => {
            await handler(notification);
          });
        }
      } catch {
        // Best-effort notification dispatch.
      }
    });
  };

  return {
    acquire,
    withLease,
    query,
    subscribe,
  };
}

export const LeaseClient = createLeaseClient;

export * from "./types";

function assertExactLeaseRoute(route: string): void {
  if (!isRouteShape(route, "lease", 3)) {
    throw new LeaseError(
      `Invalid lease route: ${route} (expected lease://{realm}/{area}/{resource}, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}

function assertLeaseTtl(ttlSecs: number): void {
  if (!Number.isSafeInteger(ttlSecs) || ttlSecs <= 0 || ttlSecs * 1000 > 2_147_483_647) {
    throw new LeaseError("ttlSecs must be a positive, schedulable safe integer", "INVALID_TTL");
  }
}

function isContention(error: unknown): boolean {
  return (
    error instanceof LeaseError &&
    ["LEASE_HELD", "LEASE_QUEUED", "LEASE_ALREADY_QUEUED"].includes(error.code)
  );
}

function isManagedCancellation(error: unknown, signal: AbortSignal): boolean {
  return (
    signal.aborted &&
    (error === signal.reason || (error instanceof Error && error.name === "AbortError"))
  );
}

function abortableDelay(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(done, ms);
    function done(): void {
      signal?.removeEventListener("abort", canceled);
      resolve();
    }
    function canceled(): void {
      clearTimeout(timer);
      signal?.removeEventListener("abort", canceled);
      reject(signal?.reason);
    }
    signal?.addEventListener("abort", canceled, { once: true });
  });
}
