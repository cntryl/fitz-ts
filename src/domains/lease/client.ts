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
import { createKeyedSingleFlight } from "../internal/keyed-single-flight";
import {
  createSubscriptionIterator,
  type SubscriptionIteratorOptions,
} from "../internal/subscription-iterator";
import { LeaseCodec } from "./codec";
import { createBufferReader } from "../../core/buffer";
import { parsePlainResponse } from "../../protocol/response";
import {
  ChangeHandler,
  ChangeNotification,
  Lease,
  LeaseInfo,
  LeaseSubscription,
  LeaseLifecycleError,
  LeaseAcquireOptions,
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

export interface LeaseClient {
  acquire(route: string, ttlSecs: number, options?: LeaseAcquireOptions): Promise<Lease>;
  withLease<T>(
    route: string,
    ttlSecs: number,
    callback: (signal: AbortSignal) => T | Promise<T>,
    options?: WithLeaseOptions,
  ): Promise<T>;
  query(route: string): Promise<LeaseInfo>;
  subscribe(route: string, handler: ChangeHandler): Promise<LeaseSubscription>;
  subscribeIterator(
    route: string,
    options?: SubscriptionIteratorOptions,
  ): AsyncIterable<ChangeNotification>;
}

export function createLeaseClient(connection: LeaseConnectionPort): LeaseClient {
  const registerSingleFlight = createKeyedSingleFlight<string, bigint>();
  const { requestFrame, requestReconnectFrame, runWithRetry } = createDomainClient(connection);
  const subscriptionsByRoute = new Map<string, LeaseSubscriptionState>();
  let initialized = false;
  let acquireHandlerInitialized = false;
  let acquisitionTail: Promise<void> = Promise.resolve();
  let nextHandlerId = 1;
  const queuedAcquisitions: Array<{
    resolve: (response: Uint8Array) => void;
    reject: (error: unknown) => void;
    settled: boolean;
  }> = [];

  connection.onDisconnect(() => {
    const error = new LeaseError("Lease acquisition interrupted by disconnect", "DISCONNECTED");
    for (const queued of queuedAcquisitions.splice(0)) {
      if (!queued.settled) queued.reject(error);
    }
  });

  connection.onReconnect(async () => {
    if (subscriptionsByRoute.size === 0) {
      return;
    }

    await restoreMapEntriesAtomically(
      subscriptionsByRoute,
      async (route, state) => {
        const subId = await subscribeWire(route, requestReconnectFrame);
        return { subId, handlers: new Map(state.handlers) };
      },
      async (route) => {
        parsePlainResponse(
          await requestReconnectFrame(MSG_LEASE_UNSUBSCRIBE, LeaseCodec.encodeUnsubscribe(route)),
        );
      },
    );
  });

  const initAcquireHandler = (): void => {
    if (acquireHandlerInitialized) return;
    acquireHandlerInitialized = true;
    connection.registerNotificationHandler(MSG_LEASE_ACQUIRE, (payload) => {
      let queued = queuedAcquisitions.shift();
      while (queued?.settled) queued = queuedAcquisitions.shift();
      queued?.resolve(payload);
    });
  };

  const runAcquire = async (
    route: string,
    ttlSecs: number,
    options: LeaseAcquireOptions = {},
  ): Promise<Lease> => {
    assertExactLeaseRoute(route);
    assertLeaseTtl(ttlSecs);
    assertWaitSeconds(options.waitSeconds ?? 0);
    if (options.signal?.aborted)
      throw options.signal.reason ?? new Error("Lease acquisition canceled");
    initAcquireHandler();
    const payload = LeaseCodec.encodeAcquire(route, ttlSecs, options.waitSeconds ?? 0);
    let queuedResolve!: (response: Uint8Array) => void;
    let queuedReject!: (error: unknown) => void;
    const queuedResponse = new Promise<Uint8Array>((resolve, reject) => {
      queuedResolve = resolve;
      queuedReject = reject;
    });
    const queued = { resolve: queuedResolve, reject: queuedReject, settled: false };
    queuedAcquisitions.push(queued);
    const removeQueued = (): void => {
      const index = queuedAcquisitions.indexOf(queued);
      if (index >= 0) queuedAcquisitions.splice(index, 1);
    };
    const onAbort = (): void => {
      queued.settled = true;
      removeQueued();
      queuedReject(options.signal?.reason ?? new Error("Lease acquisition canceled"));
    };
    options.signal?.addEventListener("abort", onAbort, { once: true });

    let decoded;
    try {
      const response = await requestFrame(MSG_LEASE_ACQUIRE, payload, options.signal);
      decoded = LeaseCodec.decodeAcquireResponse(response);
    } catch (error) {
      queued.settled = true;
      removeQueued();
      options.signal?.removeEventListener("abort", onAbort);
      queuedResponse.catch(() => undefined);
      throw error;
    }

    if (decoded.responseType === 2 || decoded.responseType === 3) {
      decoded = LeaseCodec.decodeAcquireResponse(await queuedResponse);
      options.signal?.removeEventListener("abort", onAbort);
      if (decoded.responseType !== 0 && decoded.responseType !== 1) {
        throw new LeaseError("ACQUIRE returned a second queued response", "INVALID_RESPONSE");
      }
    } else {
      queued.settled = true;
      removeQueued();
      options.signal?.removeEventListener("abort", onAbort);
    }

    if (decoded.token === undefined) {
      throw new LeaseError("ACQUIRE failed", "ACQUIRE_FAILED");
    }

    const expiresAt = decoded.expiresAt ?? BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
    return createLease(decoded.token, expiresAt, route, connection);
  };

  const acquire = (
    route: string,
    ttlSecs: number,
    options: LeaseAcquireOptions = {},
  ): Promise<Lease> => {
    const result = acquisitionTail.then(() => runAcquire(route, ttlSecs, options));
    acquisitionTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
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

    const lease = await acquire(route, ttlSecs, {
      waitSeconds: options.waitForAvailability ? (options.waitSeconds ?? 30) : 0,
      signal: options.signal,
    });

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
          throw new LeaseError(
            `QUERY failed: ${decoded.errorMessage ?? `status ${decoded.status}`}`,
            "QUERY_FAILED",
            decoded.status,
          );
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

  const subscribe = async (route: string, handler: ChangeHandler): Promise<LeaseSubscription> => {
    assertExactLeaseRoute(route);
    initNotifyHandler();
    const existing = subscriptionsByRoute.get(route);
    if (existing) {
      return addLocalSubscription(route, existing.subId, handler);
    }

    const subId = await registerSingleFlight(route, () => subscribeWire(route));
    return addLocalSubscription(route, subId, handler);
  };

  const subscribeIterator = (
    route: string,
    iteratorOptions?: SubscriptionIteratorOptions,
  ): AsyncIterable<ChangeNotification> =>
    createSubscriptionIterator(
      (handler) => subscribe(route, async (notification) => handler(notification)),
      iteratorOptions,
    );

  const subscribeWire = async (route: string, request = requestFrame): Promise<bigint> => {
    const payload = LeaseCodec.encodeSubscribe(route);
    const parsed = parsePlainResponse(await request(MSG_LEASE_SUBSCRIBE, payload));
    if (!parsed.success) {
      throw new LeaseError(
        `SUBSCRIBE failed: ${parsed.error ?? "unknown error"}`,
        "SUBSCRIBE_FAILED",
        parsed.errorCode,
      );
    }
    const reader = createBufferReader(parsed.data);
    if (reader.remainingBytes() !== 8) {
      throw new LeaseError("SUBSCRIBE failed", "SUBSCRIBE_FAILED");
    }
    return reader.readU64BE();
  };

  const addLocalSubscription = (
    route: string,
    subId: bigint,
    handler: ChangeHandler,
  ): LeaseSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByRoute.get(route);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      subscriptionsByRoute.set(route, subscription);
    }

    subscription.handlers.set(handlerId, handler);
    return createLeaseSubscription(
      () => subscriptionsByRoute.get(route)?.subId ?? subId,
      route,
      async () => {
        await unsubscribe(route, handlerId);
      },
    );
  };

  const unsubscribe = async (route: string, handlerId: number): Promise<void> => {
    const subscription = subscriptionsByRoute.get(route);
    if (!subscription) {
      return;
    }

    subscription.handlers.delete(handlerId);
    if (subscription.handlers.size > 0) {
      return;
    }

    const payload = LeaseCodec.encodeUnsubscribe(route);
    const parsed = parsePlainResponse(await requestFrame(MSG_LEASE_UNSUBSCRIBE, payload));
    if (!parsed.success) {
      throw new LeaseError(
        `UNSUBSCRIBE failed: ${parsed.error ?? "unknown error"}`,
        "UNSUBSCRIBE_FAILED",
        parsed.errorCode,
      );
    }
    subscriptionsByRoute.delete(route);
  };

  const initNotifyHandler = (): void => {
    if (initialized) {
      return;
    }

    initialized = true;
    connection.registerNotificationHandler(MSG_LEASE_NOTIFY, (payload) => {
      try {
        const { route } = LeaseCodec.decodeNotification(payload);
        const subscription = subscriptionsByRoute.get(route);
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
    subscribeIterator,
  };
}

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

function assertWaitSeconds(waitSeconds: number): void {
  if (!Number.isSafeInteger(waitSeconds) || waitSeconds < 0 || waitSeconds > 0xffff_ffff) {
    throw new LeaseError("waitSeconds must be an unsigned 32-bit integer", "INVALID_WAIT");
  }
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
