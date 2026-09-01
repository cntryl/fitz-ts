/**
 * Lease domain client.
 */

import { createDomainClient } from "../base";
import type {
  AsyncDispatchPort,
  BackgroundErrorPort,
  DisconnectListenerPort,
  NotificationPort,
  ReconnectListenerPort,
  ReconnectRestoreRequestPort,
  RequestPort,
  RetryExecutionPort,
} from "../base";
import { LeaseError } from "../../core/errors";
import { sleepWithAbort } from "../../client/internal/async";
import {
  MSG_LEASE_ACQUIRE,
  MSG_LEASE_LIST,
  MSG_LEASE_NOTIFY,
  MSG_LEASE_QUERY,
  MSG_LEASE_SUBSCRIBE,
  MSG_LEASE_UNSUBSCRIBE,
} from "../../frame/types";
import { isRegistrationPatternShape, isRouteShape } from "../_routes";
import { restoreMapEntriesAtomically } from "../internal/restore";
import { createKeyedSingleFlight } from "../internal/keyed-single-flight";
import {
  createSubscriptionIterator,
  type SubscriptionIteratorOptions,
} from "../internal/subscription-iterator";
import {
  awaitPendingUnsubscribe,
  createSubscriptionController,
  createGenerationCounter,
  isCurrentEmptyState,
} from "../internal/subscription-handle";
import {
  dispatchSubscriptionHandler,
  type SubscriptionHandlerRegistration,
} from "../internal/subscription-dispatch";
import { LeaseCodec } from "./codec";
import { createBufferReader } from "../../core/buffer";
import { parseStandardResponse } from "../../protocol/response";
import {
  ChangeHandler,
  ChangeNotification,
  Lease,
  LeaseInfo,
  LeaseSubscription,
  LeaseLifecycleError,
  LeaseAcquireOptions,
  LeaseAuthority,
  LeaseListCursor,
  LeaseListItem,
  LeaseListPage,
  LeaseInventoryOptions,
  LeaseInventoryObserver,
  WithLeaseOptions,
  createLease,
} from "./types";

type LeaseSubscriptionState = {
  subId: bigint;
  handlers: Map<number, SubscriptionHandlerRegistration<ChangeNotification>>;
  generation: number;
  // Set while a wire UNSUBSCRIBE for this route is awaiting its broker
  // round-trip. subscribe()'s "reuse the existing state" path must wait it
  // out rather than reuse it blindly — see awaitPendingUnsubscribe().
  pendingUnsubscribe?: Promise<void>;
};

type LeaseConnectionPort = RequestPort &
  Partial<BackgroundErrorPort> &
  ReconnectListenerPort &
  DisconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  RetryExecutionPort &
  Partial<ReconnectRestoreRequestPort>;

/**
 * Distributed lease facade for manual handles and managed fenced critical
 * sections. `acquire`, `withLease`, `query`, `extend`, and `release` all
 * require a concrete `lease://realm/area/resource` route. `subscribe`,
 * `unsubscribe`, `listPage`, and `list` additionally accept whole-segment `*`
 * wildcards in any position and a trailing `**` alias (e.g.
 * `lease://acme/renderers/*`, `lease://acme/**`).
 */
export interface LeaseClient {
  /**
   * Acquires a lease.
   *
   * Note: `acquire()` calls are serialized per client instance across every
   * route, not just per-route — a deferred ACQUIRE completion notification
   * carries no correlation id, only FIFO arrival order, so a second
   * `acquire()` for a completely unrelated route cannot even send its
   * request until this call's full lifecycle has resolved.
   *
   * `ttlSeconds` is a positive integer lifetime whose milliseconds must fit a
   * signed 32-bit timer; `waitSeconds` defaults to 0 and must fit an unsigned
   * 32-bit integer. Always release the returned handle. Do not use manual
   * acquisition when automatic renewal and loss signalling are required;
   * prefer {@link LeaseClient.withLease}.
   */
  acquire(route: string, options: LeaseAcquireOptions): Promise<Lease>;
  /**
   * Acquires a lease, runs `callback` while holding it, and releases it
   * afterward. Subject to the same cross-route serialization as
   * {@link LeaseClient.acquire}.
   *
   * The callback signal aborts on caller cancellation, lease loss, renewal
   * failure, or shutdown. Protected writes must carry `authority.fencingToken`.
   * Callback success does not hide renewal/release failures.
   */
  withLease<T>(
    route: string,
    callback: (signal: AbortSignal, authority: LeaseAuthority) => T | Promise<T>,
    options: WithLeaseOptions,
  ): Promise<T>;
  /** Returns current broker lease state without acquiring ownership. */
  query(
    route: string,
    options?: {
      /** Cancels this read-only query. */
      signal?: AbortSignal;
    },
  ): Promise<LeaseInfo>;
  /** Subscribes to release/expiry changes for one lease route. */
  subscribe(
    route: string,
    handler: ChangeHandler,
    options?: {
      /** Automatically unsubscribes this handler when aborted. */
      signal?: AbortSignal;
    },
  ): Promise<LeaseSubscription>;
  /** Returns an async stream of release/expiry changes; breaking iteration unsubscribes. */
  notifications(
    route: string,
    options?: SubscriptionIteratorOptions,
  ): AsyncIterable<ChangeNotification>;
  /**
   * Returns one page of leases matching `pattern`. The server snapshots the
   * full matching set on the cursor-less first call, so paging through one
   * scan (same `snapshotId`) never produces duplicates or omissions even
   * under concurrent acquire/release/expiry elsewhere. Reusing a cursor with
   * a different pattern, or after that snapshot is evicted or the broker
   * restarts, fails with `ERR_INVALID_LIST_CURSOR`.
   */
  listPage(
    pattern: string,
    options?: {
      /** Continuation cursor from a previous page; omit to start a new scan. */
      cursor?: LeaseListCursor;
      /** Requested page size; 0 or omitted uses the server default, clamped to its max. */
      limit?: number;
      /** Cancels this read-only page fetch. */
      signal?: AbortSignal;
    },
  ): Promise<LeaseListPage>;
  /**
   * Iterates pages of leases matching `pattern`. Each yielded value is a
   * page; break iteration to stop fetching additional pages.
   */
  list(
    pattern: string,
    options?: {
      /** Requested leases per broker page. */
      pageSize?: number;
      /** Cancels listing and closes the iterator. */
      signal?: AbortSignal;
    },
  ): AsyncIterableIterator<readonly LeaseListItem[]>;
  /**
   * Starts a race-safe, high-level observer of every lease matching
   * `pattern`: subscribes and waits for acknowledgement, buffers
   * notifications while it lists the current matching set to completion,
   * installs that as the initial view, then drains anything buffered
   * during bootstrap. Afterward, notifications coalesce into full LIST
   * reconciliation passes so every item retains the complete inventory
   * shape. Periodic reconciliation backstops missed notifications, and a
   * broker reconnect invalidates the view and re-runs bootstrap.
   *
   * The returned promise resolves once the initial bootstrap installs its
   * first view (`ready` is already `true`). Always `close()` (or
   * `await using`) the result.
   */
  observeInventory(
    pattern: string,
    options?: LeaseInventoryOptions,
  ): Promise<LeaseInventoryObserver>;
}

export function createLeaseClient(connection: LeaseConnectionPort): LeaseClient {
  const registerSingleFlight = createKeyedSingleFlight<string, bigint>();
  const { requestFrame, requestReconnectFrame, runWithRetry } = createDomainClient(connection);
  const subscriptionsByRoute = new Map<string, LeaseSubscriptionState>();
  // Maps a live wire subId to the pattern/route it was subscribed under.
  // LEASE_NOTIFY carries the concrete route that changed, not the
  // subscribed pattern, so dispatch must resolve subId -> pattern first
  // (a plain subscriptionsByRoute.get(decodedRoute) lookup only ever
  // matches an exact-route subscription, never a wildcard one).
  const patternsBySubId = new Map<bigint, string>();
  const subIdGeneration = createGenerationCounter();
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
      if (!queued.settled) {
        queued.settled = true;
        queued.reject(error);
      }
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
        // Update the reverse index for THIS route the instant its own
        // resubscribe lands, rather than waiting for every sibling route's
        // round-trip (and the whole loop) to finish. The broker can ack and
        // emit LEASE_NOTIFY for this route immediately, before a slower
        // sibling route finishes resubscribing — deferring this update to
        // the end of the loop left a window where the notify handler's
        // patternsBySubId.get(newSubId) missed and silently dropped it.
        patternsBySubId.delete(state.subId);
        patternsBySubId.set(subId, route);
        // Carry the generation forward: this is the same logical
        // subscription surviving reconnect, not a new one.
        return { subId, handlers: new Map(state.handlers), generation: state.generation };
      },
      async (route, restoredState) => {
        // This route's resubscribe succeeded but a sibling route's failed,
        // so restoreMapEntriesAtomically is unwinding it — undo the reverse
        // index entry set above along with the wire UNSUBSCRIBE below.
        patternsBySubId.delete(restoredState.subId);
        parseStandardResponse(
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
    options: LeaseAcquireOptions,
  ): Promise<{ lease: Lease; authority: LeaseAuthority }> => {
    assertExactLeaseRoute(route);
    assertLeaseTtl(ttlSecs);
    assertWaitSeconds(options.waitSeconds ?? 0);
    if (options.signal?.aborted)
      throw options.signal.reason ?? new Error("Lease acquisition canceled");
    const waitSeconds = options.waitSeconds ?? 0;
    const payload = LeaseCodec.encodeAcquire(route, ttlSecs, waitSeconds);
    let disconnected = false;
    const unregisterAcquireDisconnect = connection.onDisconnect(() => {
      disconnected = true;
    });
    let queuedResponse: Promise<Uint8Array> | undefined;
    let queued:
      | {
          resolve: (response: Uint8Array) => void;
          reject: (error: unknown) => void;
          settled: boolean;
        }
      | undefined;

    if (waitSeconds > 0) {
      initAcquireHandler();
      let queuedResolve!: (response: Uint8Array) => void;
      let queuedReject!: (error: unknown) => void;
      queuedResponse = new Promise<Uint8Array>((resolve, reject) => {
        queuedResolve = resolve;
        queuedReject = reject;
      });
      // Observe the internal promise immediately. Callers still receive its
      // rejection when runAcquire awaits it, but a disconnect or abort that
      // wins while the primary ACQUIRE request is pending can never become an
      // unhandled rejection in the meantime.
      void queuedResponse.catch(() => undefined);
      queued = { resolve: queuedResolve, reject: queuedReject, settled: false };
      queuedAcquisitions.push(queued);
    }

    const removeQueued = (): void => {
      if (!queued) return;
      const index = queuedAcquisitions.indexOf(queued);
      if (index >= 0) queuedAcquisitions.splice(index, 1);
    };
    const onAbort = (): void => {
      if (!queued || queued.settled) return;
      queued.settled = true;
      removeQueued();
      queued.reject(options.signal?.reason ?? new Error("Lease acquisition canceled"));
    };
    if (queued) options.signal?.addEventListener("abort", onAbort, { once: true });

    try {
      let decoded;
      try {
        const response = await requestFrame(MSG_LEASE_ACQUIRE, payload, options.signal);
        decoded = LeaseCodec.decodeAcquireResponse(response);
      } catch (error) {
        if (options.signal?.aborted) {
          throw options.signal.reason ?? new Error("Lease acquisition canceled");
        }
        if (disconnected) {
          throw new LeaseError("Lease acquisition interrupted by disconnect", "DISCONNECTED");
        }
        throw error;
      }

      if (decoded.responseType === 2 || decoded.responseType === 3) {
        if (!queuedResponse) {
          throw new LeaseError("ACQUIRE queued without a wait request", "INVALID_RESPONSE");
        }
        decoded = LeaseCodec.decodeAcquireResponse(await queuedResponse);
        if (decoded.responseType !== 0 && decoded.responseType !== 1) {
          throw new LeaseError("ACQUIRE returned a second queued response", "INVALID_RESPONSE");
        }
      }

      if (decoded.token === undefined) {
        throw new LeaseError("ACQUIRE failed", "ACQUIRE_FAILED");
      }

      const expiresAt =
        decoded.expiresAt ?? BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
      return {
        lease: createLease(decoded.token, expiresAt, route, connection),
        authority: Object.freeze({ fencingToken: decoded.token }),
      };
    } finally {
      if (queued) queued.settled = true;
      removeQueued();
      options.signal?.removeEventListener("abort", onAbort);
      unregisterAcquireDisconnect();
    }
  };

  const acquireWithAuthority = (
    route: string,
    ttlSecs: number,
    options: LeaseAcquireOptions,
  ): Promise<{ lease: Lease; authority: LeaseAuthority }> => {
    const result = acquisitionTail.then(() => runAcquire(route, ttlSecs, options));
    acquisitionTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  };

  const acquire = async (route: string, options: LeaseAcquireOptions): Promise<Lease> =>
    (await acquireWithAuthority(route, options.ttlSeconds, options)).lease;

  const withLease = async <T>(
    route: string,
    callback: (signal: AbortSignal, authority: LeaseAuthority) => T | Promise<T>,
    options: WithLeaseOptions,
  ): Promise<T> => {
    const ttlSecs = options.ttlSeconds;
    assertExactLeaseRoute(route);
    assertLeaseTtl(ttlSecs);
    if (options.signal?.aborted) {
      throw options.signal.reason ?? new Error("Lease execution canceled");
    }

    const { lease, authority } = await acquireWithAuthority(route, ttlSecs, {
      ttlSeconds: ttlSecs,
      waitSeconds: options.waitSeconds ?? 0,
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

    // renew()'s loop only discovers a disconnect indirectly, on its next
    // periodic extend() attempt (up to ttlSecs/3 seconds away) — during
    // that whole window the callback would keep running under the false
    // assumption it still exclusively owns the lease. Listen for disconnect
    // directly so the callback's cancellation signal fires immediately,
    // matching createLease's own handle, which does the same.
    const onLeaseConnectionLost = (): void => {
      if (callbackDone || leaseLoss !== undefined) return;
      const error = new LeaseError("Lease ownership was lost", "LOST", undefined, {
        reason: "disconnected",
      });
      leaseLoss = error;
      lifecycle.abort(error);
    };
    const unsubscribeLeaseConnectionLost = connection.onDisconnect(onLeaseConnectionLost);

    const renew = async (): Promise<void> => {
      while (!callbackDone && leaseLoss === undefined) {
        await abortableDelay((ttlSecs * 1000) / 3, stopRenewal.signal).catch(() => undefined);
        if (callbackDone || stopRenewal.signal.aborted) {
          return;
        }
        try {
          await lease.extend({ ttlSeconds: ttlSecs });
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
        callbackValue = await callback(lifecycle.signal, authority);
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
          await lease.release({ signal: cleanup.signal });
        } catch (error) {
          releaseFailure = error;
        } finally {
          clearTimeout(timer);
        }
      }

      // A lease-loss detected during renewal is always a real failure — never
      // filter it as a "managed cancellation," even though it's exactly what
      // caused `lifecycle` to abort. `releaseFailure` comes from `cleanup`, a
      // separate AbortController with its own 5s watchdog that has nothing to
      // do with `lifecycle` — an unconfirmed release must never be treated as
      // a benign cancellation regardless of its error's `.name`. Only
      // `callbackFailure` is eligible for the "managed" classification, and
      // only when `lifecycle` was genuinely aborted for a reason we already
      // track (a lost lease, or the caller's own signal) rather than merely
      // because the callback itself finished (line below always aborts
      // `lifecycle` afterward, win or lose).
      const wasRealAbort = leaseLoss !== undefined || options.signal?.aborted === true;
      const failures = [
        leaseLoss,
        callbackFailure !== undefined &&
        isManagedCancellation(callbackFailure, lifecycle.signal, wasRealAbort)
          ? undefined
          : callbackFailure,
        releaseFailure,
      ].filter((failure) => failure !== undefined);
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
      unsubscribeLeaseConnectionLost();
      if (timer !== undefined) clearTimeout(timer);
      options.signal?.removeEventListener("abort", onParentAbort);
      await renewal.catch(() => undefined);
    }
  };

  const query = async (
    route: string,
    options: { signal?: AbortSignal } = {},
  ): Promise<LeaseInfo> => {
    assertExactLeaseRoute(route);
    return runWithRetry(
      {
        domain: "lease",
        operation: "query",
        retryClass: "replayable_read",
      },
      async () => {
        const payload = LeaseCodec.encodeQuery(route);
        const response = await requestFrame(MSG_LEASE_QUERY, payload, options.signal);
        const decoded = LeaseCodec.decodeQueryResponse(response);
        if (decoded.status !== 0) {
          throw new LeaseError(
            `QUERY failed: ${decoded.errorMessage ?? `status ${decoded.status}`}`,
            "QUERY_FAILED",
            decoded.errorCode,
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

  const subscribe = async (
    route: string,
    handler: ChangeHandler,
    options?: {
      signal?: AbortSignal;
      preDispatch?: (notification: ChangeNotification) => void;
    },
  ): Promise<LeaseSubscription> => {
    assertLeaseSubscriptionPattern(route);
    initNotifyHandler();

    while (true) {
      const existing = subscriptionsByRoute.get(route);
      if (existing) {
        if (existing.pendingUnsubscribe) {
          // An UNSUBSCRIBE for this route is in flight — reusing this
          // state now would register the handler locally without ever
          // sending a fresh wire SUBSCRIBE. Wait it out, then re-decide
          // against whatever state (or lack of one) remains.
          await awaitPendingUnsubscribe(existing);
          continue;
        }
        return addLocalSubscription(
          route,
          existing.subId,
          handler,
          options?.signal,
          options?.preDispatch,
        );
      }

      const subId = await registerSingleFlight(route, () => subscribeWire(route));
      return addLocalSubscription(route, subId, handler, options?.signal, options?.preDispatch);
    }
  };

  const notifications = (
    route: string,
    iteratorOptions?: SubscriptionIteratorOptions,
  ): AsyncIterable<ChangeNotification> =>
    createSubscriptionIterator(
      (handler) => subscribe(route, async (notification) => handler(notification)),
      iteratorOptions,
    );

  const subscribeWire = async (route: string, request = requestFrame): Promise<bigint> => {
    const payload = LeaseCodec.encodeSubscribe(route);
    const parsed = parseStandardResponse(await request(MSG_LEASE_SUBSCRIBE, payload));
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
    signal?: AbortSignal,
    preDispatch?: (notification: ChangeNotification) => void,
  ): LeaseSubscription => {
    const handlerId = nextHandlerId++;
    let subscription = subscriptionsByRoute.get(route);
    if (!subscription) {
      subscription = { subId, handlers: new Map(), generation: subIdGeneration.next() };
      subscriptionsByRoute.set(route, subscription);
      patternsBySubId.set(subId, route);
    }

    const controller = createSubscriptionController<LeaseSubscription>(
      async () => unsubscribe(route, handlerId),
      signal,
    );
    subscription.handlers.set(handlerId, {
      handler,
      fail: (error) => controller.fail(error),
      preDispatch,
    });
    return controller.handle;
  };

  const unsubscribe = async (route: string, handlerId: number): Promise<void> => {
    const subscription = subscriptionsByRoute.get(route);
    if (!subscription || !subscription.handlers.has(handlerId)) {
      return;
    }

    if (subscription.handlers.size > 1) {
      // Other handlers remain — safe to remove this one locally without a
      // wire round-trip.
      subscription.handlers.delete(handlerId);
      return;
    }

    // This is the last handler. Don't remove it locally until the wire
    // UNSUBSCRIBE is confirmed — if it fails, the broker still expects
    // notifications to keep reaching it, and a caller that sees
    // unsubscribe() throw should be able to assume nothing changed.
    const wireUnsubscribe = (async (): Promise<void> => {
      const payload = LeaseCodec.encodeUnsubscribe(route);
      const parsed = parseStandardResponse(await requestFrame(MSG_LEASE_UNSUBSCRIBE, payload));
      if (!parsed.success) {
        throw new LeaseError(
          `UNSUBSCRIBE failed: ${parsed.error ?? "unknown error"}`,
          "UNSUBSCRIBE_FAILED",
          parsed.errorCode,
        );
      }
    })();
    subscription.pendingUnsubscribe = wireUnsubscribe;
    try {
      await wireUnsubscribe;
    } finally {
      if (subscription.pendingUnsubscribe === wireUnsubscribe) {
        subscription.pendingUnsubscribe = undefined;
      }
    }

    subscription.handlers.delete(handlerId);
    // A concurrent subscribe() may have reused this same (not-yet-deleted)
    // state object while the round-trip above was in flight, repopulating
    // `handlers` — only clear the route-level bookkeeping if it's still
    // genuinely empty.
    if (isCurrentEmptyState(subscriptionsByRoute, route, subscription)) {
      subscriptionsByRoute.delete(route);
      patternsBySubId.delete(subscription.subId);
    }
  };

  const initNotifyHandler = (): void => {
    if (initialized) {
      return;
    }

    initialized = true;
    connection.registerNotificationHandler(MSG_LEASE_NOTIFY, (payload) => {
      try {
        const { subId, route } = LeaseCodec.decodeNotification(payload);
        // `route` is the concrete lease route that changed, not necessarily
        // the (possibly wildcard) pattern it was subscribed under — resolve
        // the owning subscription by subId first, matching every other
        // pattern-subscribing domain.
        const pattern = patternsBySubId.get(subId);
        const subscription = pattern === undefined ? undefined : subscriptionsByRoute.get(pattern);
        if (!subscription) {
          return;
        }

        const notification: ChangeNotification = { route };
        for (const registration of subscription.handlers.values()) {
          dispatchSubscriptionHandler(connection, registration, notification, "lease", route);
        }
      } catch (error) {
        connection.reportBackgroundError?.("fitz.lease.notification_malformed", error, {
          messageType: MSG_LEASE_NOTIFY,
        });
      }
    });
  };

  const listPage = async (
    pattern: string,
    options: { cursor?: LeaseListCursor; limit?: number; signal?: AbortSignal } = {},
  ): Promise<LeaseListPage> => {
    assertLeaseSubscriptionPattern(pattern);
    return runWithRetry(
      {
        domain: "lease",
        operation: "list",
        retryClass: "replayable_read",
      },
      async () => {
        const payload = LeaseCodec.encodeList(pattern, {
          cursor: options.cursor,
          limit: options.limit,
        });
        const response = await requestFrame(MSG_LEASE_LIST, payload, options.signal);
        return LeaseCodec.decodeListResponse(response);
      },
    );
  };

  const list = async function* (
    pattern: string,
    options: { pageSize?: number; signal?: AbortSignal } = {},
  ): AsyncIterableIterator<readonly LeaseListItem[]> {
    let cursor: LeaseListCursor | undefined;
    while (true) {
      const page = await listPage(pattern, {
        cursor,
        limit: options.pageSize,
        signal: options.signal,
      });
      yield Object.freeze(page.items.slice());
      if (!page.nextCursor) {
        return;
      }
      cursor = page.nextCursor;
    }
  };

  const observeInventory = async (
    pattern: string,
    options: LeaseInventoryOptions = {},
  ): Promise<LeaseInventoryObserver> => {
    assertLeaseSubscriptionPattern(pattern);
    const baseIntervalMs = options.reconcileIntervalMs ?? DEFAULT_RECONCILE_INTERVAL_MS;
    if (!Number.isFinite(baseIntervalMs) || baseIntervalMs <= 0) {
      throw new LeaseError(
        "reconcileIntervalMs must be a positive finite number of milliseconds",
        "INVALID_OBSERVE_OPTIONS",
      );
    }

    let view = new Map<string, LeaseListItem>();
    let ready = false;
    let subscriptionReady = false;
    let subscriptionGeneration = 0;
    let reconcileRequested = true;
    let activeReconcile: Promise<void> | undefined;
    let closed = false;
    let reconcileTimer: ReturnType<typeof setTimeout> | undefined;
    let activeRecovery: Promise<void> | undefined;
    const updateHandlers = new Set<(snapshot: ReadonlyMap<string, LeaseListItem>) => void>();

    const emitUpdate = (): void => {
      const snapshot = new Map(view);
      for (const handler of updateHandlers) {
        try {
          handler(snapshot);
        } catch (error) {
          connection.reportBackgroundError?.("fitz.lease.observer_handler_failed", error, {
            pattern,
          });
        }
      }
    };

    const fullRelist = async (): Promise<Map<string, LeaseListItem>> => {
      const next = new Map<string, LeaseListItem>();
      for await (const page of list(pattern, { signal: options.signal })) {
        for (const item of page) next.set(item.route, item);
      }
      return next;
    };

    // `requestReconcile(true)` is reserved for genuine-gap callers —
    // bootstrap and post-reconnect re-bootstrap — where the view really is
    // known-stale. Routine callers (a single notification's coalesced
    // relist, the periodic backstop pass, or a chained follow-up pass below)
    // call it with no argument: `snapshot()`'s contract already guarantees a
    // stale read during a routine reconcile pass is safe, so flipping
    // `ready` false for those would just be UI flicker with no benefit.
    const requestReconcile = (suspectGap = false): Promise<void> => {
      reconcileRequested = true;
      if (suspectGap) ready = false;
      if (options.signal?.aborted) {
        ready = false;
        return Promise.resolve();
      }
      if (!subscriptionReady || closed) return Promise.resolve();
      if (activeReconcile) return activeReconcile;

      activeReconcile = (async () => {
        let attempts = 0;
        while (reconcileRequested && !closed) {
          if (!subscriptionReady || options.signal?.aborted) return;
          reconcileRequested = false;
          attempts++;
          const passSubscriptionGeneration = subscriptionGeneration;
          const candidate = await fullRelist();
          if (closed) return;
          if (!subscriptionReady || subscriptionGeneration !== passSubscriptionGeneration) {
            // The wire subscription failed or was replaced during this LIST
            // pass. Never install a candidate captured across that gap; the
            // recovery loop will establish a new subscription and request a
            // fresh pass.
            reconcileRequested = true;
            return;
          }
          if (reconcileRequested) {
            // A known-raced page cannot make bootstrap ready. Back off so
            // continuous churn cannot hammer LIST at wire speed, then keep
            // trying until one complete pass has no raced invalidation (or
            // the caller cancels the observer bootstrap).
            const delayMs = Math.min(
              OBSERVER_RECONCILE_RETRY_BASE_MS * 2 ** Math.min(attempts - 1, 5),
              OBSERVER_RECONCILE_RETRY_MAX_MS,
            );
            try {
              await sleepWithAbort(delayMs, options.signal);
            } catch (error) {
              if (options.signal?.aborted) return;
              throw error;
            }
            continue;
          }

          // No invalidation raced this complete pass. Installing the view,
          // marking ready, and leaving bootstrap mode are synchronous, so a
          // later notification starts a new coalesced pass instead of
          // slipping through an acknowledgement/list handoff gap.
          view = candidate;
          ready = true;
          emitUpdate();
          break;
        }
      })().finally(() => {
        activeReconcile = undefined;
        if (reconcileRequested && subscriptionReady && !closed) {
          void requestReconcile().catch((error) => {
            connection.reportBackgroundError?.("fitz.lease.observer_reconcile_failed", error, {
              pattern,
            });
          });
        }
      });
      return activeReconcile;
    };

    const invalidate = (): void => {
      void requestReconcile().catch((error) => {
        connection.reportBackgroundError?.("fitz.lease.observer_reconcile_failed", error, {
          pattern,
        });
      });
    };

    // Step 1: establish the patterned subscription and wait for its
    // acknowledgement. Notifications that arrive before this resolves set
    // the pending reconciliation flag and are covered by the first LIST.
    let subscription = await subscribe(pattern, () => undefined, {
      signal: options.signal,
      preDispatch: invalidate,
    });
    subscriptionReady = true;
    subscriptionGeneration++;

    // The internal subscription's `completion` only rejects on a genuine
    // local failure (e.g. the bounded async-dispatch queue overflowing),
    // which also fires a real wire UNSUBSCRIBE and deletes this
    // subscription's bookkeeping. Without watching it, that failure would
    // silently degrade the observer to relying only on the periodic
    // reconcile backstop (or freeze outright when reconcileIntervalMs is 0),
    // contradicting the "race-safe" guarantee. Surface it and attempt to
    // recover by resubscribing and re-running bootstrap, mirroring the
    // reconnect path below.
    const watchSubscriptionCompletion = (sub: LeaseSubscription): void => {
      sub.completion.catch((error: unknown) => {
        if (closed || sub !== subscription) return;
        ready = false;
        subscriptionReady = false;
        subscriptionGeneration++;
        reconcileRequested = true;
        connection.reportBackgroundError?.("fitz.lease.observer_subscription_failed", error, {
          pattern,
        });
        void recoverSubscription();
      });
    };

    const recoverSubscription = (replaceSubscription = true): Promise<void> => {
      if (closed) return Promise.resolve();
      if (activeRecovery) return activeRecovery;
      ready = false;
      if (replaceSubscription) subscriptionReady = false;
      activeRecovery = (async () => {
        let attempt = 0;
        while (!closed && !options.signal?.aborted) {
          if (!subscriptionReady) {
            try {
              const nextSubscription = await subscribe(pattern, () => undefined, {
                signal: options.signal,
                preDispatch: invalidate,
              });
              if (closed || options.signal?.aborted) {
                await nextSubscription.unsubscribe().catch(() => undefined);
                return;
              }
              subscription = nextSubscription;
              subscriptionReady = true;
              subscriptionGeneration++;
              watchSubscriptionCompletion(subscription);
            } catch (error) {
              connection.reportBackgroundError?.("fitz.lease.observer_resubscribe_failed", error, {
                pattern,
              });
            }
          }

          if (subscriptionReady) {
            try {
              await requestReconcile(true);
              if (subscriptionReady && ready) return;
            } catch (error) {
              connection.reportBackgroundError?.("fitz.lease.observer_rebootstrap_failed", error, {
                pattern,
              });
            }
          }

          attempt++;
          const delayMs = Math.min(
            OBSERVER_RECONCILE_RETRY_BASE_MS * 2 ** Math.min(attempt - 1, 5),
            OBSERVER_RECONCILE_RETRY_MAX_MS,
          );
          try {
            await sleepWithAbort(delayMs, options.signal);
          } catch {
            if (options.signal?.aborted) return;
          }
        }
      })().finally(() => {
        activeRecovery = undefined;
      });
      return activeRecovery;
    };

    watchSubscriptionCompletion(subscription);

    // Steps 2-5: every notification sets `reconcileRequested`. LIST passes
    // repeat until one completes without a raced invalidation, and only that
    // pass is installed as ready. QUERY is deliberately not used because it
    // omits holder incarnation, acquisition time, and renewal count.
    try {
      await requestReconcile(true);
    } catch (error) {
      await subscription.unsubscribe();
      throw error;
    }

    const unsubscribeReconnect = connection.onReconnect(async () => {
      if (closed) return;
      try {
        await requestReconcile(true);
      } catch (error) {
        connection.reportBackgroundError?.("fitz.lease.observer_rebootstrap_failed", error, {
          pattern,
        });
        // A failed reconnect LIST leaves the view knowingly stale even
        // though the generic reconnect path restored the subscription.
        // Retry the relist with bounded backoff rather than waiting for a
        // notification or the periodic backstop.
        void recoverSubscription(false);
      }
    });

    const scheduleReconcile = (): void => {
      if (closed || baseIntervalMs <= 0) return;
      const jitter = 1 + (Math.random() * 0.4 - 0.2); // +/- 20%
      const delay = Math.max(0, Math.round(baseIntervalMs * jitter));
      reconcileTimer = setTimeout(() => {
        // `closed` alone isn't enough: `LeaseInventoryOptions.signal` is
        // documented to cancel background work once running, so an aborted
        // signal must stop this periodic timer too, not just close().
        if (closed || options.signal?.aborted) return;
        void (async () => {
          try {
            await requestReconcile();
          } catch (error) {
            connection.reportBackgroundError?.("fitz.lease.observer_reconcile_failed", error, {
              pattern,
            });
          } finally {
            scheduleReconcile();
          }
        })();
      }, delay);
    };
    scheduleReconcile();

    const handleObserverAbort = (): void => {
      ready = false;
      if (reconcileTimer !== undefined) clearTimeout(reconcileTimer);
    };
    options.signal?.addEventListener("abort", handleObserverAbort, { once: true });

    const close = async (): Promise<void> => {
      if (closed) return;
      closed = true;
      if (reconcileTimer !== undefined) clearTimeout(reconcileTimer);
      options.signal?.removeEventListener("abort", handleObserverAbort);
      unsubscribeReconnect();
      updateHandlers.clear();
      if (activeReconcile) await activeReconcile.catch(() => undefined);
      if (activeRecovery) await activeRecovery.catch(() => undefined);
      await subscription.unsubscribe();
    };

    return {
      get ready(): boolean {
        return ready;
      },
      snapshot: (): ReadonlyMap<string, LeaseListItem> => new Map(view),
      onUpdate: (handler: (snapshot: ReadonlyMap<string, LeaseListItem>) => void): (() => void) => {
        updateHandlers.add(handler);
        return () => updateHandlers.delete(handler);
      },
      close,
      async [Symbol.asyncDispose](): Promise<void> {
        try {
          await close();
        } catch {
          // Disposal is explicitly best effort.
        }
      },
    };
  };

  return {
    acquire,
    withLease,
    query,
    subscribe,
    notifications,
    listPage,
    list,
    observeInventory,
  };
}

export * from "./types";

const DEFAULT_RECONCILE_INTERVAL_MS = 60_000;
const OBSERVER_RECONCILE_RETRY_BASE_MS = 10;
const OBSERVER_RECONCILE_RETRY_MAX_MS = 250;

function assertExactLeaseRoute(route: string): void {
  if (!isRouteShape(route, "lease", 3)) {
    throw new LeaseError(
      `Invalid lease route: ${route} (expected lease://{realm}/{area}/{resource}, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}

function assertLeaseSubscriptionPattern(pattern: string): void {
  if (!isRegistrationPatternShape(pattern, "lease", 3)) {
    throw new LeaseError(
      `Invalid lease pattern: ${pattern} (expected an exact lease://{realm}/{area}/{resource} route, whole-segment * wildcards, or a trailing ** alias)`,
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

// `lifecycle` is aborted unconditionally once the callback settles (even on
// success), so `lifecycle.signal.aborted` alone can't distinguish "this
// failure is the callback's expected reaction to a real external
// cancellation" from "this just happens to be named AbortError." Callers
// must pass `wasRealAbort` — true only when `lifecycle` was aborted for a
// reason we already know about (a lost lease, or the caller's own signal) —
// so an unrelated AbortError-named error isn't silently swallowed.
function isManagedCancellation(
  error: unknown,
  signal: AbortSignal,
  wasRealAbort: boolean,
): boolean {
  return (
    wasRealAbort &&
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
