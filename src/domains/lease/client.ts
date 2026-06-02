/**
 * Lease domain client.
 */

import { createDomainClient } from "../base";
import type { Connection } from "../../client/connection";
import { LeaseError } from "../../core/errors";
import {
  MSG_LEASE_ACQUIRE,
  MSG_LEASE_NOTIFY,
  MSG_LEASE_QUERY,
  MSG_LEASE_SUBSCRIBE,
  MSG_LEASE_UNSUBSCRIBE,
} from "../../frame/types";
import { isRouteShape } from "../_routes";
import { LeaseCodec } from "./codec";
import {
  ChangeHandler,
  ChangeNotification,
  Lease,
  LeaseInfo,
  LeaseSubscription,
  createLease,
  createLeaseSubscription,
} from "./types";

type LeaseSubscriptionState = {
  subId: bigint;
  handlers: Map<number, ChangeHandler>;
};

export type LeaseClient = ReturnType<typeof createLeaseClient>;

export function createLeaseClient(connection: Connection) {
  const { requestFrame } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, LeaseSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  let initialized = false;
  let nextHandlerId = 1;

  connection.onReconnect(async () => {
    if (subscriptionsByPattern.size === 0) {
      return;
    }

    const subscriptions = Array.from(subscriptionsByPattern.entries(), ([pattern, state]) => ({
      pattern,
      handlers: Array.from(state.handlers.entries()),
    }));
    subscriptionsByPattern.clear();
    patternsBySubId.clear();

    for (const subscription of subscriptions) {
      const subId = await subscribeWire(subscription.pattern);
      subscriptionsByPattern.set(subscription.pattern, {
        subId,
        handlers: new Map(subscription.handlers),
      });
      patternsBySubId.set(subId, subscription.pattern);
    }
  });

  const acquire = async (route: string, ttlSecs: number): Promise<Lease> => {
    assertExactLeaseRoute(route);
    const payload = LeaseCodec.encodeAcquire(route, ttlSecs);
    const response = await requestFrame(MSG_LEASE_ACQUIRE, payload);
    const decoded = LeaseCodec.decodeAcquireResponse(response);

    if (decoded.token === undefined) {
      throw new LeaseError("ACQUIRE failed", "ACQUIRE_FAILED");
    }

    const expiresAt = decoded.expiresAt ?? BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
    return createLease(decoded.token, expiresAt, route, connection);
  };

  const query = async (route: string): Promise<LeaseInfo> => {
    assertExactLeaseRoute(route);
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
      expiresAt: decoded.expiresAt,
    };
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

  const subscribeWire = async (pattern: string): Promise<bigint> => {
    const payload = LeaseCodec.encodeSubscribe(pattern);
    const response = await requestFrame(MSG_LEASE_SUBSCRIBE, payload);
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
      patternsBySubId.set(subId, pattern);
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
    patternsBySubId.delete(subscription.subId);
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
        const { subId, route } = LeaseCodec.decodeNotification(payload);
        const pattern = patternsBySubId.get(subId);
        if (!pattern) {
          return;
        }

        const subscription = subscriptionsByPattern.get(pattern);
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
    query,
    subscribe,
  };
}

type LeaseClientConstructor = {
  new (connection: Connection): LeaseClient;
  (connection: Connection): LeaseClient;
};

export const LeaseClient: LeaseClientConstructor = function (connection: Connection) {
  return createLeaseClient(connection);
} as unknown as LeaseClientConstructor;

export * from "./types";

function assertExactLeaseRoute(route: string): void {
  if (!isRouteShape(route, "lease", 3)) {
    throw new LeaseError(
      `Invalid lease route: ${route} (expected lease://{realm}/{area}/{resource}, no empty segments or wildcards)`,
      "INVALID_ROUTE",
    );
  }
}
