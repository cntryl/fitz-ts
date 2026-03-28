/**
 * Lease domain client.
 */

import { Connection } from "../../client/connection";
import { LeaseError } from "../../core/errors";
import {
  MSG_LEASE_ACQUIRE,
  MSG_LEASE_NOTIFY,
  MSG_LEASE_QUERY,
  MSG_LEASE_SUBSCRIBE,
  MSG_LEASE_UNSUBSCRIBE,
} from "../../frame/types";
import { DomainClient } from "../base";
import { LeaseCodec } from "./codec";
import { ChangeHandler, ChangeNotification, Lease, LeaseInfo, LeaseSubscription } from "./types";

type LeaseSubscriptionState = {
  subId: bigint;
  handlers: Map<number, ChangeHandler>;
};

export class LeaseClient extends DomainClient {
  private readonly subscriptionsByPattern = new Map<string, LeaseSubscriptionState>();
  private readonly patternsBySubId = new Map<bigint, string>();
  private initialized = false;
  private nextHandlerId = 1;

  constructor(connection: Connection) {
    super(connection);
    this.connection.onReconnect(async () => {
      if (this.subscriptionsByPattern.size === 0) {
        return;
      }

      const subscriptions = Array.from(
        this.subscriptionsByPattern.entries(),
        ([pattern, state]) => ({
          pattern,
          handlers: Array.from(state.handlers.entries()),
        }),
      );
      this.subscriptionsByPattern.clear();
      this.patternsBySubId.clear();

      for (const subscription of subscriptions) {
        const subId = await this.subscribeWire(subscription.pattern);
        this.subscriptionsByPattern.set(subscription.pattern, {
          subId,
          handlers: new Map(subscription.handlers),
        });
        this.patternsBySubId.set(subId, subscription.pattern);
      }
    });
  }

  async acquire(route: string, ttlSecs: number): Promise<Lease> {
    const payload = LeaseCodec.encodeAcquire(route, ttlSecs);
    const response = await this.requestFrame(MSG_LEASE_ACQUIRE, payload);
    const decoded = LeaseCodec.decodeAcquireResponse(response);

    if (decoded.token === undefined) {
      throw new LeaseError("ACQUIRE failed", "ACQUIRE_FAILED");
    }

    const expiresAt = decoded.expiresAt ?? BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
    return new Lease(decoded.token, expiresAt, route, this.connection);
  }

  async query(route: string): Promise<LeaseInfo> {
    const payload = LeaseCodec.encodeQuery(route);
    const response = await this.requestFrame(MSG_LEASE_QUERY, payload);
    const decoded = LeaseCodec.decodeQueryResponse(response);
    if (decoded.status !== 0) {
      throw new LeaseError("QUERY failed", "QUERY_FAILED", decoded.status);
    }
    return {
      isHeld: decoded.isHeld ?? false,
      owner: decoded.owner,
      token: decoded.token,
      expiresAt: decoded.expiresAt,
    };
  }

  async subscribe(pattern: string, handler: ChangeHandler): Promise<LeaseSubscription> {
    this.initNotifyHandler();
    const existing = this.subscriptionsByPattern.get(pattern);
    if (existing) {
      return this.addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await this.subscribeWire(pattern);
    return this.addLocalSubscription(pattern, subId, handler);
  }

  private async subscribeWire(pattern: string): Promise<bigint> {
    const payload = LeaseCodec.encodeSubscribe(pattern);
    const response = await this.requestFrame(MSG_LEASE_SUBSCRIBE, payload);
    const decoded = LeaseCodec.decodeSubscribeResponse(response);

    if (decoded.subId === undefined) {
      throw new LeaseError("SUBSCRIBE failed", "SUBSCRIBE_FAILED");
    }

    return decoded.subId;
  }

  private addLocalSubscription(
    pattern: string,
    subId: bigint,
    handler: ChangeHandler,
  ): LeaseSubscription {
    const handlerId = this.nextHandlerId++;
    let subscription = this.subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      this.subscriptionsByPattern.set(pattern, subscription);
      this.patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    return new LeaseSubscription(subId, pattern, async () => {
      await this.unsubscribe(pattern, handlerId);
    });
  }

  private async unsubscribe(pattern: string, handlerId: number): Promise<void> {
    const subscription = this.subscriptionsByPattern.get(pattern);
    if (!subscription) {
      return;
    }

    subscription.handlers.delete(handlerId);
    if (subscription.handlers.size > 0) {
      return;
    }

    this.subscriptionsByPattern.delete(pattern);
    this.patternsBySubId.delete(subscription.subId);
    const payload = LeaseCodec.encodeUnsubscribe(pattern);
    await this.requestFrame(MSG_LEASE_UNSUBSCRIBE, payload);
  }

  private initNotifyHandler(): void {
    if (this.initialized) {
      return;
    }

    this.initialized = true;
    this.connection.registerNotificationHandler(MSG_LEASE_NOTIFY, (payload) => {
      try {
        const { subId, route } = LeaseCodec.decodeNotification(payload);
        const pattern = this.patternsBySubId.get(subId);
        if (!pattern) {
          return;
        }

        const subscription = this.subscriptionsByPattern.get(pattern);
        if (!subscription) {
          return;
        }

        const notification: ChangeNotification = { route };
        for (const handler of subscription.handlers.values()) {
          this.connection.dispatchAsyncHandler(async () => {
            await handler(notification);
          });
        }
      } catch {
        // Best-effort notification dispatch.
      }
    });
  }
}

export * from "./types";
