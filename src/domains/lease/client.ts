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
import {
  ChangeHandler,
  ChangeNotification,
  Lease,
  LeaseInfo,
  LeaseSubscription,
} from "./types";

export class LeaseClient extends DomainClient {
  private readonly subscriptions = new Map<
    bigint,
    { pattern: string; handler: ChangeHandler }
  >();
  private initialized = false;

  constructor(connection: Connection) {
    super(connection);
    this.connection.onReconnect(async () => {
      if (this.subscriptions.size === 0) {
        return;
      }

      const subscriptions = Array.from(this.subscriptions.values());
      this.subscriptions.clear();
      for (const subscription of subscriptions) {
        await this.subscribe(subscription.pattern, subscription.handler);
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

    const expiresAt =
      decoded.expiresAt ??
      BigInt(Math.floor(Date.now() / 1000)) + BigInt(ttlSecs);
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

  async subscribe(
    pattern: string,
    handler: ChangeHandler,
  ): Promise<LeaseSubscription> {
    this.initNotifyHandler();
    const payload = LeaseCodec.encodeSubscribe(pattern);
    const response = await this.requestFrame(MSG_LEASE_SUBSCRIBE, payload);
    const decoded = LeaseCodec.decodeSubscribeResponse(response);

    if (decoded.subId === undefined) {
      throw new LeaseError("SUBSCRIBE failed", "SUBSCRIBE_FAILED");
    }

    this.subscriptions.set(decoded.subId, { pattern, handler });
    return new LeaseSubscription(decoded.subId, pattern, async (subId) => {
      await this.unsubscribe(subId);
    });
  }

  private async unsubscribe(subId: bigint): Promise<void> {
    const subscription = this.subscriptions.get(subId);
    if (!subscription) {
      return;
    }

    this.subscriptions.delete(subId);
    const payload = LeaseCodec.encodeUnsubscribe(subscription.pattern);
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
        const subscription = this.subscriptions.get(subId);
        if (!subscription) {
          return;
        }

        const notification: ChangeNotification = { route };
        this.connection.dispatchAsyncHandler(async () => {
          await subscription.handler(notification);
        });
      } catch {
        // Best-effort notification dispatch.
      }
    });
  }
}

export * from "./types";
