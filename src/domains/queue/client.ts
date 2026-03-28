/**
 * Queue domain client.
 */

import { Connection } from "../../client/connection";
import { QueueError } from "../../core/errors";
import {
  MSG_QUEUE_ENQUEUE,
  MSG_QUEUE_NOTIFY,
  MSG_QUEUE_RESERVE,
  MSG_QUEUE_SUBSCRIBE,
  MSG_QUEUE_UNSUBSCRIBE,
} from "../../frame/types";
import { DomainClient } from "../base";
import { QueueCodec } from "./codec";
import {
  AvailabilityHandler,
  AvailabilityNotification,
  EnqueueOptions,
  QueueItem,
  QueueStatus,
  QueueSubscription,
} from "./types";

type QueueSubscriptionState = {
  subId: bigint;
  handlers: Map<number, AvailabilityHandler>;
};

export class QueueClient extends DomainClient {
  private readonly subscriptionsByPattern = new Map<string, QueueSubscriptionState>();
  private readonly patternsBySubId = new Map<bigint, string>();
  private notificationHandlerRegistered = false;
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

  async enqueue(route: string, body: Uint8Array, options?: EnqueueOptions): Promise<bigint> {
    const payload = QueueCodec.encodeEnqueue(route, body, options);
    const response = await this.requestFrame(MSG_QUEUE_ENQUEUE, payload);
    const decoded = QueueCodec.decodeEnqueueResponse(response);
    this.checkStatus(decoded.status, "ENQUEUE");

    if (decoded.messageId === undefined) {
      throw new QueueError("ENQUEUE response missing messageId", "MISSING_MESSAGE_ID");
    }

    return decoded.messageId;
  }

  async reserve(
    route: string,
    leaseSeconds: number,
    batchSize: number = 1,
    waitSeconds: number = 0,
  ): Promise<QueueItem[]> {
    if (waitSeconds <= 0) {
      return this.reserveOnce(route, leaseSeconds, batchSize, 0);
    }

    let items = await this.reserveOnce(route, leaseSeconds, batchSize, 0);
    if (items.length > 0) {
      return items;
    }

    const deadline = Date.now() + waitSeconds * 1000;
    let pendingNotifications = 0;
    let waiter: (() => void) | undefined;
    const subscription = await this.subscribe(route, async () => {
      pendingNotifications += 1;
      if (!waiter) {
        return;
      }

      const resolve = waiter;
      waiter = undefined;
      pendingNotifications = 0;
      resolve();
    });

    try {
      while (true) {
        items = await this.reserveOnce(route, leaseSeconds, batchSize, 0);
        if (items.length > 0) {
          return items;
        }

        const remainingMs = deadline - Date.now();
        if (remainingMs <= 0) {
          return items;
        }

        await new Promise<void>((resolve) => {
          if (pendingNotifications > 0) {
            pendingNotifications = 0;
            resolve();
            return;
          }

          const release = () => {
            clearTimeout(timeoutId);
            if (waiter === release) {
              waiter = undefined;
            }
            resolve();
          };
          const timeoutId = setTimeout(release, remainingMs);
          waiter = release;
        });
      }
    } finally {
      await subscription.unsubscribe();
    }
  }

  private async reserveOnce(
    route: string,
    leaseSeconds: number,
    batchSize: number,
    waitSeconds: number,
  ): Promise<QueueItem[]> {
    const payload = QueueCodec.encodeReserve(route, leaseSeconds, batchSize, waitSeconds);
    const response = await this.requestFrame(MSG_QUEUE_RESERVE, payload);
    const decoded = QueueCodec.decodeReserveResponse(response);
    this.checkStatus(decoded.status, "RESERVE");

    return (decoded.items ?? []).map(
      (item) => new QueueItem(item.id, item.token, item.body, route, this.connection),
    );
  }

  async subscribe(pattern: string, handler: AvailabilityHandler): Promise<QueueSubscription> {
    this.initNotificationHandler();
    const existing = this.subscriptionsByPattern.get(pattern);
    if (existing) {
      return this.addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await this.subscribeWire(pattern);
    return this.addLocalSubscription(pattern, subId, handler);
  }

  private async subscribeWire(pattern: string): Promise<bigint> {
    const payload = QueueCodec.encodeSubscribe(pattern);
    const response = await this.requestFrame(MSG_QUEUE_SUBSCRIBE, payload);
    const decoded = QueueCodec.decodeSubscribeResponse(response);
    this.checkStatus(decoded.status, "SUBSCRIBE");

    if (decoded.subId === undefined) {
      throw new QueueError("SUBSCRIBE response missing subId", "MISSING_SUB_ID");
    }

    return decoded.subId;
  }

  private addLocalSubscription(
    pattern: string,
    subId: bigint,
    handler: AvailabilityHandler,
  ): QueueSubscription {
    const handlerId = this.nextHandlerId++;
    let subscription = this.subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      this.subscriptionsByPattern.set(pattern, subscription);
      this.patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);

    return new QueueSubscription(subId, pattern, async () => {
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
    const payload = QueueCodec.encodeUnsubscribe(pattern);
    const response = await this.requestFrame(MSG_QUEUE_UNSUBSCRIBE, payload);
    const decoded = QueueCodec.decodeUnsubscribeResponse(response);
    this.checkStatus(decoded.status, "UNSUBSCRIBE");
  }

  private initNotificationHandler(): void {
    if (this.notificationHandlerRegistered) {
      return;
    }

    this.notificationHandlerRegistered = true;
    this.connection.registerNotificationHandler(MSG_QUEUE_NOTIFY, (payload) => {
      try {
        const { subId, route } = QueueCodec.decodeNotification(payload);
        const pattern = this.patternsBySubId.get(subId);
        if (!pattern) {
          return;
        }

        const subscription = this.subscriptionsByPattern.get(pattern);
        if (!subscription) {
          return;
        }

        const notification: AvailabilityNotification = { route };
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

  private checkStatus(status: number, operation: string): void {
    if (status === QueueStatus.Ok) {
      return;
    }

    const statusNames: Record<number, string> = {
      [QueueStatus.QueueNotFound]: "QueueNotFound",
      [QueueStatus.MessageNotFound]: "MessageNotFound",
      [QueueStatus.InvalidToken]: "InvalidToken",
      [QueueStatus.QueueFull]: "QueueFull",
      [QueueStatus.InvalidDelay]: "InvalidDelay",
    };

    throw new QueueError(
      `${operation} failed: ${statusNames[status] ?? `Unknown(${status})`}`,
      operation,
      status,
    );
  }
}

export * from "./types";
