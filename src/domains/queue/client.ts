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

export class QueueClient extends DomainClient {
  private readonly subscriptions = new Map<
    bigint,
    { pattern: string; handler: AvailabilityHandler }
  >();
  private notificationHandlerRegistered = false;

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

  async enqueue(
    route: string,
    body: Uint8Array,
    options?: EnqueueOptions,
  ): Promise<bigint> {
    const payload = QueueCodec.encodeEnqueue(route, body, options);
    const response = await this.requestFrame(MSG_QUEUE_ENQUEUE, payload);
    const decoded = QueueCodec.decodeEnqueueResponse(response);
    this.checkStatus(decoded.status, "ENQUEUE");

    if (decoded.messageId === undefined) {
      throw new QueueError(
        "ENQUEUE response missing messageId",
        "MISSING_MESSAGE_ID",
      );
    }

    return decoded.messageId;
  }

  async reserve(
    route: string,
    leaseSeconds: number,
    batchSize: number = 1,
    waitSeconds: number = 0,
  ): Promise<QueueItem[]> {
    const payload = QueueCodec.encodeReserve(
      route,
      leaseSeconds,
      batchSize,
      waitSeconds,
    );
    const response = await this.requestFrame(MSG_QUEUE_RESERVE, payload);
    const decoded = QueueCodec.decodeReserveResponse(response);
    this.checkStatus(decoded.status, "RESERVE");

    return (decoded.items ?? []).map(
      (item) =>
        new QueueItem(item.id, item.token, item.body, route, this.connection),
    );
  }

  async subscribe(
    pattern: string,
    handler: AvailabilityHandler,
  ): Promise<QueueSubscription> {
    this.initNotificationHandler();

    const payload = QueueCodec.encodeSubscribe(pattern);
    const response = await this.requestFrame(MSG_QUEUE_SUBSCRIBE, payload);
    const decoded = QueueCodec.decodeSubscribeResponse(response);
    this.checkStatus(decoded.status, "SUBSCRIBE");

    if (decoded.subId === undefined) {
      throw new QueueError(
        "SUBSCRIBE response missing subId",
        "MISSING_SUB_ID",
      );
    }

    this.subscriptions.set(decoded.subId, { pattern, handler });

    return new QueueSubscription(decoded.subId, pattern, async (subId) => {
      await this.unsubscribe(subId);
    });
  }

  private async unsubscribe(subId: bigint): Promise<void> {
    const subscription = this.subscriptions.get(subId);
    if (!subscription) {
      return;
    }

    this.subscriptions.delete(subId);
    const payload = QueueCodec.encodeUnsubscribe(subscription.pattern);
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
        const subscription = this.subscriptions.get(subId);
        if (!subscription) {
          return;
        }

        const notification: AvailabilityNotification = { route };
        this.connection.dispatchAsyncHandler(async () => {
          await subscription.handler(notification);
        });
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
