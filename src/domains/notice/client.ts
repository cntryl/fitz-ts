/**
 * Notice domain client.
 */

import { Connection } from "../../client/connection";
import { NoticeError } from "../../core/errors";
import {
  MSG_NOTICE_NOTIFY,
  MSG_NOTICE_PUBLISH,
  MSG_NOTICE_SUBSCRIBE,
  MSG_NOTICE_UNSUBSCRIBE,
} from "../../frame/types";
import { DomainClient } from "../base";
import { NoticeCodec } from "./codec";
import { NoticeHandler, NoticeMsg, NoticeSubscription } from "./types";

type NoticeSubscriptionState = {
  subId: bigint;
  handlers: Map<number, NoticeHandler>;
};

export class NoticeClient extends DomainClient {
  private readonly subscriptionsByPattern = new Map<string, NoticeSubscriptionState>();
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

  async publish(route: string, body: Uint8Array): Promise<void> {
    const payload = NoticeCodec.encodePublish(route, body);
    const cancelOptionalResponse = this.connection
      .getMultiplexer()
      .expectOptionalResponse(MSG_NOTICE_PUBLISH);
    try {
      await this.connection.sendFireAndForget(MSG_NOTICE_PUBLISH, payload);
    } catch (error) {
      cancelOptionalResponse();
      throw error;
    }
  }

  async subscribe(pattern: string, handler: NoticeHandler): Promise<NoticeSubscription> {
    this.initNotifyHandler();
    const existing = this.subscriptionsByPattern.get(pattern);
    if (existing) {
      return this.addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await this.subscribeWire(pattern);
    return this.addLocalSubscription(pattern, subId, handler);
  }

  private async subscribeWire(pattern: string): Promise<bigint> {
    const payload = NoticeCodec.encodeSubscribe(pattern);
    const response = await this.requestFrame(MSG_NOTICE_SUBSCRIBE, payload);
    const decoded = NoticeCodec.decodeSubscribeResponse(response);

    if (decoded.subId === undefined) {
      throw new NoticeError("SUBSCRIBE response missing subId", "MISSING_SUB_ID");
    }

    return decoded.subId;
  }

  private addLocalSubscription(
    pattern: string,
    subId: bigint,
    handler: NoticeHandler,
  ): NoticeSubscription {
    const handlerId = this.nextHandlerId++;
    let subscription = this.subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      this.subscriptionsByPattern.set(pattern, subscription);
      this.patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    return new NoticeSubscription(subId, pattern, async () => {
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
    const payload = NoticeCodec.encodeUnsubscribe(pattern);
    await this.requestFrame(MSG_NOTICE_UNSUBSCRIBE, payload);
  }

  private initNotifyHandler(): void {
    if (this.initialized) {
      return;
    }

    this.initialized = true;
    this.connection.registerNotificationHandler(MSG_NOTICE_NOTIFY, (payload) => {
      try {
        const { subId, route, body } = NoticeCodec.decodeNotification(payload);
        const pattern = this.patternsBySubId.get(subId);
        if (!pattern) {
          return;
        }

        const subscription = this.subscriptionsByPattern.get(pattern);
        if (!subscription) {
          return;
        }

        const msg: NoticeMsg = { route, body };
        for (const handler of subscription.handlers.values()) {
          this.connection.dispatchAsyncHandler(async () => {
            await handler(msg);
          });
        }
      } catch {
        // Best-effort notification dispatch.
      }
    });
  }
}

export * from "./types";
