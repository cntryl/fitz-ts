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

export class NoticeClient extends DomainClient {
  private readonly subscriptions = new Map<
    bigint,
    { pattern: string; handler: NoticeHandler }
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

  async subscribe(
    pattern: string,
    handler: NoticeHandler,
  ): Promise<NoticeSubscription> {
    this.initNotifyHandler();
    const payload = NoticeCodec.encodeSubscribe(pattern);
    const response = await this.requestFrame(MSG_NOTICE_SUBSCRIBE, payload);
    const decoded = NoticeCodec.decodeSubscribeResponse(response);

    if (decoded.subId === undefined) {
      throw new NoticeError(
        "SUBSCRIBE response missing subId",
        "MISSING_SUB_ID",
      );
    }

    this.subscriptions.set(decoded.subId, { pattern, handler });
    return new NoticeSubscription(decoded.subId, pattern, async (subId) => {
      await this.unsubscribe(subId);
    });
  }

  private async unsubscribe(subId: bigint): Promise<void> {
    const subscription = this.subscriptions.get(subId);
    if (!subscription) {
      return;
    }

    this.subscriptions.delete(subId);
    const payload = NoticeCodec.encodeUnsubscribe(subscription.pattern);
    await this.requestFrame(MSG_NOTICE_UNSUBSCRIBE, payload);
  }

  private initNotifyHandler(): void {
    if (this.initialized) {
      return;
    }

    this.initialized = true;
    this.connection.registerNotificationHandler(
      MSG_NOTICE_NOTIFY,
      (payload) => {
        try {
          const { subId, route, body } =
            NoticeCodec.decodeNotification(payload);
          const subscription = this.subscriptions.get(subId);
          if (!subscription) {
            return;
          }

          const msg: NoticeMsg = { route, body };
          this.connection.dispatchAsyncHandler(async () => {
            await subscription.handler(msg);
          });
        } catch {
          // Best-effort notification dispatch.
        }
      },
    );
  }
}

export * from "./types";
