/**
 * Schedule domain client.
 */

import { Connection } from "../../client/connection";
import {
  MSG_SCHEDULE_CANCEL,
  MSG_SCHEDULE_CREATE,
  MSG_SCHEDULE_LIST,
  MSG_SCHEDULE_NOTIFY,
  MSG_SCHEDULE_SUBSCRIBE,
  MSG_SCHEDULE_UNSUBSCRIBE,
} from "../../frame/types";
import { DomainClient } from "../base";
import { parseStandardResponse } from "../../protocol/response";
import { ScheduleCodec } from "./codec";
import {
  ScheduleEntry,
  ScheduleError,
  ScheduleHandler,
  ScheduleNotification,
  ScheduleSubscription,
} from "./types";

export class ScheduleClient extends DomainClient {
  private readonly subscriptions = new Map<
    bigint,
    { pattern: string; handler: ScheduleHandler }
  >();
  private notifyHandlerInitialized = false;

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

  async create(
    route: string,
    cronExpr: string,
    payload: Uint8Array = new Uint8Array(),
  ): Promise<string> {
    const response = await this.requestFrame(
      MSG_SCHEDULE_CREATE,
      ScheduleCodec.encodeCreate(route, cronExpr, payload),
    );
    const decoded = ScheduleCodec.decodeCreateResponse(
      this.assertSuccess(response, "CREATE"),
    );
    return decoded.scheduleId ?? route;
  }

  async cancel(route: string): Promise<void> {
    const response = await this.requestFrame(
      MSG_SCHEDULE_CANCEL,
      ScheduleCodec.encodeCancel(route),
    );
    ScheduleCodec.decodeCancelResponse(this.assertSuccess(response, "CANCEL"));
  }

  async list(
    offset: bigint = 0n,
    limit: bigint = 0n,
  ): Promise<[ScheduleEntry[], bigint]> {
    const response = await this.requestFrame(
      MSG_SCHEDULE_LIST,
      ScheduleCodec.encodeList(offset, limit),
    );
    const decoded = ScheduleCodec.decodeListResponse(
      this.assertSuccess(response, "LIST"),
    );
    return [decoded.entries, decoded.totalCount];
  }

  async subscribe(
    pattern: string,
    handler: ScheduleHandler,
  ): Promise<ScheduleSubscription> {
    this.initNotifyHandler();
    const response = await this.requestFrame(
      MSG_SCHEDULE_SUBSCRIBE,
      ScheduleCodec.encodeSubscribe(pattern),
    );
    const decoded = ScheduleCodec.decodeSubscribeResponse(
      this.assertSuccess(response, "SUBSCRIBE"),
    );

    const subId = decoded.subId ?? BigInt(this.subscriptions.size + 1);
    this.subscriptions.set(subId, { pattern, handler });
    return new ScheduleSubscription(subId, pattern, handler, async () => {
      await this.unsubscribe(subId, pattern);
    });
  }

  private async unsubscribe(subId: bigint, pattern: string): Promise<void> {
    this.subscriptions.delete(subId);
    const response = await this.requestFrame(
      MSG_SCHEDULE_UNSUBSCRIBE,
      ScheduleCodec.encodeUnsubscribe(pattern),
    );
    ScheduleCodec.decodeUnsubscribeResponse(
      this.assertSuccess(response, "UNSUBSCRIBE"),
    );
  }

  private initNotifyHandler(): void {
    if (this.notifyHandlerInitialized) {
      return;
    }

    this.notifyHandlerInitialized = true;
    this.connection.registerNotificationHandler(
      MSG_SCHEDULE_NOTIFY,
      (payload) => {
        try {
          const decoded = ScheduleCodec.decodeNotification(payload);
          if (decoded.subId !== undefined) {
            const subscription = this.subscriptions.get(decoded.subId);
            if (!subscription) {
              return;
            }

            const notification: ScheduleNotification = {
              payload: decoded.payload,
            };
            Promise.resolve(subscription.handler(notification)).catch(
              () => undefined,
            );
            return;
          }

          const subscriptions = Array.from(this.subscriptions.values());
          if (subscriptions.length === 0) {
            return;
          }

          const notification: ScheduleNotification = {
            payload: decoded.payload,
          };
          for (const subscription of subscriptions) {
            Promise.resolve(subscription.handler(notification)).catch(
              () => undefined,
            );
          }
        } catch {
          // Best-effort notification dispatch.
        }
      },
    );
  }

  private assertSuccess(payload: Uint8Array, operation: string): Uint8Array {
    const result = parseStandardResponse(payload);
    if (result.success) {
      return result.data;
    }

    throw new ScheduleError(
      `${operation} failed: ${result.error ?? "Unknown error"}`,
      this.mapErrorCode(result.error),
    );
  }

  private mapErrorCode(message?: string): string {
    const normalized = message?.toLowerCase() ?? "";
    if (normalized.includes("not found")) {
      return "NOT_FOUND";
    }
    if (normalized.includes("cron")) {
      return "INVALID_CRON";
    }
    return "REQUEST_FAILED";
  }
}
