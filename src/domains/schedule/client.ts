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

function isValidConcreteScheduleRoute(route: string): boolean {
  return /^schedule:\/\/([^/*]+)\/([^/*]+)\/([^/*]+)\/([^/*]+)$/.test(route);
}

function assertConcreteScheduleRoute(route: string, noun: string): void {
  if (!isValidConcreteScheduleRoute(route)) {
    throw new ScheduleError(`Invalid ${noun}: ${route}`, "INVALID_ROUTE");
  }
}

type ScheduleSubscriptionState = {
  subId: bigint;
  handlers: Map<number, ScheduleHandler>;
};

export class ScheduleClient extends DomainClient {
  private readonly subscriptionsByPattern = new Map<string, ScheduleSubscriptionState>();
  private readonly patternsBySubId = new Map<bigint, string>();
  private notifyHandlerInitialized = false;
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

  async create(
    route: string,
    cronExpr: string,
    payload: Uint8Array = new Uint8Array(),
  ): Promise<string> {
    assertConcreteScheduleRoute(route, "route");

    const response = await this.requestFrame(
      MSG_SCHEDULE_CREATE,
      ScheduleCodec.encodeCreate(route, cronExpr, payload),
    );
    const decoded = ScheduleCodec.decodeCreateResponse(this.assertSuccess(response, "CREATE"));
    return decoded.scheduleId ?? route;
  }

  async cancel(route: string): Promise<void> {
    assertConcreteScheduleRoute(route, "route");

    const response = await this.requestFrame(
      MSG_SCHEDULE_CANCEL,
      ScheduleCodec.encodeCancel(route),
    );
    ScheduleCodec.decodeCancelResponse(this.assertSuccess(response, "CANCEL"));
  }

  async list(offset: bigint = 0n, limit: bigint = 0n): Promise<[ScheduleEntry[], bigint]> {
    const response = await this.requestFrame(
      MSG_SCHEDULE_LIST,
      ScheduleCodec.encodeList(offset, limit),
    );
    const decoded = ScheduleCodec.decodeListResponse(this.assertSuccess(response, "LIST"));
    return [decoded.entries, decoded.totalCount];
  }

  async subscribe(pattern: string, handler: ScheduleHandler): Promise<ScheduleSubscription> {
    assertConcreteScheduleRoute(pattern, "pattern");

    this.initNotifyHandler();
    const existing = this.subscriptionsByPattern.get(pattern);
    if (existing) {
      return this.addLocalSubscription(pattern, existing.subId, handler);
    }

    const subId = await this.subscribeWire(pattern);
    return this.addLocalSubscription(pattern, subId, handler);
  }

  private async subscribeWire(pattern: string): Promise<bigint> {
    const response = await this.requestFrame(
      MSG_SCHEDULE_SUBSCRIBE,
      ScheduleCodec.encodeSubscribe(pattern),
    );
    const decoded = ScheduleCodec.decodeSubscribeResponse(
      this.assertSuccess(response, "SUBSCRIBE"),
    );

    return decoded.subId;
  }

  private addLocalSubscription(
    pattern: string,
    subId: bigint,
    handler: ScheduleHandler,
  ): ScheduleSubscription {
    const handlerId = this.nextHandlerId++;
    let subscription = this.subscriptionsByPattern.get(pattern);
    if (!subscription) {
      subscription = { subId, handlers: new Map() };
      this.subscriptionsByPattern.set(pattern, subscription);
      this.patternsBySubId.set(subId, pattern);
    }

    subscription.handlers.set(handlerId, handler);
    return new ScheduleSubscription(subId, pattern, handler, async () => {
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
    const response = await this.requestFrame(
      MSG_SCHEDULE_UNSUBSCRIBE,
      ScheduleCodec.encodeUnsubscribe(pattern),
    );
    ScheduleCodec.decodeUnsubscribeResponse(this.assertSuccess(response, "UNSUBSCRIBE"));
  }

  private initNotifyHandler(): void {
    if (this.notifyHandlerInitialized) {
      return;
    }

    this.notifyHandlerInitialized = true;
    this.connection.registerNotificationHandler(MSG_SCHEDULE_NOTIFY, (payload) => {
      try {
        const decoded = ScheduleCodec.decodeNotification(payload);
        const pattern = this.patternsBySubId.get(decoded.subId);
        if (!pattern) {
          return;
        }

        const subscription = this.subscriptionsByPattern.get(pattern);
        if (!subscription) {
          return;
        }

        const notification: ScheduleNotification = {
          payload: decoded.payload,
        };
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
    if (normalized.includes("invalid route")) {
      return "INVALID_ROUTE";
    }
    if (normalized.includes("cron")) {
      return "INVALID_CRON";
    }
    return "REQUEST_FAILED";
  }
}
