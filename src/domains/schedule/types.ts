/**
 * Schedule domain types
 * Per fitz-go/internal/domains/schedule (cron-based task scheduling)
 */

import {
  ErrCodeScheduleInvalidDeliveryMode,
  ErrCodeScheduleInvalidSubscription,
  ErrCodeScheduleSubscriptionLimit,
} from "../../core/errors";

/**
 * ScheduleEntry represents a schedule returned by a list page
 * Per CLIENT_SPEC: route, cron, payload
 */
export interface ScheduleEntry {
  id: string; // Route as identity
  route: string;
  cron: string;
  deliveryMode: ScheduleDeliveryMode;
  payload: Uint8Array;
}

export type ScheduleDeliveryMode = "Broadcast" | "Single";

/**
 * Notification is the payload delivered when a schedule fires (SCHEDULE_NOTIFY 705)
 */
export interface ScheduleNotification {
  route: string;
  payload: Uint8Array;
}

export interface DecodedScheduleNotification {
  subId: bigint;
  route: string;
  payload: Uint8Array;
}

/**
 * ScheduleHandler is called when a schedule fires for a subscribed pattern
 * It is fire-and-forget; the return value is not used
 */
export type ScheduleHandler = (notification: ScheduleNotification) => void | Promise<void>;

/**
 * ScheduleSubscription represents an active subscription to schedule fire notifications
 */
export type ScheduleSubscription = ReturnType<typeof createScheduleSubscription>;

export function createScheduleSubscription(
  getSubId: () => bigint,
  pattern: string,
  unsubscribeFn: () => Promise<void>,
) {
  const unsubscribe = async (): Promise<void> => {
    return unsubscribeFn();
  };

  return {
    get subId(): bigint {
      return getSubId();
    },
    pattern,
    unsubscribe,
  };
}

export interface ScheduleCreateResponse {
  scheduleId?: string;
}

export type ScheduleCancelResponse = Record<string, never>;

export interface ScheduleListPage {
  entries: ScheduleEntry[];
  totalCount: bigint;
}

export interface ScheduleSubscribeResponse {
  subId: bigint;
}

export type ScheduleUnsubscribeResponse = Record<string, never>;

/**
 * Schedule operation status codes
 */
export enum ScheduleStatus {
  Ok = 0,
  ScheduleNotFound = 1,
  TaskNotFound = 2,
  InvalidCron = 3,
  InvalidDelay = 4,
  InvalidTimestamp = 5,
}

export const ScheduleStatusNames: Record<number, string> = {
  [ScheduleStatus.ScheduleNotFound]: "NOT_FOUND",
  [ScheduleStatus.TaskNotFound]: "NOT_FOUND",
  [ScheduleStatus.InvalidCron]: "INVALID_CRON",
  [ScheduleStatus.InvalidDelay]: "INVALID_DELAY",
  [ScheduleStatus.InvalidTimestamp]: "INVALID_TIMESTAMP",
  // Broker domain error codes live in a separate numeric namespace from the
  // ScheduleStatus wire enum above (no overlap with 0-5), but standard
  // responses report both through the same errorCode field — merge them
  // into one lookup table so a subscribe()/subscribeIterator() failure
  // reported this way resolves to its real symbolic name instead of
  // falling through to a generic Unknown(N).
  [ErrCodeScheduleInvalidSubscription]: "INVALID_SUBSCRIPTION",
  [ErrCodeScheduleSubscriptionLimit]: "SUBSCRIPTION_LIMIT",
  [ErrCodeScheduleInvalidDeliveryMode]: "INVALID_DELIVERY_MODE",
};
