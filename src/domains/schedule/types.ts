/**
 * Schedule domain types
 * Per fitz-go/internal/domains/schedule (cron-based task scheduling)
 */

import {
  ErrCodeScheduleBackendError,
  ErrCodeScheduleInvalidDeliveryMode,
  ErrCodeScheduleInvalidSubscription,
  ErrCodeScheduleSubscriptionLimit,
} from "../../core/errors";
import { createSubscriptionHandle } from "../internal/subscription-handle";

/**
 * ScheduleEntry represents a schedule returned by a list page
 * Per CLIENT_SPEC: route, cron, payload
 */
export interface ScheduleEntry {
  /** Concrete schedule route. */
  route: string;
  /** Broker-supported cron expression. */
  cron: string;
  /** Whether each firing reaches all subscribers or one selected subscriber. */
  deliveryMode: ScheduleDeliveryMode;
  /** Payload delivered at each firing. */
  payload: Uint8Array;
}

/** `Broadcast` delivers to every eligible subscriber; `Single` selects one consumer. */
export type ScheduleDeliveryMode = "Broadcast" | "Single";

/**
 * Notification is the payload delivered when a schedule fires (SCHEDULE_NOTIFY 705)
 */
export interface ScheduleNotification {
  /** Concrete schedule route that fired. */
  route: string;
  /** Payload configured when the schedule was created. */
  payload: Uint8Array;
}

export interface DecodedScheduleNotification {
  subId: bigint;
  route: string;
  payload: Uint8Array;
}

/**
 * Handles a schedule firing. Delivery acknowledgement does not wait for the
 * returned promise; failures are reported through background-error handling.
 * Use Queue-backed scheduled work when processing must be durable.
 */
export type ScheduleHandler = (notification: ScheduleNotification) => void | Promise<void>;

/**
 * ScheduleSubscription represents an active subscription to schedule fire notifications
 */
export interface ScheduleSubscription extends AsyncDisposable {
  /** Resolves after unsubscribe; rejects with `AsyncHandlerOverflowError` on local overflow. */
  readonly completion: Promise<void>;
  /** Stops this local consumer and eventually releases shared broker subscription state. */
  unsubscribe(): Promise<void>;
}

export function createScheduleSubscription(
  unsubscribeFn: () => Promise<void>,
  signal?: AbortSignal,
): ScheduleSubscription {
  return createSubscriptionHandle<ScheduleSubscription>(unsubscribeFn, signal);
}

export interface ScheduleCreateResponse {
  scheduleId?: string;
}

export type ScheduleCancelResponse = Record<string, never>;

/** Decoded schedule page metadata used by the public entries iterator. */
export interface ScheduleListPage {
  /** Schedules in this page. */
  entries: readonly ScheduleEntry[];
  /** Total schedules matching the selector at listing time. */
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
  /** Operation succeeded. */
  Ok = 0,
  /** Schedule route does not exist. */
  ScheduleNotFound = 1,
  /** Scheduled task no longer exists. */
  TaskNotFound = 2,
  /** Cron expression is invalid. */
  InvalidCron = 3,
  /** Delay is outside the supported range. */
  InvalidDelay = 4,
  /** Timestamp is invalid. */
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
  [ErrCodeScheduleBackendError]: "BACKEND_ERROR",
};
