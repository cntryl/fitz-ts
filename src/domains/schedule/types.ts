/**
 * Schedule domain types
 * Per fitz-go/internal/domains/schedule (cron-based task scheduling)
 */

/**
 * ScheduleEntry represents a schedule returned by List
 * Per CLIENT_SPEC: route, cron, payload
 */
export interface ScheduleEntry {
  id: string; // Route as identity
  route: string;
  cron: string;
  payload: Uint8Array;
}

/**
 * Notification is the payload delivered when a schedule fires (SCHEDULE_NOTIFY 705)
 */
export interface ScheduleNotification {
  payload: Uint8Array;
}

export interface DecodedScheduleNotification {
  subId: bigint;
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
  subId: bigint,
  pattern: string,
  unsubscribeFn: () => Promise<void>,
) {
  const unsubscribe = async (): Promise<void> => {
    return unsubscribeFn();
  };

  return {
    subId,
    pattern,
    unsubscribe,
  };
}

export interface ScheduleCreateResponse {
  scheduleId?: string;
}

export type ScheduleCancelResponse = Record<string, never>;

export interface ScheduleListResponse {
  totalCount: bigint;
  entries: ScheduleEntry[];
}

export interface ScheduleSubscribeResponse {
  subId: bigint;
}

export type ScheduleUnsubscribeResponse = Record<string, never>;

/**
 * Schedule domain errors
 */
export class ScheduleError extends Error {
  public readonly code?: string;

  constructor(message: string, code?: string) {
    super(message);
    this.name = "ScheduleError";
    this.code = code;
  }
}

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
