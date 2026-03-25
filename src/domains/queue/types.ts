/**
 * Queue domain types
 * Per fitz-go/internal/domains/queue/queue.go
 */

import { Connection } from "../../client/connection";
import { QueueCodec } from "./codec";
import { QueueError } from "../../core/errors";
import { MSG_QUEUE_EXTEND, MSG_QUEUE_COMPLETE } from "../../frame/types";

/**
 * Queue item represents a reserved queue message.
 * It carries the route and token required for `extend()` and `complete()`.
 */
export class QueueItem {
  readonly body: Uint8Array;

  private id: bigint;
  private token: bigint;
  private readonly route: string;
  private readonly connection: Connection;

  constructor(
    id: bigint,
    token: bigint,
    body: Uint8Array,
    route: string,
    connection: Connection,
  ) {
    this.id = id;
    this.token = token;
    this.body = body;
    this.route = route;
    this.connection = connection;
  }

  /**
   * Extend the lease on this queue item.
   * @param leaseSecs Lease duration in seconds
   */
  async extend(leaseSecs: number): Promise<void> {
    const payload = QueueCodec.encodeExtend(
      this.route,
      this.id,
      this.token,
      leaseSecs,
    );
    const response = await this.connection.request(MSG_QUEUE_EXTEND, payload);
    const decoded = QueueCodec.decodeExtendResponse(response);

    if (decoded.status !== QueueStatus.Ok) {
      const statusName =
        QueueStatus[decoded.status] || `Unknown(${decoded.status})`;
      throw new QueueError(
        `EXTEND failed: ${statusName}`,
        statusName,
        decoded.status,
      );
    }
  }

  /**
   * Complete processing of this queue item and remove it from the queue.
   */
  async complete(): Promise<void> {
    const requestPayload = QueueCodec.encodeComplete(
      this.route,
      this.id,
      this.token,
    );
    const response = await this.connection.request(
      MSG_QUEUE_COMPLETE,
      requestPayload,
    );
    const decoded = QueueCodec.decodeCompleteResponse(response);

    if (decoded.status !== QueueStatus.Ok) {
      const statusName =
        QueueStatus[decoded.status] || `Unknown(${decoded.status})`;
      throw new QueueError(
        `COMPLETE failed: ${statusName}`,
        statusName,
        decoded.status,
      );
    }
  }

  testOnlyInvalidToken(): bigint {
    return this.token + 1n;
  }

  async testOnlyCompleteWithToken(token: bigint): Promise<void> {
    const requestPayload = QueueCodec.encodeComplete(
      this.route,
      this.id,
      token,
    );
    const response = await this.connection.request(
      MSG_QUEUE_COMPLETE,
      requestPayload,
    );
    const decoded = QueueCodec.decodeCompleteResponse(response);

    if (decoded.status !== QueueStatus.Ok) {
      const statusName =
        QueueStatus[decoded.status] || `Unknown(${decoded.status})`;
      throw new QueueError(
        `COMPLETE failed: ${statusName}`,
        statusName,
        decoded.status,
      );
    }
  }
}

/**
 * Availability notification from a queue.
 */
export interface AvailabilityNotification {
  route: string;
}

/**
 * Handler for availability notifications.
 */
export type AvailabilityHandler = (
  notification: AvailabilityNotification,
) => void | Promise<void>;

/**
 * Queue availability subscription.
 */
export class QueueSubscription {
  private readonly subId: bigint;
  private readonly pattern: string;
  private readonly unsubscribeFn: (subId: bigint) => Promise<void>;

  constructor(
    subId: bigint,
    pattern: string,
    unsubscribeFn: (subId: bigint) => Promise<void>,
  ) {
    this.subId = subId;
    this.pattern = pattern;
    this.unsubscribeFn = unsubscribeFn;
  }

  /**
   * Unsubscribe from availability notifications.
   */
  async unsubscribe(): Promise<void> {
    await this.unsubscribeFn(this.subId);
  }
}

/**
 * Queue operation status codes
 */
export enum QueueStatus {
  Ok = 0,
  QueueNotFound = 1,
  MessageNotFound = 2,
  InvalidToken = 3,
  QueueFull = 4,
  InvalidDelay = 5,
}

/**
 * Options for enqueue operations.
 */
export interface EnqueueOptions {
  priority?: number;
  delayMs?: number;
  ttlMs?: number;
}

/**
 * Internal codec response types.
 */
export interface QueueEnqueueResponse {
  status: number;
  messageId?: bigint;
}

export interface QueueReserveResponse {
  status: number;
  items?: Array<{
    id: bigint;
    token: bigint;
    body: Uint8Array;
  }>;
  cursor?: Uint8Array;
}

export interface QueueExtendResponse {
  status: number;
}

export interface QueueCompleteResponse {
  status: number;
}

export interface QueueSubscribeResponse {
  status: number;
  subId?: bigint;
}

export interface QueueUnsubscribeResponse {
  status: number;
}
