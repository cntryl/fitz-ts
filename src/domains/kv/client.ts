/**
 * KV domain client.
 */

import { createDomainClient } from "../base";
import type {
  AsyncDispatchPort,
  DisconnectListenerPort,
  NotificationPort,
  ReconnectListenerPort,
  ReconnectRestoreRequestPort,
  RequestPort,
  RetryExecutionPort,
} from "../base";
import { KvCodec } from "./codec";
import { createKvTransaction, KvTransaction } from "./transaction";
import {
  createKvSubscription,
  KvBeginOptions,
  KvHandler,
  KvNotification,
  KvStatus,
  KvSubscription,
} from "./types";
import {
  MSG_KV_BEGIN,
  MSG_KV_NOTIFY,
  MSG_KV_SUBSCRIBE,
  MSG_KV_UNSUBSCRIBE,
} from "../../frame/types";
import { KvError } from "../../core/errors";
import { isRegistrationPatternShape, isRouteShape } from "../_routes";
import { parseStandardResponse } from "../../protocol/response";
import { createBufferReader } from "../../core/buffer";
import { restoreMapEntriesAtomically } from "../internal/restore";

type KvConnectionPort = RequestPort &
  DisconnectListenerPort &
  RetryExecutionPort &
  ReconnectListenerPort &
  NotificationPort &
  AsyncDispatchPort &
  Partial<ReconnectRestoreRequestPort>;

type KvSubscriptionState = { subId: bigint; handlers: Map<number, KvHandler> };

export type KvClient = ReturnType<typeof createKvClient>;

export function createKvClient(connection: KvConnectionPort) {
  const { requestFrame, requestReconnectFrame } = createDomainClient(connection);
  const subscriptionsByPattern = new Map<string, KvSubscriptionState>();
  const patternsBySubId = new Map<bigint, string>();
  let nextHandlerId = 1;
  let notifyHandlerInitialized = false;

  connection.onReconnect(async () => {
    await restoreMapEntriesAtomically(subscriptionsByPattern, async (pattern, state) => ({
      subId: await subscribeWire(pattern, requestReconnectFrame),
      handlers: new Map(state.handlers),
    }));
    patternsBySubId.clear();
    for (const [pattern, state] of subscriptionsByPattern) {
      patternsBySubId.set(state.subId, pattern);
    }
  });

  const begin = async (route: string, options: KvBeginOptions): Promise<KvTransaction> => {
    if (!isRouteShape(route, "kv", 3, { allowBareRoute: true })) {
      throw new KvError(
        `Invalid kv route: ${route} (expected kv://{realm}/{area}/{resource} or {realm}/{area}/{resource}, no empty segments or wildcards)`,
        "INVALID_ROUTE",
      );
    }
    if (!options?.durability) {
      throw new KvError("BEGIN requires explicit durability", "MISSING_DURABILITY");
    }

    const payload = KvCodec.encodeBegin(route, options.mode ?? "ReadWrite", options.durability);
    const response = await requestFrame(MSG_KV_BEGIN, payload);
    const decoded = KvCodec.decodeBeginResponse(response);

    if (decoded.status !== KvStatus.Ok || decoded.txId === undefined) {
      throw new KvError("BEGIN failed", "BEGIN_FAILED", decoded.status);
    }

    return createKvTransaction(connection, route, decoded.txId);
  };

  const subscribeWire = async (pattern: string, request = requestFrame): Promise<bigint> => {
    const parsed = parseStandardResponse(
      await request(MSG_KV_SUBSCRIBE, KvCodec.encodeSubscribe(pattern)),
    );
    if (!parsed.success) {
      throw new KvError(
        `KV SUBSCRIBE failed: ${parsed.error ?? "unknown error"}`,
        "SUBSCRIBE_FAILED",
        parsed.errorCode,
      );
    }
    const reader = createBufferReader(parsed.data);
    const subId = reader.readU64BE();
    if (!reader.isEOF())
      throw new KvError("KV SUBSCRIBE response has trailing bytes", "INVALID_RESPONSE");
    return subId;
  };

  const unsubscribe = async (pattern: string, handlerId: number): Promise<void> => {
    const state = subscriptionsByPattern.get(pattern);
    if (!state) return;
    state.handlers.delete(handlerId);
    if (state.handlers.size > 0) return;
    subscriptionsByPattern.delete(pattern);
    patternsBySubId.delete(state.subId);
    const parsed = parseStandardResponse(
      await requestFrame(MSG_KV_UNSUBSCRIBE, KvCodec.encodeUnsubscribe(pattern)),
    );
    if (!parsed.success) {
      throw new KvError(
        `KV UNSUBSCRIBE failed: ${parsed.error ?? "unknown error"}`,
        "UNSUBSCRIBE_FAILED",
        parsed.errorCode,
      );
    }
  };

  const initNotifyHandler = (): void => {
    if (notifyHandlerInitialized) return;
    notifyHandlerInitialized = true;
    connection.registerNotificationHandler(MSG_KV_NOTIFY, (payload) => {
      try {
        const decoded = KvCodec.decodeNotification(payload);
        const pattern = patternsBySubId.get(decoded.subId);
        const state = pattern === undefined ? undefined : subscriptionsByPattern.get(pattern);
        if (!state) return;
        const notification: KvNotification = {
          route: decoded.route,
          mutationCount: decoded.mutationCount,
        };
        for (const handler of state.handlers.values()) {
          connection.dispatchAsyncHandler(async () => handler(notification));
        }
      } catch {
        // Malformed notifications are dropped without disrupting the receive loop.
      }
    });
  };

  const subscribe = async (pattern: string, handler: KvHandler): Promise<KvSubscription> => {
    if (!isRegistrationPatternShape(pattern, "kv", 3)) {
      throw new KvError(
        `Invalid kv subscription pattern: ${pattern} (must match a three-segment KV route)`,
        "INVALID_ROUTE",
      );
    }
    initNotifyHandler();
    let state = subscriptionsByPattern.get(pattern);
    if (!state) {
      state = { subId: await subscribeWire(pattern), handlers: new Map() };
      subscriptionsByPattern.set(pattern, state);
      patternsBySubId.set(state.subId, pattern);
    }
    const handlerId = nextHandlerId++;
    state.handlers.set(handlerId, handler);
    return createKvSubscription(state.subId, pattern, async () => unsubscribe(pattern, handlerId));
  };

  return {
    begin,
    subscribe,
  };
}

export const KvClient = createKvClient;

export type { KvTransaction } from "./transaction";
export * from "./types";
