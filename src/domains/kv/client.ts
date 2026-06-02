/**
 * KV domain client.
 */

import { createDomainClient } from "../base";
import { KvCodec } from "./codec";
import { createKvTransaction, KvTransaction } from "./transaction";
import { KvBeginOptions, KvStatus } from "./types";
import { MSG_KV_BEGIN } from "../../frame/types";
import { KvError } from "../../core/errors";
import { isRouteShape } from "../_routes";
import type { Connection } from "../../client/connection";

export type KvClient = ReturnType<typeof createKvClient>;

export function createKvClient(connection: Connection) {
  const { requestFrame } = createDomainClient(connection);

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

  return {
    begin,
  };
}

type KvClientConstructor = {
  new (connection: Connection): KvClient;
  (connection: Connection): KvClient;
};

export const KvClient: KvClientConstructor = function (connection: Connection) {
  return createKvClient(connection);
} as unknown as KvClientConstructor;

export type { KvTransaction } from "./transaction";
export * from "./types";
