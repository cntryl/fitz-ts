/**
 * KV domain client.
 */

import { DomainClient } from "../base";
import { KvCodec } from "./codec";
import { KvTransaction } from "./transaction";
import { KvBeginOptions, KvStatus } from "./types";
import { MSG_KV_BEGIN } from "../../frame/types";
import { KvError } from "../../core/errors";
import { isRouteShape } from "../_routes";

export class KvClient extends DomainClient {
  async begin(route: string, options: KvBeginOptions): Promise<KvTransaction> {
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
    const response = await this.requestFrame(MSG_KV_BEGIN, payload);
    const decoded = KvCodec.decodeBeginResponse(response);

    if (decoded.status !== KvStatus.Ok || decoded.txId === undefined) {
      throw new KvError("BEGIN failed", "BEGIN_FAILED", decoded.status);
    }

    return new KvTransaction(this.connection, route, decoded.txId);
  }
}

export { KvTransaction } from "./transaction";
export * from "./types";
