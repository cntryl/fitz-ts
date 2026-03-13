/**
 * KV domain client.
 */

import { DomainClient } from "../base";
import { KvCodec } from "./codec";
import { KvTransaction } from "./transaction";
import { KvBeginOptions, KvStatus, TxMode } from "./types";
import { MSG_KV_BEGIN } from "../../frame/types";
import { KvError } from "../../core/errors";

function isValidKvRoute(route: string): boolean {
  const match = /^kv:\/\/([^/]+)\/([^/]+)\/([^/]+)$/.exec(route);
  return match !== null;
}

export class KvClient extends DomainClient {
  async begin(
    route: string,
    mode: TxMode = "ReadWrite",
    options: KvBeginOptions = {},
  ): Promise<KvTransaction> {
    if (!isValidKvRoute(route)) {
      throw new KvError(`Invalid route: ${route}`, "INVALID_ROUTE");
    }

    const payload = KvCodec.encodeBegin(
      route,
      mode,
      options.durability ?? "Async",
    );
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
