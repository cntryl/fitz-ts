import { bench, describe } from "vitest";
import { RpcCodec } from "../../src/domains/rpc/codec";
import { encoder, routes } from "../_shared";

const body = encoder.encode("rpc-payload");
const correlationId = RpcCodec.generateCorrelationId();

const responseFrame = RpcCodec.encodeResponse(correlationId, 1n, body, true);
const requestFrame = RpcCodec.encodeRequest(correlationId, routes.rpc, routes.reply, body);

describe("fitz-ts rpc benchmarks", () => {
  bench("rpc encode request", () => {
    RpcCodec.encodeRequest(RpcCodec.generateCorrelationId(), routes.rpc, routes.reply, body);
  });

  bench("rpc encode response", () => {
    RpcCodec.encodeResponse(correlationId, 1n, body, false);
  });

  bench("rpc decode response", () => {
    RpcCodec.decodeResponse(responseFrame);
  });

  bench("rpc decode inbound request", () => {
    RpcCodec.decodeInboundRequest(requestFrame);
  });

  bench("rpc subscribe worker encode", () => {
    RpcCodec.encodeSubscribeWorker(routes.rpc);
  });
});
