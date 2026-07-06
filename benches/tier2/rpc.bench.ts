import { describe } from "vitest";
import { RpcCodec } from "../../src/domains/rpc/codec";
import { SYNC_CODEC_BATCH_SIZE, benchBatch } from "../_bench";
import { buildCorrelationIds, cycleFixture, payloads, routes } from "../_shared";

const body = payloads.rpc;
const correlationIds = buildCorrelationIds(SYNC_CODEC_BATCH_SIZE);
const correlationId = correlationIds[0];

const responseFrame = RpcCodec.encodeResponse(correlationId, 1n, body, true);
const requestFrame = RpcCodec.encodeRequest(correlationId, routes.rpc, body);

describe("fitz-ts rpc benchmarks", () => {
  benchBatch("rpc encode request", SYNC_CODEC_BATCH_SIZE, (index) => {
    return RpcCodec.encodeRequest(cycleFixture(correlationIds, index), routes.rpc, body);
  });

  benchBatch("rpc encode response", SYNC_CODEC_BATCH_SIZE, () => {
    return RpcCodec.encodeResponse(correlationId, 1n, body, false);
  });

  benchBatch("rpc decode response", SYNC_CODEC_BATCH_SIZE, () => {
    return RpcCodec.decodeResponse(responseFrame);
  });

  benchBatch("rpc decode inbound request", SYNC_CODEC_BATCH_SIZE, () => {
    return RpcCodec.decodeInboundRequest(requestFrame);
  });

  benchBatch("rpc subscribe worker encode", SYNC_CODEC_BATCH_SIZE, () => {
    return RpcCodec.encodeSubscribeWorker(routes.rpc, 1);
  });

  benchBatch("rpc correlation id generation", SYNC_CODEC_BATCH_SIZE, () => {
    return RpcCodec.generateCorrelationId();
  });
});
