import { bench, describe } from "vitest";
import { QueueCodec } from "../../src/domains/queue/codec";
import { encoder, routes } from "../_shared";

const body = encoder.encode("queue-payload");

describe("fitz-ts queue benchmarks", () => {
  bench("queue enqueue encode", () => {
    QueueCodec.encodeEnqueue(routes.queue, body, { delayMs: 1500 });
  });

  bench("queue reserve encode", () => {
    QueueCodec.encodeReserve(routes.queue, 60, 10);
  });

  bench("queue complete encode", () => {
    QueueCodec.encodeComplete(routes.queue, 123n, 456n);
  });

  bench("queue extend encode", () => {
    QueueCodec.encodeExtend(routes.queue, 123n, 456n, 30);
  });

  bench("queue subscribe encode", () => {
    QueueCodec.encodeSubscribe("queue://bench/**");
  });
});
