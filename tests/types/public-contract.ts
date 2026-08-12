import type {
  KvClient,
  LeaseClient,
  NoticeClient,
  QueueClient,
  RpcClient,
  ScheduleClient,
  StreamClient,
} from "../../src/index.node";

declare const kv: KvClient;
declare const lease: LeaseClient;
declare const notice: NoticeClient;
declare const queue: QueueClient;
declare const rpc: RpcClient;
declare const schedule: ScheduleClient;
declare const stream: StreamClient;
declare const bytes: Uint8Array;

void kv.begin("kv://realm/area/resource", {
  durability: "Sync",
  signal: AbortSignal.timeout(1),
});
void queue.enqueue("queue://realm/area/resource", { body: bytes, delaySeconds: 2 });
void queue.reserve("queue://realm/area/resource", {
  leaseSeconds: 30,
  batchSize: 2,
  waitSeconds: 1,
});
const frames = rpc.call("rpc://realm/area/resource", { body: bytes });
void frames.next();
void lease.acquire("lease://realm/area/resource", { ttlSeconds: 30, waitSeconds: 1 });
void notice.publish("notice://realm/area/resource", { body: bytes });
void schedule.create("schedule://realm/area/resource/run", {
  cron: "* * * * *",
  deliveryMode: "Single",
});
void schedule.entries("schedule://realm/**", { pageSize: 10n });
void stream.read("stream://**", { fromOffset: 0n, mode: "replay" }).next();

// @ts-expect-error positional queue payload was removed
void queue.enqueue("queue://realm/area/resource", bytes);
// @ts-expect-error millisecond delay was removed
void queue.enqueue("queue://realm/area/resource", { body: bytes, delayMs: 2 });
// @ts-expect-error call no longer has a positional body
void rpc.call("rpc://realm/area/resource", bytes);
// @ts-expect-error acquire no longer has positional ttl
void lease.acquire("lease://realm/area/resource", 30);
// @ts-expect-error publish no longer has a positional body
void notice.publish("notice://realm/area/resource", bytes);
// @ts-expect-error eager schedule listing was removed
void schedule.listBySelector("schedule://realm/**");
// @ts-expect-error old Stream positional read was removed
void stream.read("stream://**", 0n, 100);
// @ts-expect-error raw Stream pages are private
void stream.readPage("stream://**", 0n, 100);
// @ts-expect-error subscription iterator alias was removed
void stream.subscribeIterator("stream://**");
