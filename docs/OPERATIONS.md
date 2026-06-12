# fitz-ts Operations Guide

This guide covers the production-facing behavior of the fitz-ts client: connection lifecycle, reconnect expectations, observability hooks, and verification workflow.

## Connection Lifecycle

The client transitions through these states:

- `DISCONNECTED`
- `CONNECTING`
- `CONNECTED`
- `AUTHENTICATING`
- `AUTHENTICATED`
- `RECONNECTING`
- `CLOSED`

`connect()` opens the selected transport, sends `CONNECT`, and treats the connection as authenticated only after the auth settle window passes without the broker closing the socket.

On a live client, `connect()` is idempotent. Concurrent callers share the same
initial connect or reconnect lifecycle instead of creating replacement
connections behind the facade.

Caller-triggered abort during `connect({ signal })` fails the attempt without
marking the client as auth rejected. A later `connect()` attempt may be made on
the same client instance.

After the client has established at least one authenticated session, transport loss moves it back through `RECONNECTING` and then `AUTHENTICATING` unless `reconnect.enabled` is set to `false`. Reconnect listeners are replayed during the reconnect authentication flow before the client reports `AUTHENTICATED`, so notice, queue, lease, and stream subscriptions are restored and RPC workers are re-registered before the connection is considered fully ready again.

If application code calls `connect()` during that recovery window, the call
waits for the active reconnect path to finish. It does not start a second dial,
swap out cached domain clients, or replace the owned connection object.

The initial `connect()` call is still one-shot by default. If startup should keep waiting for Fitz to come back, add that outer loop in the application process manager or bootstrap code.

## Heartbeats

Idle subscription clients stay connected by default. `heartbeat.enabled` defaults to `true`, `heartbeat.intervalMs` defaults to `10000`, and `heartbeat.timeoutMs` defaults to `30000`.

The heartbeat loop only sends when the client has not seen application traffic within the current interval. That means a busy client stays quiet, while an idle subscribed client keeps its transport alive.

Transport behavior is capability-aware:

- Node WebSocket uses native ping/pong when available
- TCP enables socket keepalive and suppresses receive-idle disconnects while heartbeat is on
- browser WebSocket suppresses receive-idle disconnects and relies on close/error for liveness

## Wake Gates

`createWakeGate()` is the primitive for the safe pattern “notification wakes the loop, then the loop performs the authoritative read or claim.”

Use it to avoid lost wakes in subscription-driven consumers:

- queue availability wakes the worker, then `reserve()` claims work
- stream commit wakes the reader, then `read()` reads records
- schedule notifications can be consumed directly with `schedule.waitForNotifications()`

`reserveWhenAvailable()` defaults `batchSize` to `1`; `readWhenCommitted()` defaults `batchSize` to `100`.

Queue and stream callbacks are wake signals only. Do not treat the callback as the work handler unless the domain explicitly says the callback is the source of truth.

## Token Provider Expectations

- `tokenProvider` may be sync or async.
- It is invoked on the initial connect and again on each reconnect attempt.
- Return an empty string for anonymous brokers.
- If tokens can expire, prefer generating a fresh token on each call rather than caching one in the client.

## Observability Hooks

`ClientConfig.observability` is optional and additive. If omitted, the client behaves exactly as before.

Available hooks:

- `logger.log(level, event, fields)` for structured operational logs
- `tracer.startSpan(name, attributes)` for request-level tracing
- `meter.counter(...)`, `meter.histogram(...)`, and optional `meter.gauge(...)` for metrics
- `onLifecycleEvent(event)` for connection lifecycle notifications

Async handler controls:

- `asyncHandlers.maxConcurrency` limits how many notification or RPC worker handlers may run at once.
- `asyncHandlers.timeoutMs` bounds how long one handler may run before the client records a handler failure.
- Handler work is dispatched off the receive loop, so slow handlers no longer block frame intake, but they should still be kept small and idempotent.

Example:

```typescript
import { Client, type FitzLogger, type FitzMeter, type FitzTracer } from "@cntryl/fitz";

const logger: FitzLogger = {
  log(level, event, fields) {
    console.log(JSON.stringify({ level, event, ...fields }));
  },
};

const tracer: FitzTracer = {
  startSpan(name, attributes) {
    return {
      setAttribute(key, value) {
        void key;
        void value;
      },
      recordException(error) {
        console.error(name, error, attributes);
      },
      end() {},
    };
  },
};

const meter: FitzMeter = {
  counter(name, value, attributes) {
    console.log("counter", name, value, attributes);
  },
  histogram(name, value, attributes) {
    console.log("histogram", name, value, attributes);
  },
  gauge(name, value, attributes) {
    console.log("gauge", name, value, attributes);
  },
};

const client = Client({
  url: "ws://localhost:4090/ws",
  reconnect: { enabled: true },
  observability: {
    logger,
    tracer,
    meter,
    onLifecycleEvent(event) {
      console.log("lifecycle", event);
    },
  },
});
```

Current emitted lifecycle events:

- `connect_start`
- `auth_start`
- `connect_succeeded`
- `connect_failed`
- `connection_lost`
- `reconnect_scheduled`
- `reconnect_start`
- `reconnect_succeeded`
- `reconnect_failed`
- `reconnect_exhausted`
- `closed`

## Retry Defaults

`ClientConfig.retry` is enabled by default:

- `maxAttempts: 3`
- `backoffMs: 100`
- `maxBackoffMs: 1000`

Automatic retries are deliberately narrow:

- idempotent reads are retried on transient transport, connection, timeout, and retryable typed domain failures
- `queue.enqueue()` is retried only after Fitz explicitly rejects the write with a known transient commit failure or queue backpressure response
- all other writes and session-bound operations wait for reconnect before their first send, but are not replayed after an ambiguous post-send failure

The retry classifier does not hide invalid routes, auth failures, not-found errors, invalid tokens, invalid fences, or stale transaction/session failures.

Current metric names:

- `fitz.connection.lifecycle`
- `fitz.request.retry`
- `fitz.request.retry_exhausted`
- `fitz.request.started`
- `fitz.request.failed`
- `fitz.request.timeout`
- `fitz.request.duration`
- `fitz.requests.in_flight`
- `fitz.response.received`
- `fitz.response.ignored`
- `fitz.response.dropped`

## Stateful Handle Discipline

- KV transactions, stream sessions, queue items, and leases are stateful handles. Use them sequentially.
- Parallel work is supported across different transactions, sessions, and domains.
- Avoid issuing concurrent operations against the same `KvTransaction`, `StreamSession`, `QueueItem`, or `Lease`; the client now serializes transport writes globally, but same-handle sequencing is still an application-level responsibility.

## Shutdown Expectations

- Always call `close()` during process shutdown.
- `close()` cancels in-flight requests, tears down the receive loop, and closes the active transport.
- `close()` is the only permanent terminal state for a `Client`; failed connect
  attempts and reconnect loss do not implicitly replace the owned connection.
- Domain clients are cached on the owning client and survive reconnect; recreate them only after building a new top-level `Client`.
- Stateful handles obtained before `close()` are no longer valid after shutdown or reconnect recovery. Treat post-close use as an application bug and reacquire fresh handles.
- Queue inflight reservations and lease ownership state are broker-session scoped. `QueueItem` and `Lease` handles from before a disconnect now fail fast instead of waiting for a dead session to recover.
- Pending RPC iterators fail promptly when the underlying connection closes or resets.

## Verification Workflow

Fast local checks:

```bash
npm ci
npm run verify:fast
```

Broker-backed checks:

```bash
docker compose up -d
npm run verify
docker compose down --volumes
```

Focused broker-backed connection checks:

```bash
npm run test:integration -- tests/integration/connection.test.ts
```

That suite verifies two operational guarantees across TCP and WebSocket, anonymous and JWT-authenticated brokers:

- notice subscriptions can be re-established after reconnect-oriented client recovery flows
- `tokenProvider` is invoked again when reconnecting to an auth-required broker

Release-oriented checklist:

```bash
npm ci
npm run verify:fast
docker compose up -d
npm run verify
npm run bench
npm run test:unit -- tests/unit/perf/hotpath-thresholds.test.ts
docker compose down --volumes
npm run pack:smoke
```

> For bench validation, run the full suite multiple times locally before formalizing any CI threshold gating. `tier4` is intended to capture broader integration-style encode/parse composition and may be noisier than lower-tier microbenchmarks.
