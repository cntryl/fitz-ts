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

Caller-triggered abort during `connect({ signal })` fails the attempt without
marking the client as auth rejected. A later `connect()` attempt may be made on
the same client instance.

If reconnect is enabled, transport loss moves the client back through `RECONNECTING` and then `AUTHENTICATING`. Reconnect listeners are replayed during the reconnect authentication flow before the client reports `AUTHENTICATED`, so subscriptions and workers are restored before the connection is considered fully ready again.

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

const client = new Client({
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

Current metric names:

- `fitz.connection.lifecycle`
- `fitz.request.started`
- `fitz.request.failed`
- `fitz.request.timeout`
- `fitz.request.duration`
- `fitz.requests.in_flight`
- `fitz.response.received`
- `fitz.response.ignored`
- `fitz.response.dropped`

## Stateful Handle Discipline

- KV transactions and stream sessions are stateful handles. Use them sequentially.
- Parallel work is supported across different transactions, sessions, and domains.
- Avoid issuing concurrent operations against the same `KvTransaction` or `StreamSession`; the client now serializes transport writes globally, but same-handle sequencing is still an application-level responsibility.

## Shutdown Expectations

- Always call `close()` during process shutdown.
- `close()` cancels in-flight requests, tears down the receive loop, and closes the active transport.
- Domain clients are connection-scoped; recreate them after building a new top-level `Client`.
- Stateful handles obtained before `close()` are no longer valid after shutdown or reconnect recovery. Treat post-close use as an application bug and reacquire fresh handles.
- Pending RPC iterators fail promptly when the underlying connection closes or resets.

## Verification Workflow

Fast local checks:

```bash
npm ci
npm run verify:fast
```

Broker-backed checks:

```bash
docker compose -f ../fitz-go/compose.yml up -d
npm run verify
docker compose -f ../fitz-go/compose.yml down --volumes
```

Focused broker-backed connection checks:

```bash
npm run test:integration -- --run tests/integration/connection.test.ts
```

That suite verifies two operational guarantees across TCP and WebSocket, anonymous and JWT-authenticated brokers:

- notice subscriptions can be re-established after reconnect-oriented client recovery flows
- `tokenProvider` is invoked again when reconnecting to an auth-required broker

Release-oriented checklist:

```bash
npm ci
npm run verify:fast
docker compose -f ../fitz-go/compose.yml up -d
npm run verify
npm run bench -- --run tests/bench/hotpath.bench.ts
docker compose -f ../fitz-go/compose.yml down --volumes
npm run pack:smoke
```
