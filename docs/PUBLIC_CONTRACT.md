# fitz-ts Public Contract

This document captures the stable behavior that release verification treats as
contractual.

## Lifecycle

- `Client#getState()` may report: `DISCONNECTED`, `CONNECTING`, `CONNECTED`,
  `AUTHENTICATING`, `AUTHENTICATED`, `RECONNECTING`, or `CLOSED`.
- A live `Client` owns exactly one `Connection` instance until `close()`.
- `connect()` is idempotent on a live client. Concurrent callers coalesce onto
  the same connect or reconnect lifecycle instead of replacing the owned
  connection.
- `connect({ signal })` uses `AbortSignal` as the control plane for caller
  cancellation.
- Aborting `connect()` fails the attempt without treating the client as auth
  rejected.
- `close()` is idempotent and permanently transitions the client to `CLOSED`.

## Reconnect

- Automatic reconnect is enabled by default after the client has established at least one authenticated session. Set `reconnect.enabled` to `false` to disable it.
- The initial `connect()` attempt is single-shot unless the caller explicitly retries it.
- Calling `connect()` while the client is already connecting or reconnecting
  waits for that in-flight lifecycle. It does not create a second transport or
  replace cached domain clients.
- Reconnect replays notice, queue, lease, schedule, and stream subscriptions.
- Reconnect re-registers RPC workers before reporting `AUTHENTICATED`.
- In-flight request or iterator work from the pre-disconnect connection is
  failed; callers must reacquire fresh handles after reconnect.

## Heartbeats And Wake Gates

- Heartbeats are enabled by default after authentication settles. The default configuration is `intervalMs: 10000` and `timeoutMs: 30000`.
- Heartbeats noop when the client has seen application send or receive activity within the current interval.
- Transport behavior is capability-specific: Node WebSocket uses ping/pong, TCP enables socket keepalive, and browser WebSocket suppresses receive-idle disconnects rather than fabricating a protocol heartbeat.
- `createWakeGate()` is the client-side wake primitive used by subscription-driven helpers.
- Queue and stream wake helpers (`reserveWhenAvailable()` and `readWhenCommitted()`) treat subscription callbacks as wake signals only; the authoritative work step remains `reserve()` or `read()`.
- `schedule.waitForNotifications()` yields notifications directly because schedule has no separate SDK claim/read step.

## Automatic Retry

- `ClientConfig.retry` defaults to `enabled: true`, `maxAttempts: 3`,
  `backoffMs: 100`, and `maxBackoffMs: 1000`.
- The client automatically retries only:
  - idempotent reads: KV `get` / `scan`, Stream `read` / `readPage` / `peek` /
    `metadata`, Lease `query`
  - queue `enqueue()` after a broker-confirmed transient negative response
- The client does not automatically replay ambiguous post-send writes, RPC
  calls, queue reservations or acknowledgements, lease ownership changes, or
  notice publishes.

## Async Work

- RPC calls return async iterators; cancellation and disconnect surface while
  awaiting `next()`.
- Async handler fan-out is bounded by `asyncHandlers.maxConcurrency` and
  `asyncHandlers.timeoutMs`.
- Duplicate local subscriptions on the same pattern share one wire
  subscription and must not duplicate broker-side registration.
- `QueueItem`, `Lease`, `KvTransaction`, and `StreamSession` handles from the
  pre-disconnect session are stale after reconnect and fail fast.

## Packaging

- The published package must be installable from `npm pack`.
- ESM import, CommonJS require, and TypeScript declarations are all release
  gated.
