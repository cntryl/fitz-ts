# fitz-ts Public Contract

This document captures the stable behavior that release verification treats as
contractual.

## Lifecycle

- `Client#getState()` may report: `DISCONNECTED`, `CONNECTING`, `CONNECTED`,
  `AUTHENTICATING`, `AUTHENTICATED`, `RECONNECTING`, or `CLOSED`.
- `connect({ signal })` uses `AbortSignal` as the control plane for caller
  cancellation.
- Aborting `connect()` fails the attempt without treating the client as auth
  rejected.
- `close()` is idempotent and permanently transitions the client to `CLOSED`.

## Reconnect

- Automatic reconnect is enabled by default after the client has established at least one authenticated session. Set `reconnect.enabled` to `false` to disable it.
- The initial `connect()` attempt is single-shot unless the caller explicitly retries it.
- Reconnect replays notice, queue, lease, schedule, and stream subscriptions.
- Reconnect re-registers RPC workers before reporting `AUTHENTICATED`.
- In-flight request or iterator work from the pre-disconnect connection is
  failed; callers must reacquire fresh handles after reconnect.

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
