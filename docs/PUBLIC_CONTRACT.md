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

- Automatic reconnect only occurs when `reconnect.enabled` is true.
- Reconnect replays notice, queue, lease, schedule, and stream subscriptions.
- Reconnect re-registers RPC workers before reporting `AUTHENTICATED`.
- In-flight request or iterator work from the pre-disconnect connection is
  failed; callers must reacquire fresh handles after reconnect.

## Async Work

- RPC calls return async iterators; cancellation and disconnect surface while
  awaiting `next()`.
- Async handler fan-out is bounded by `asyncHandlers.maxConcurrency` and
  `asyncHandlers.timeoutMs`.
- Duplicate local subscriptions on the same pattern share one wire
  subscription and must not duplicate broker-side registration.

## Packaging

- The published package must be installable from `npm pack`.
- ESM import, CommonJS require, and TypeScript declarations are all release
  gated.
