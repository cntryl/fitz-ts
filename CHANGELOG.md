# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and the project follows Semantic Versioning.

## [Unreleased]

### Changed

- Breaking: Queue reserve, Stream read, and Stream last responses now require a concrete route for every returned item. `QueueItem`, `StreamReadItem`, and `StreamRecord` expose that route, and Queue reserves plus Stream reads/peeks accept any whole-segment pattern capable of matching three segments.

## [0.0.14] - 2026-08-03

### Added

- Abort-aware async iterator subscription APIs for KV, Lease, Notice, and Schedule that unsubscribe when iteration ends.
- Async disposal for KV transactions and Stream sessions, with automatic rollback of open work.

### Changed

- Breaking: `ScheduleDeliveryMode` now uses `"Broadcast" | "Single"`. The wire values remain `0` and `1`; callers should replace lowercase mode literals with the PascalCase forms.
- Reconnect listener failures are reported through observability and isolated so one domain restore failure does not prevent the reconnected session from becoming usable.

### Fixed

- Reconnect established sessions after transport loss instead of inferring an authentication rejection before the next server frame.
- Retry replayable reads after local request-queue backpressure.
- Keep Notice subscription IDs synchronized after broker re-registration.

## [0.0.13] - 2026-08-01

### Changed

- Breaking: domain accessors are readonly lazy properties (`client.kv`, `client.queue`, `client.rpc`, `client.lease`, `client.notice`, `client.stream`, and `client.schedule`) instead of methods.
- Public client and domain declarations now use named interfaces rather than factory `ReturnType` aliases.
- Concurrent subscriptions for the same KV, Queue, Notice, Lease, Stream, or Schedule pattern now share one broker registration; failed registration attempts remain retryable and the final local handle owns wire unsubscription.

## [0.1.0] - 2026-03-25

### Added

- Optional observability hooks on `ClientConfig` for structured logging, tracing, metrics, and lifecycle notifications.
- Connection lifecycle events for connect, authenticate, reconnect, disconnect, and close flows.
- Request-level tracing and metric hooks in the multiplexer for started, failed, timeout, duration, in-flight, received, ignored, and dropped events.
- Error context support through `FitzError.getContext()` for richer operational debugging.
- Operations guide covering lifecycle, reconnect, observability, shutdown, and verification.

### Changed

- Request, connection, and authentication failures now carry structured context when raised from core client paths.

### Verification

- Added unit coverage for connection lifecycle observability and multiplexer tracing/metrics behavior.
