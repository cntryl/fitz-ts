# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and the project follows Semantic Versioning.

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
