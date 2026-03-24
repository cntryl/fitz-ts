# fitz-ts Grading

Version: 0.1.0
Date: 2026-03-24
Rubric: ../fitz/docs/clients/client-requirements.md

## Status Legend
- PASS: requirement satisfied with linked evidence
- PARTIAL: partially satisfied, missing concrete items
- FAIL: implemented incorrectly or missing
- UNASSESSED: not yet audited with evidence

## Snapshot (initial baseline)
- T0: UNASSESSED
- T1: PARTIAL (initial sampled grading)
- T2: FAIL (initial sampled grading)
- World-class: NO
- Current sampled counts: PASS=3 PARTIAL=3 FAIL=11 UNASSESSED=90

## Requirement Scorecard
| Req ID | Tier | Status | Evidence | Notes |
|---|---|---|---|---|
| REQ-PROTO-001 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-002 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-003 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-004 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-005 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-006 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-007 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-008 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-009 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-010 | T0 | UNASSESSED | - | pending audit |
| REQ-PROTO-011 | T1 | UNASSESSED | - | pending audit |
| REQ-PROTO-012 | T1 | UNASSESSED | - | pending audit |
| REQ-PROTO-013 | T1 | UNASSESSED | - | pending audit |
| REQ-PROTO-014 | T1 | UNASSESSED | - | pending audit |
| REQ-PROTO-015 | T1 | UNASSESSED | - | pending audit |
| REQ-PROTO-016 | T1 | UNASSESSED | - | pending audit |
| REQ-PROTO-017 | T1 | UNASSESSED | - | pending audit |
| REQ-PROTO-018 | T1 | UNASSESSED | - | pending audit |
| REQ-API-001 | T0 | UNASSESSED | - | pending audit |
| REQ-API-002 | T0 | UNASSESSED | - | pending audit |
| REQ-API-003 | T0 | UNASSESSED | - | pending audit |
| REQ-API-004 | T0 | UNASSESSED | - | pending audit |
| REQ-API-005 | T0 | UNASSESSED | - | pending audit |
| REQ-API-006 | T0 | UNASSESSED | - | pending audit |
| REQ-API-007 | T0 | UNASSESSED | - | pending audit |
| REQ-API-008 | T1 | UNASSESSED | - | pending audit |
| REQ-API-009 | T1 | UNASSESSED | - | pending audit |
| REQ-API-010 | T1 | UNASSESSED | - | pending audit |
| REQ-ERGON-001 | T0 | UNASSESSED | - | pending audit |
| REQ-ERGON-002 | T0 | UNASSESSED | - | pending audit |
| REQ-ERGON-003 | T0 | UNASSESSED | - | pending audit |
| REQ-ERGON-004 | T1 | UNASSESSED | - | pending audit |
| REQ-ERGON-005 | T1 | UNASSESSED | - | pending audit |
| REQ-ERGON-006 | T1 | UNASSESSED | - | pending audit |
| REQ-ERGON-007 | T1 | UNASSESSED | - | pending audit |
| REQ-ERGON-008 | T1 | UNASSESSED | - | pending audit |
| REQ-ERGON-009 | T1 | UNASSESSED | - | pending audit |
| REQ-ERGON-010 | T1 | UNASSESSED | - | pending audit |
| REQ-ERGON-011 | T2 | UNASSESSED | - | pending audit |
| REQ-ERGON-012 | T2 | UNASSESSED | - | pending audit |
| REQ-ERGON-013 | T2 | UNASSESSED | - | pending audit |
| REQ-CONN-001 | T0 | UNASSESSED | - | pending audit |
| REQ-CONN-002 | T0 | UNASSESSED | - | pending audit |
| REQ-CONN-003 | T0 | UNASSESSED | - | pending audit |
| REQ-CONN-004 | T0 | UNASSESSED | - | pending audit |
| REQ-CONN-005 | T1 | UNASSESSED | - | pending audit |
| REQ-CONN-006 | T1 | UNASSESSED | - | pending audit |
| REQ-CONN-007 | T1 | UNASSESSED | - | pending audit |
| REQ-CONN-008 | T1 | UNASSESSED | - | pending audit |
| REQ-CONN-009 | T1 | UNASSESSED | - | pending audit |
| REQ-CONN-010 | T2 | UNASSESSED | - | pending audit |
| REQ-CONN-011 | T2 | UNASSESSED | - | pending audit |
| REQ-CONC-001 | T0 | UNASSESSED | - | pending audit |
| REQ-CONC-002 | T0 | UNASSESSED | - | pending audit |
| REQ-CONC-003 | T0 | UNASSESSED | - | pending audit |
| REQ-CONC-004 | T0 | UNASSESSED | - | pending audit |
| REQ-CONC-005 | T1 | UNASSESSED | - | pending audit |
| REQ-CONC-006 | T1 | UNASSESSED | - | pending audit |
| REQ-CONC-007 | T1 | UNASSESSED | - | pending audit |
| REQ-CONC-008 | T2 | UNASSESSED | - | pending audit |
| REQ-CONC-009 | T2 | UNASSESSED | - | pending audit |
| REQ-ERR-001 | T0 | UNASSESSED | - | pending audit |
| REQ-ERR-002 | T0 | UNASSESSED | - | pending audit |
| REQ-ERR-003 | T0 | UNASSESSED | - | pending audit |
| REQ-ERR-004 | T1 | UNASSESSED | - | pending audit |
| REQ-ERR-005 | T1 | UNASSESSED | - | pending audit |
| REQ-ERR-006 | T1 | UNASSESSED | - | pending audit |
| REQ-ERR-007 | T1 | UNASSESSED | - | pending audit |
| REQ-ERR-008 | T2 | UNASSESSED | - | pending audit |
| REQ-ERR-009 | T2 | UNASSESSED | - | pending audit |
| REQ-OBS-001 | T1 | FAIL | src search (`logger|slog`) in src had no logger implementation | no optional structured logger wiring found |
| REQ-OBS-002 | T1 | FAIL | src search (`logger|slog`) in src had no connection/reconnect logging hooks | required operational log events not implemented |
| REQ-OBS-003 | T1 | FAIL | src search found no structured operation-error logging fields | structured kv log field contract not implemented |
| REQ-OBS-004 | T2 | FAIL | src search (`trace|tracer|opentelemetry`) found no tracing integration | optional tracer spans absent |
| REQ-OBS-005 | T2 | FAIL | src search (`trace|tracer|opentelemetry`) found no span attributes integration | Fitz OTel attributes absent |
| REQ-OBS-006 | T2 | FAIL | only getMetrics found in src/client/multiplexer.ts; no meter hooks found | required OTel metrics not implemented |
| REQ-OBS-007 | T2 | FAIL | no tracer/meter wiring present to gate opt-in behavior | zero-cost opt-in telemetry contract unimplemented |
| REQ-PERF-001 | T1 | UNASSESSED | - | pending audit |
| REQ-PERF-002 | T1 | UNASSESSED | - | pending audit |
| REQ-PERF-003 | T1 | UNASSESSED | - | pending audit |
| REQ-PERF-004 | T2 | FAIL | no PERF_RESULTS.md with p99 loopback latency evidence | benchmark target evidence missing |
| REQ-PERF-005 | T2 | FAIL | no frame encode microbench evidence present | frame encode latency unmeasured |
| REQ-PERF-006 | T2 | FAIL | no RPC correlation lookup benchmark at 1k+ in-flight | latency target unverified |
| REQ-PERF-007 | T2 | FAIL | no notice publish throughput benchmark evidence | throughput target unverified |
| REQ-PERF-008 | T2 | FAIL | package.json has bench script but no benchmark files found in repo | required hot-path benchmark suite absent |
| REQ-TEST-001 | T0 | UNASSESSED | - | pending audit |
| REQ-TEST-002 | T0 | UNASSESSED | - | pending audit |
| REQ-TEST-003 | T0 | UNASSESSED | - | pending audit |
| REQ-TEST-004 | T1 | PASS | tests/integration/*.test.ts cover KV, Queue, Notice, RPC, Lease, Stream, Schedule happy-path lifecycle; npm run test:integration passed | domain integration lifecycle coverage present and green |
| REQ-TEST-005 | T1 | PASS | tests/integration/fixture/transport.ts runWithBothTransports iterates tcp+ws and anonymous+valid_jwt; suites import and use helper | integration suite runs across both transports |
| REQ-TEST-006 | T1 | PARTIAL | vitest.integration.config.ts enforces testTimeout/hookTimeout; fixture.ts sets client timeout default | suite has deadlines, but per-operation explicit request signal timeouts are not uniformly applied |
| REQ-TEST-007 | T1 | PASS | tests/integration/fixture/fixture.ts uniqueRoute() helper and integration tests derive routes from it | route uniqueness guardrails are implemented |
| REQ-TEST-008 | T1 | UNASSESSED | - | pending audit |
| REQ-TEST-009 | T1 | UNASSESSED | - | pending audit |
| REQ-TEST-010 | T1 | PARTIAL | .github/workflows/ci.yml matrix covers ws/tcp x anonymous/valid_jwt; local conformance-results.json captures ws+anonymous only | need fresh artifacts proving 100% P0 for all 4 CI combinations |
| REQ-TEST-011 | T2 | UNASSESSED | - | pending audit |
| REQ-TEST-012 | T2 | PARTIAL | conformance-results.json shows P1 100% for ws+anonymous run | need matrix-wide evidence for all 4 combinations |
| REQ-TEST-013 | T2 | FAIL | package.json contains "bench" script, but repo has no benchmark source files | benchmark suite requirement not met |
| REQ-TEST-014 | T2 | UNASSESSED | - | pending audit |
| REQ-DOCS-001 | T1 | UNASSESSED | - | pending audit |
| REQ-DOCS-002 | T1 | UNASSESSED | - | pending audit |
| REQ-DOCS-003 | T1 | UNASSESSED | - | pending audit |
| REQ-DOCS-004 | T1 | UNASSESSED | - | pending audit |
| REQ-DOCS-005 | T2 | UNASSESSED | - | pending audit |
| REQ-DOCS-006 | T2 | UNASSESSED | - | pending audit |
| REQ-DOCS-007 | T2 | UNASSESSED | - | pending audit |
| REQ-DOCS-008 | T2 | FAIL | repository search found no CHANGELOG.md | release change tracking artifact missing |
