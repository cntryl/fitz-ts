# Fitz TypeScript World-Class TODO

You are working in fitz-ts. The SDK is already close to the target bar, but the grading report still has partial and unassessed areas. Close those gaps without redesigning the public API or reintroducing client-side protocol invention.

## Canonical Sources

- [../fitz/docs/clients/client-spec.md](../fitz/docs/clients/client-spec.md)
- [../fitz/docs/clients/client-acceptance-criteria.md](../fitz/docs/clients/client-acceptance-criteria.md)
- [../fitz/docs/clients/client-implementation-guide.md](../fitz/docs/clients/client-implementation-guide.md)
- [../fitz/docs/clients/connection-flow.md](../fitz/docs/clients/connection-flow.md)
- [docs/GRADING.md](docs/GRADING.md)
- [README.md](README.md)
- [docs/OPERATIONS.md](docs/OPERATIONS.md)
- [docs/PUBLIC_CONTRACT.md](docs/PUBLIC_CONTRACT.md)
- [PERF_RESULTS.md](PERF_RESULTS.md)
- [tests/conformance/conformance.test.ts](tests/conformance/conformance.test.ts)

## What Is Still Missing

- The grading report still has partials in protocol, connection, error, observability, and performance auditing.
- Some conformance and lifecycle paths still accept partial results because the harness cannot yet prove the strongest behavior under every transport and auth combination.
- The repo has benchmark evidence, but the performance thresholds and a few release-oriented claims are not yet formalized.
- A few convenience overloads and full error/log/trace matrices still need audit-level proof.

## Work In Order

1. Finish the remaining protocol and transport audit.
   - Close the TLV field-layout audit.
   - Verify browser and Node transport edge cases and keep websocket/tcp behavior identical where the contract says they should be.
2. Close the remaining lifecycle and concurrency audits.
   - Prove connect/auth settle, reconnect, shutdown, and cancel behavior across both transports.
   - Finish the same-handle sequencing, disconnect cleanup, and abort propagation audits.
3. Finish the API, error, and observability matrix.
   - Audit the remaining convenience overloads.
   - Close the outstanding canonical error-code mapping and retryability coverage.
   - Complete the structured logging, tracing attribute, and metrics coverage proofs.
4. Formalize performance and release evidence.
   - Replace "benchmark exists" with named thresholds and reproducible targets.
   - Keep `PERF_RESULTS.md` authoritative and update it after deliberate benchmark changes only.
5. Keep the conformance and CI story complete.
   - Preserve the 15-scenario conformance harness and the 2x2 transport/auth matrix.
   - Make sure the repo docs, artifacts, and workflow all describe the same release bar.

## Concrete Gap Checklist

- `docs/GRADING.md`: close the remaining partials and remaining `UNASSESSED` rows that matter to the SDK contract.
- `src/client/connection.ts`: finish the lifecycle, reconnect, shutdown, and cancellation proofs.
- `src/core/errors.ts` and `src/client/multiplexer.ts`: finish the remaining error and correlation audits.
- `src/core/types.ts` and `src/client/connection.ts`: finish the observability and async handler matrix.
- `PERF_RESULTS.md` and `tests/bench/hotpath.bench.ts`: formalize the performance bar instead of leaving it as evidence-only.

## Definition Of Done

- `docs/GRADING.md` has no unresolved partials or unassessed rows that matter to the SDK contract.
- Conformance, integration, and benchmark documentation all match the shipped behavior.
- No route parsing or normalization is introduced at the client layer.
- The repo can be defended as world-class against the same contract used by the other Fitz SDKs.

## Constraints

- Keep routes opaque; do not reintroduce client-side route validation or normalization.
- Do not widen the public API unless the Fitz contract requires it.
- Prefer additive changes and targeted tests over broad rewrites.
- Treat the grading doc as a live contract, not an after-the-fact status note.