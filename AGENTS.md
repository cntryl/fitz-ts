# fitz-ts Agent Notes

## Toolchain

This repo uses direct tools instead of a wrapper toolchain:

- `tsc` for typechecking and declaration emit
- `vitest` for unit, integration, conformance, and benchmark runs
- `rolldown` for JS bundle output
- `oxlint` for linting
- `oxfmt` for formatting

Use the `package.json` scripts as the source of truth.

## Core Commands

- `npm run verify:fast`
- `npm run verify`
- `npm run build`
- `npm run test:unit`
- `npm run test:integration`
- `npm run test:conformance`
- `npm run bench`
- `npm run lint`
- `npm run fmt:check`

## Review Checklist

- Run `npm ci` after dependency changes.
- Run `npm run verify:fast` for local validation.
- Run `npm run verify` before release-facing changes are considered done.
- Keep [`scripts/pack-smoke.js`](/D:/repos/cntryl/fitz-workspace/fitz-ts/scripts/pack-smoke.js) green when changing build or package metadata.
