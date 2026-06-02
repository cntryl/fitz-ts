# fitz-ts Agent Notes

## Toolchain

This repo uses Vite+ and npm script aliases. Use the package.json scripts as the source of truth.

- `vp check` for combined format, lint, and type checks
- `vp fmt` for formatting
- `vp lint` for linting
- `vp test` for unit, integration, and conformance tests
- `vp pack` for JS bundle output
- `tsc` for declaration emit

Use the `package.json` scripts as the source of truth.

## Style Preference

- Prefer factory functions and plain object APIs instead of class constructors for runtime objects.
- Implement stateful abstractions with closure-backed factories.
- Preserve `Error` subclasses as classes when native exception semantics are desired.

## Core Commands

- `npm run verify:fast`
- `npm run verify`
- `npm run build`
- `npm run test`
- `npm run test:unit`
- `npm run test:integration`
- `npm run test:conformance`
- `npm run pack:smoke`
- `npm run bench`
- `npm run lint`
- `npm run fmt`
- `npm run fmt:check`

## Review Checklist

- Run `npm ci` after dependency changes.
- Run `npm run verify:fast` for local validation.
- Run `npm run verify` before release-facing changes are considered done.
