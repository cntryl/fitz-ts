declare global {
  interface SymbolConstructor {
    readonly asyncDispose: unique symbol;
  }
}

// The block above only teaches TypeScript that `Symbol.asyncDispose` exists;
// it does nothing at runtime. `[Symbol.asyncDispose]` is used as a real
// object key elsewhere in this package (kv transactions, stream sessions) to
// back `await using`, so on any engine without native Explicit Resource
// Management, `Symbol.asyncDispose` would evaluate to `undefined` and those
// disposal methods would silently fail to register under the well-known
// symbol. Polyfill it defensively; a native implementation is left alone.
if (typeof Symbol.asyncDispose !== "symbol") {
  Object.defineProperty(Symbol, "asyncDispose", {
    value: Symbol("Symbol.asyncDispose"),
    writable: false,
    configurable: false,
    enumerable: false,
  });
}

export {};
