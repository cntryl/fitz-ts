declare global {
  interface SymbolConstructor {
    readonly asyncDispose: unique symbol;
  }
}

export {};
