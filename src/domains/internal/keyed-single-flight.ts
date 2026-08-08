/** Coordinates concurrent registration attempts for the same logical key. */
export function createKeyedSingleFlight<K, V>() {
  const inFlight = new Map<K, Promise<V>>();

  return async (key: K, register: () => Promise<V>): Promise<V> => {
    const existing = inFlight.get(key);
    if (existing) return existing;

    const pending = register().finally(() => {
      if (inFlight.get(key) === pending) inFlight.delete(key);
    });
    inFlight.set(key, pending);
    return pending;
  };
}
