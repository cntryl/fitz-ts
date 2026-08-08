export async function restoreMapEntriesAtomically<TKey, TValue>(
  entries: Map<TKey, TValue>,
  restoreEntry: (key: TKey, value: TValue) => Promise<TValue>,
  rollbackEntry?: (key: TKey, restoredValue: TValue) => Promise<void>,
): Promise<void> {
  if (entries.size === 0) {
    return;
  }

  const snapshot = Array.from(entries.entries());
  const restored: Array<[TKey, TValue]> = [];

  try {
    for (const [key, value] of snapshot) {
      restored.push([key, await restoreEntry(key, value)]);
    }
  } catch (error) {
    if (rollbackEntry) {
      for (const [key, value] of restored.reverse()) {
        await rollbackEntry(key, value).catch(() => undefined);
      }
    }
    throw error;
  }

  entries.clear();
  for (const [key, value] of restored) {
    entries.set(key, value);
  }
}
