export async function restoreMapEntriesAtomically<TKey, TValue>(
  entries: Map<TKey, TValue>,
  restoreEntry: (key: TKey, value: TValue) => Promise<TValue>,
): Promise<void> {
  if (entries.size === 0) {
    return;
  }

  const snapshot = Array.from(entries.entries());
  const restored: Array<[TKey, TValue]> = [];

  for (const [key, value] of snapshot) {
    restored.push([key, await restoreEntry(key, value)]);
  }

  entries.clear();
  for (const [key, value] of restored) {
    entries.set(key, value);
  }
}
