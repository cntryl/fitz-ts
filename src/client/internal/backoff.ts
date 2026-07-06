export function jitteredBackoffMs(baseDelayMs: number, maxBackoffMs: number): number {
  const normalizedBaseMs = Math.max(baseDelayMs, 0);
  const normalizedMaxMs = Math.max(maxBackoffMs, 0);
  const cappedBaseMs = Math.min(normalizedBaseMs, normalizedMaxMs);
  if (cappedBaseMs <= 0) {
    return 0;
  }

  const jitterMs = Math.floor(Math.random() * cappedBaseMs * 0.5);
  return Math.min(Math.max(cappedBaseMs + jitterMs, 1), normalizedMaxMs);
}
