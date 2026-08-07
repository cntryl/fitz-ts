/**
 * Shared helpers for the subscribe()/unsubscribe() lifecycle every
 * pattern/route-subscribing domain (KV, Lease, Notice, Queue, Schedule,
 * Stream) repeats with the same shape:
 *
 *   const state = subscriptionsByPattern.get(pattern);
 *   state.handlers.delete(handlerId);
 *   if (state.handlers.size === 0) {
 *     await requestFrame(MSG_..._UNSUBSCRIBE, ...);
 *     subscriptionsByPattern.delete(pattern);
 *     patternsBySubId.delete(state.subId);
 *   }
 *
 * and:
 *
 *   createXSubscription(() => subscriptionsByPattern.get(pattern)?.subId ?? state.subId, ...)
 */

/** A monotonically increasing counter, one per domain client instance. */
export function createGenerationCounter(): { next(): number } {
  let value = 0;
  return {
    next(): number {
      value += 1;
      return value;
    },
  };
}

/**
 * True only when `state` is still the live entry for `key` AND has no
 * remaining handlers — the exact precondition every domain's unsubscribe()
 * must re-check immediately before deleting `state` from its bookkeeping
 * maps, after an `await` for the wire UNSUBSCRIBE round-trip.
 *
 * Without this re-check, a concurrent subscribe() call that reused the same
 * (not-yet-deleted) state object while the unsubscribe was in flight gets
 * silently orphaned: its handler survives in `state.handlers`, but the
 * unconditional delete below removes it from every map anyway.
 */
export function isCurrentEmptyState<K, V extends { handlers: Map<unknown, unknown> }>(
  map: Map<K, V>,
  key: K,
  state: V,
): boolean {
  return map.get(key) === state && state.handlers.size === 0;
}

/**
 * Builds a `subId` getter that tracks a specific subscription handle's own
 * lineage rather than blindly re-resolving `pattern`/`route` against
 * whatever entry currently occupies that key.
 *
 * A plain `map.get(key) === state` identity check is not sufficient here:
 * reconnect (`restoreMapEntriesAtomically`) always rebuilds a brand-new
 * state object for a pattern/route even on the legitimate "this subscription
 * survived reconnect" path, so an identity check would misclassify every
 * post-reconnect subscription as stale and pin it to its pre-reconnect
 * subId forever. A generation number that reconnect carries forward (but a
 * genuinely new subscribe() bumps) distinguishes the two cases: same
 * generation, new subId -> the live update is real; different generation ->
 * this pattern/route was unsubscribed and re-subscribed independently, so
 * the originally-captured value is the correct (if now-stale) answer for
 * *this* handle.
 */
export function createLiveSubIdGetter<K>(
  map: Map<K, { subId: bigint; generation: number }>,
  key: K,
  capturedSubId: bigint,
  capturedGeneration: number,
): () => bigint {
  return () => {
    const current = map.get(key);
    return current && current.generation === capturedGeneration ? current.subId : capturedSubId;
  };
}

/**
 * Waits out a wire UNSUBSCRIBE currently in flight for `state`, if any.
 *
 * subscribe()'s "reuse the existing local state" fast path must not fire
 * while an unsubscribe for the same pattern/route is still awaiting its
 * broker round-trip. Reusing that about-to-be-invalidated state registers
 * the new handler locally without ever sending a fresh wire SUBSCRIBE — so
 * if the broker goes on to confirm the unsubscribe, the new handler is left
 * silently unregistered at the broker while still believed live locally
 * (notifications for it are then just never delivered).
 *
 * Callers should loop: after this resolves, re-read the map and decide
 * fresh. If the unsubscribe failed, the surviving state (still genuinely
 * live at the broker) is safe to reuse as normal. If it succeeded, the
 * state is gone and a real subscribe() call is required.
 */
export async function awaitPendingUnsubscribe(state: {
  pendingUnsubscribe?: Promise<unknown>;
}): Promise<void> {
  if (state.pendingUnsubscribe) {
    await state.pendingUnsubscribe.catch(() => undefined);
  }
}
