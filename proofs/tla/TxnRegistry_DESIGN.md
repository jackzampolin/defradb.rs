# TxnRegistry — stale-transaction cleanup race

Backlog item #4 (`proofs/README.md` Coverage map; `proofs/survey/db.md` candidate
`TxnRegistryCleanupRace`). Tool: TLA+ / TLC.

## Property

> The stale-transaction cleanup sweep never evicts a still-live transaction: only
> txns genuinely idle past `max_idle_age` are removed and rolled back; no active
> txn is lost to a concurrent sweep.

Formalized as `INV_NoLiveEvicted == ~removedLive` in `TxnRegistry.tla`, where
`removedLive` latches true the first time the sweep removes a txn whose **true**
idle gap `clock - lastSeen` was `<= MaxIdle` at the instant of removal.

## Source anchors (Rust — the mechanism being modeled)

All in `crates/db/src/`:

| Symbol in model | Rust code | Anchor |
|---|---|---|
| `cleanup_stale_transactions` (the sweep) | `DbTransactionRegistry::cleanup_stale_transactions` | `txn_registry.rs:266-348` |
| Phase 1 `Collect` (read-lock candidate collection) | `self.transactions.read()` + `filter(idle_for(now) > max_idle_age)` | `txn_registry.rs:275-285` |
| Phase 2 `ProcessCandidate` (write-locked re-check + remove) | `self.transactions.write()` + `Arc::ptr_eq && current.idle_for(now) > max_idle_age => guard.remove` | `txn_registry.rs:300-318` |
| `RemoveDecision` w/ `Recheck="WriteLocked"` | the `current.idle_for(Instant::now()) > max_idle_age` guard | `txn_registry.rs:311-312` |
| `Touch` (get / get_ctx refresh) | `get_ctx`: `read()` then `ctx.touch()` | `txn_registry.rs:192-199` |
| `Touch` (trait `get`) | `get`: `read()` then `ctx.touch()` | `txn_registry.rs:766-784` |
| `lastSeen` write | `DbTransactionContext::touch` → `*last_request_seen = Instant::now()` | `txn_context.rs:65-75` |
| `IdleFor` | `DbTransactionContext::idle_for` → `now.duration_since(last_request_seen())` | `txn_context.rs:86-88` |
| read/write-lock exclusion | `transactions: RwLock<HashMap<..>>` (`std::sync::RwLock`) | `txn_registry.rs:88` |
| `IsStale` threshold | `idle_for(now) > max_idle_age`; default `max_idle_age = 600s` | `txn_registry.rs:39`, `:282`, `:293`, `:312` |

The load-bearing invariant in the code is the comment at `txn_registry.rs:305-308`:
holding the registry **write** lock blocks new `get()`/`get_ctx()` touches (which take
the **read** lock) while the final idle re-check + remove runs. The model encodes that
RwLock exclusion (`NoReadLock`/`NoWriteLock` guards on `TouchAcquire` and
`ProcessCandidate`) and the write-locked re-check (`Recheck="WriteLocked"`), and proves
the race is closed.

## Go parity (ground truth from `origin/develop`)

**Go has no stale-transaction cleanup sweep at all.** HTTP explicit transactions are
stored in a `sync.Map` and removed only on explicit commit/discard:

- `http/handler_tx.go:29,37` — `txs.Store(tx.ID(), tx)` on create.
- `http/handler_tx.go:49,61` — `txs.Load` then `txs.Delete` on commit.
- `http/handler_tx.go:73` — `txs.LoadAndDelete` on discard.
- `http/middleware.go:60-75` — request lookup is `txs.Load(id)`; it refreshes **no**
  idle clock (there is none).

A repo-wide search of `origin/develop` for a txn reaper/idle-timeout/sweep returns
nothing. Consequence: in Go an orphaned HTTP txn handle leaks until process exit. The
Rust registry **adds** a periodic sweep (`start_stale_transaction_cleanup`,
`txn_registry.rs:357-400`) that Go lacks. So this slice models a **Rust-specific
hardening**, and the race it guards against does not exist in Go because Go never evicts.
The proof's value is confirming the Rust addition is correct (no live-txn loss), not Go
parity — Go is the trivial "never evict" case (which the model's GREEN run also covers:
with no `Collect`, `removedLive` stays false).

## Model

`TxnRegistry.tla`. Logical clock `clock`; per-txn true `lastSeen` (ground truth);
`present` map membership; `rlock`/`wlock` model `RwLock` read/write holders;
`candidates` is the phase-1 collected set. Actions: `Tick`, `TouchAcquire`/`TouchRelease`
(a get's read-locked refresh), `Collect` (phase 1), `ProcessCandidate` (phase 2 remove).

`Recheck` selects the mechanism:
- `"WriteLocked"` — real code: phase-2 removes only if `IsStale(t, clock)` re-evaluated
  under the write lock. **GREEN.**
- `"None"` — buggy naive sweep: phase-2 trusts the phase-1 verdict and removes
  unconditionally. **RED.**

### Independent oracle (so green is not vacuous)

`removedLive` is set from the **true** idle gap `~IsStale(t, clock)` at removal time —
derived from the actual touch history and clock, **not** from the sweep's own re-check
decision. Under `"None"` the sweep can remove a txn whose oracle idle gap is 0 (just
touched), and TLC catches it. Under `"WriteLocked"` removal requires `IsStale`, which is
the negation of the oracle's liveness predicate at the same instant, so a live txn can
never be removed. Non-vacuity is separately witnessed: a probe invariant
`\A t : present[t]` is **refuted** in the GREEN config (TLC reaches `present[t1]=FALSE`),
proving the GREEN run does exercise genuine eviction of truly-stale txns while still
upholding `INV_NoLiveEvicted`.

## Runs / verify

From `proofs/tla/`:

```bash
# GREEN — write-locked re-check holds: no live txn evicted (1589 states).
./tools/tlc -metadir states/txnreg_green \
  -config MC_TxnRegistry_Green.cfg MC_TxnRegistry_Green.tla

# RED — naive sweep evicts a txn touched between collect and remove (counterexample).
./tools/tlc -metadir states/txnreg_red \
  -config MC_TxnRegistry_Red_NaiveSweep.cfg MC_TxnRegistry_Red_NaiveSweep.tla
```

| Config | Recheck | INV_NoLiveEvicted | Verdict |
|---|---|---|---|
| `MC_TxnRegistry_Green` | `WriteLocked` | holds (1589 distinct states) | GREEN ✓ |
| `MC_TxnRegistry_Red_NaiveSweep` | `None` | violated (live t1 evicted) | RED ✓ |

### RED counterexample (depth 7)

1. clock advances to 2; `lastSeen = [t1↦0, t2↦0]` (both idle 2 > MaxIdle 1).
2. `TouchAcquire(t1)` — a get takes the read lock on t1.
3. `Collect` — sweep snapshots both as stale candidates (read lock shared with the touch).
4. `TouchRelease(t1)` — the get refreshes `lastSeen[t1] = 2` (t1 now idle 0, live).
5. `ProcessCandidate(t1)` with `Recheck="None"` — removes t1 unconditionally;
   `~IsStale(t1, 2)` is true (idle 0 ≤ 1) so the oracle fires: `removedLive = TRUE`.

The real code's write-locked re-check (step 5 under `"WriteLocked"`) re-reads
`idle_for(now) = 0 ≤ MaxIdle`, declines the remove, and the invariant holds.

## Boundaries / honest reach

- **Bounded:** 2 txns, `MaxIdle=1`, `MaxTime=4`. This is the minimal witnessing shape
  (one toucher + one sweep over two txns). The property is structural (does the remove
  gate on a write-locked re-measurement of the same idle clock the toucher updates), not
  quantity-sensitive.
- **Atomicity abstraction:** `TouchAcquire`/`TouchRelease` and `ProcessCandidate` model
  the read- and write-locked critical sections; `Tick` is disallowed while a lock is held,
  matching that the real `idle_for`/`touch`/re-check each read `Instant::now()` once inside
  a held lock. The model does **not** capture sub-instant time skew within a single locked
  section (the real code calls `Instant::now()` twice in phase 2, `:292` and `:312`; both
  are after the candidate's own action lock is held, so no touch can intervene — the model
  collapses them to one `clock` read, which is the conservative, faithful abstraction).
- **`Arc::ptr_eq` guard (`:311`):** modeled implicitly — `present[t]` membership plus
  single-sweep `candidates` means a removed-then-reinserted id cannot be confused; the
  ptr_eq check guards the (here unmodeled) id-reuse case. The id counter is monotonic
  (`txn_registry.rs:714`), so id reuse does not occur in practice; ptr_eq is belt-and-
  suspenders and out of this slice's scope.
- **Per-context action lock (`:289-290`):** the real phase-2 first takes the candidate's
  async action lock, then the registry write lock. The model folds the action lock into the
  write-locked section; this only *removes* interleavings (strictly fewer races), so it
  cannot hide a counterexample the real code would have — and the RED run shows the
  remaining write-lock-vs-read-lock race is the one that matters.
- **Model ≠ code:** no automated conformance harness; anchors above are the manual link.
