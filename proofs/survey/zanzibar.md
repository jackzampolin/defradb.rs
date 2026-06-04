# zanzibar — Formal-Modelability Survey

## Purpose

`crates/zanzibar/` is a standalone Google-Zanzibar permission engine with a
pluggable KV backend (`ZanzibarStore` trait). It evaluates userset-rewrite rules
(`This / ComputedUserset / TupleToUserset / Union / Intersection / Difference`)
via a goal-tree search with cycle detection (`NodeTrail`) and per-check
memoization (`CheckCache`). `acp` re-exports this engine as its decision core.

## State machines

- **Permission evaluation** (`engine/evaluate.rs`): recursive rewrite-closure
  search. `This` → direct tuple lookup; `ComputedUserset` → same-object relation;
  `TupleToUserset` → follow relation targets/entity-sets (wildcard grants
  immediately); set ops fold as OR / AND / (base AND NOT subtract). Algebraic and
  deterministic — Lean territory, not a concurrent state machine.
- **Cycle detection / termination** (`engine/cache.rs` `NodeTrail`): a visited-set
  threaded through recursion so cyclic relation graphs return `false` instead of
  diverging. Abstracted by the fuel/budget termination proof in the acp slice.
- **Memoization** (`CheckCache`): within a single `check`/`check_many` call,
  caches `(resource,object,relation,subject) → bool`. Pure plumbing — must not
  change the result vs. uncached eval (a corollary of determinism).
- **Lookup table** (`lookup.rs`): pure HashMap-of-HashMap policy index. Glue.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Check soundness / no-escalation | Lean | `check=true` iff subject in rewrite-closure; no grant outside closure; Difference denies; positive-fragment removal monotonicity; deterministic eval; termination | yes — **acp** slice (`Acp/Soundness.lean`: `eval_iff_derives`, `check_sound/complete`, `INV_NoEscalation`, `check_deterministic`, `eval_terminates`, `buggyDifferenceOverGrants`) | low |
| Cache coherence | Lean/none | memoized `check_many` returns the same verdict as uncached `check` for every request | no — but it is a direct corollary of `check_deterministic`; not worth a separate slice | low |
| Cycle-detection safety | Lean/none | cyclic relation graph terminates and denies rather than diverging | yes (abstracted) — `eval_terminates` fuel model in acp slice | low |

## Verdict

**Plumbing-with-one-core-idea, and the core is already proven.** The engine's
entire correctness surface (rewrite-closure soundness, no-escalation,
determinism, Difference correctness, termination) is covered by the existing
**acp** Lean slice, which anchors directly on this crate's `expression/` and
`engine/` source. The remaining pieces (cache, lookup table, store traits) are
deterministic glue with no new proof obligations. No new model-worthy work here.
