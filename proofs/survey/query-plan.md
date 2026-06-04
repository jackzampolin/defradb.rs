# query-plan — formal-modelability survey

## Purpose
Query plan tree + planner + execution-boundary abstractions (extracted from `query`
per #670). Implements Volcano-iterator plan nodes (scan, select, limit, joins,
aggregates, cursor, mutation create/update/delete/upsert, permission/SE filters),
the planner (builder, index selection, mapping), and the read/write boundary traits
(`DocFetcher`, `DocMutator`). `txn/` holds plan-layer transaction primitives: the
`TransactionContext` trait, registry, and the **deferred-ACP-mutation overlay**.

## State machines
- **Deferred-ACP overlay** (`txn/context.rs`): explicit txns buffer ACP register/
  unregister writes as commit-time hooks AND maintain a txn-local `projected_registrations`
  map (`Registered{owner}` / `Unregistered`). Access checks read the projection first
  (`check_doc_access_with_overlay`), so a not-yet-committed registration already gates
  reads to the owner; hooks then apply the real ACP writes only after the storage txn
  commits. Hooks are fail-soft (logged, catch_unwind). This is a two-phase (projected →
  committed) security state machine with isolation + commit-ordering concerns.
- **UpsertNode** (`plan/mutation/upsert.rs`): 0-match→create / 1-match→update / >1→error
  branching. Deterministic, single-shot; integration tests cover it.
- **PlanNode lifecycle** (init→start→next*→close): per-node iterator protocol; mechanical.

## Candidates
| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Deferred-ACP overlay consistency | TLA+ | Within a txn, a projected `Registered{owner}` gates reads to that owner and `Unregistered` opens reads, exactly as the post-commit ACP state would; on commit all hooks apply (a registered doc becomes ACP-registered); on rollback no hook runs and no ACP state changes; a concurrent txn never observes another's uncommitted projection. No reader gains access the committed state would deny (fail-closed across the projected→committed boundary). | no (Commits/Acp slices reference `check_doc_access_with_overlay` only as the gate call-site; neither models the buffered overlay/commit-hook transition) | medium |

## Verdict
**Model-worthy: yes — one medium candidate.** The deferred-ACP overlay is a genuine
security state machine (uncommitted projection that gates access + commit-time hook
application) not covered by the existing Commits (dual-path gating) or Acp (Zanzibar
soundness + revocation cache) slices, which both assume already-committed ACP state.
Everything else in the crate — Volcano plan nodes, upsert branching, planner/index
selection, fetcher/mutator boundary traits — is deterministic plumbing validated by
the integration suite; no model warranted.
