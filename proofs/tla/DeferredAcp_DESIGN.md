# DeferredAcp — Deferred-ACP overlay consistency (TLA+)

Backlog item #2 (`proofs/README.md` Coverage map / `survey/query-plan.md`): *Deferred-ACP
overlay consistency — a txn-local ACP projection gates reads exactly as the committed state
would; fail-closed across commit/rollback.*

This slice models the **deferred-ACP overlay** in
`crates/query/src/txn/primitives/context.rs`: a `DeferredAcpMutations` object that an explicit
DefraDB transaction uses to (a) maintain a txn-LOCAL `projected_registrations` map and (b)
buffer the real ACP register/unregister writes as commit-time hooks. Access checks within
the txn consult the projection FIRST (`check_doc_access_with_overlay`); the buffered hooks
apply the actual ACP writes only after the storage txn commits.

It is **distinct from the Acp and Commits slices**, which both assume an *already-committed*
ACP state (Zanzibar soundness + revocation cache; dual-path commit gating). This slice is the
only one that models the **uncommitted projection → committed transition** itself: isolation
between concurrent txns, atomic commit-time hook application, no-op rollback, and the
fail-closed gate across the projected→committed boundary.

## Property

The model proves, over all interleavings of **two concurrent txns** doing register /
unregister / read / commit / rollback:

| Invariant | Statement |
|---|---|
| `INV_FailClosedActive` | **Headline.** Every read the overlay GRANTED is, at the instant of the grant, also granted by the ground-truth ACP rule over the committed state that *this txn's own commit would produce* (`committed (+) proj[t]`). An in-flight projection never grants access the committed state would deny. |
| `INV_NoCrossTxnLeak` | **Isolation.** A read granted when the txn had NO projection of its own for the doc must be granted by the BARE committed state — never justified by a *sibling* txn's uncommitted projection. |
| `INV_RollbackNoOp` | **Atomicity / abort.** A rolled-back txn changes committed ACP not at all (ghost bit `rbDirtied`, set exactly when a rollback path mutates `committed`, stays FALSE). |
| `INV_TypeOK` | type invariant. |

### Independent oracle (why GREEN is not vacuous)

Correctness is judged from `committed` — the actually-committed ACP registration state — via
`Grant(state, d, r)`, the ground-truth ACP rule (`Unregistered ⇒ anyone`; `Registered{o} ⇒
only the authenticated identity o`). This is **not** the overlay's own decision
(`OverlayGrant`). The `Read` action records, for each granted read, the *oracle* verdict
`Grant(ProspectiveCommitted(t), d, r)` computed by the independent `Grant`. `ProspectiveCommitted(t)`
is `committed` overlaid with **t's own projection only** — exactly what t's commit hooks
produce — so a buggy overlay that grants more than ground truth cannot make the invariant
vacuously true. The `RollbackNoOp` ghost bit is set by the `Rollback` action itself
independently of who rolled back or what they projected.

**Vacuity self-check (run, observed):** three negated reachability probes against the GREEN
config are each reported **violated** by TLC — i.e. GREEN actually reaches the interesting
states, so the invariants hold over a non-trivial space:

- `NoProjectedReadGranted` violated → a read is actually granted through a projection (or by a
  committed txn), not only by the committed fall-through.
- `NoCommittedChange` violated → a commit takes real effect (`committed` changes from init).
- `NotBothFinished` violated → both txns reach terminal (committed/rolledback) states.

## Source anchors

### Rust (this repo) — the mechanism under test

| Symbol in model | Code | Anchor |
|---|---|---|
| `Reg(o)` / `Unreg` (`ProjectedDocRegistration::Registered{owner}` / `Unregistered`) | `enum ProjectedDocRegistration` | `crates/query/src/txn/primitives/context.rs:30-34` |
| `proj[t]` (txn-local `projected_registrations`) | `DeferredAcpState.projected_registrations` | `context.rs:48-52` |
| one `DeferredAcpMutations` per txn (isolation by construction) | `Arc::new(DeferredAcpMutations::new())` at `begin()` | `crates/db/src/txn_registry.rs:721-722, 749-756` |
| `proj[t][d] := Reg(o)` + buffer hook (`Register` action) | `schedule_register_doc_object` | `context.rs:120-158` (projection insert 131-137; hook push 141-157) |
| `proj[t][d] := Unreg` + buffer hook (`Unregister` action) | `schedule_unregister_doc_object` | `context.rs:161-204` (projection insert 172-176) |
| `EffectiveProj` / `OverlayGrant` (the gate: projection-first, else committed) | `check_doc_access_with_overlay` | `context.rs:328-351` |
| `OwnerCheck="Strict"` (Registered grants only to owner) | `matches!(identity, Identity::Authenticated(did) if did == &owner)` | `context.rs:342-344` |
| projected `Unregistered ⇒ open` | `ProjectedDocRegistration::Unregistered => true` | `context.rs:341` |
| `Commit` runs hooks atomically (one action) | `run_all_logged` drains and runs all hooks | `context.rs:207-237` |
| commit hook wired to **commit only** (`on_success_async`), never rollback | `db_txn.on_success_async(... run_all_logged ...)` | `txn_registry.rs:723-734` |
| `on_success_async` semantics: fired on `commit()`, NOT on `discard()` | corekv `Transaction` trait docs | `crates/storage/src/corekv/traits.rs:304-369` |
| projection scoped per query execution via task-local | `scope_deferred_acp_mutations` | `crates/query/src/runner/executor.rs:444-474`, `context.rs:266-303` |
| register/unregister scheduled from the mutation runner | `schedule_register_doc_object` / `schedule_unregister_doc_object` call sites | `crates/query/src/runner/mutation.rs:705-720, 764-771` |

### Go (origin/develop — the live upstream, not the stale checkout)

The Rust deferred overlay is a **Rust-specific** mechanism; Go has no txn-local projection that
gates reads before commit. In Go, document registration runs **synchronously** against the live
document-ACP, and access checks read the live ACP directly:

| Concept | Anchor |
|---|---|
| `registerDocWithACP` calls `RegisterDocObject` synchronously (no projection/overlay) | `internal/db/collection_acp.go:31-46` |
| `RegisterDocOnCollectionWithDocumentACP` → `documentACP.RegisterDocObject(...)` directly | `internal/db/acp/register.go:23-51` |
| `checkAccessOfDocWithACP` reads live ACP (`CheckAccessOfDocOnCollectionWithACP`) — no projection | `internal/db/collection_acp.go:48-72` |
| Go's txn-local staging analog: `txn.OnSuccess(...)` deferred side-effects (e.g. `doc.Clean()`) wait for commit success, mirroring the same "delay state side-effects until commit" discipline the Rust hooks follow | `internal/db/document.go:506-507` |

**Audit note (for `proofs/README.md` Boundaries):** the Rust overlay's *value-add* over Go is
that, within an explicit txn, a just-created doc's reads are gated by the projection *before*
the real ACP write lands at commit. The committed-state oracle in this slice is exactly what
Go computes synchronously, so the model's GREEN claim — "projection gates as committed would" —
is the Rust mechanism reproducing Go's semantics one phase early, fail-closed. The hazards the
RED variants expose (shared overlay across txns, hooks on rollback, owner-check bypass) are
Rust-implementation hazards with no Go counterpart, which is precisely why they are worth a
model.

## Scenarios (red/green)

| Config | Knobs | Verdict | Mechanism toggled |
|---|---|---|---|
| `MC_DeferredAcp_Green` | `PerTxn / NoHooks / Strict` | **GREEN — all hold** | correct mechanism |
| `MC_DeferredAcp_Red_SharedOverlay` | `Shared / NoHooks / Strict` | **RED** — `INV_FailClosedActive` *and* `INV_NoCrossTxnLeak` violated | one global `DeferredAcpMutations` shared by both txns; txn B reads off txn A's uncommitted projection → access B's own commit would never grant |
| `MC_DeferredAcp_Red_RollbackHooks` | `PerTxn / RunHooks / Strict` | **RED** — `INV_RollbackNoOp` violated | rollback fires the buffered hooks (as if `on_success_async` ran on `discard()`) → committed ACP mutated though the txn aborted |
| `MC_DeferredAcp_Red_OwnerBypass` | `PerTxn / NoHooks / Any` | **RED** — `INV_FailClosedActive` violated | overlay grants a projected `Registered{owner}` to ANY authenticated identity (the `did == owner` check dropped) → a stranger reads what committed denies |

The `SharedOverlay` case violates `INV_NoCrossTxnLeak` even checked alone (verified) — the
dedicated isolation invariant is not subsumed by the headline; it pins the cross-txn-leak
property by name.

## Run / verify

```bash
cd proofs/tla
export JAVA=/opt/homebrew/opt/openjdk/bin/java   # or any JDK 11+; the tlc wrapper also honors JAVA_HOME
./tools/tlc -metadir states/b_dacp_green -config MC_DeferredAcp_Green.cfg              MC_DeferredAcp_Green.tla
./tools/tlc -metadir states/b_dacp_so    -config MC_DeferredAcp_Red_SharedOverlay.cfg MC_DeferredAcp_Red_SharedOverlay.tla
./tools/tlc -metadir states/b_dacp_rb    -config MC_DeferredAcp_Red_RollbackHooks.cfg MC_DeferredAcp_Red_RollbackHooks.tla
./tools/tlc -metadir states/b_dacp_ob    -config MC_DeferredAcp_Red_OwnerBypass.cfg   MC_DeferredAcp_Red_OwnerBypass.tla
```

Observed: GREEN reports "No error has been found" (120420 distinct states). Each RED reports
the named invariant "is violated" with a concrete two-txn counterexample trace.

## Boundaries

- **Bounded instance:** 2 txns, 1 doc, 2 candidate owners + the anonymous principal, ≤2
  projection ops per txn. The doc starts committed-Registered to `u1` (genuinely protected),
  so any over-grant to `u2`/anon or any cross-txn leak is a real violation. This is the minimal
  witnessing shape; conclusions are structural (the gate, the isolation boundary, the
  commit-vs-rollback hook split), not quantity-sensitive.
- **Abstracted:** the actual Zanzibar/`DocumentACP` relation evaluation is collapsed into the
  `Grant` rule (Unregistered ⇒ anyone; Registered{owner} ⇒ owner-only). Multi-relation policies,
  the bearer-token guard (`RequestBearerTokenGuard`), and hook fail-soft/`catch_unwind` logging
  are out of scope — modeled as an atomic, total commit application. The relation engine itself
  is the Acp slice's job and is exercised by `--test acp`.
- **Modeled as atomic:** commit applies the whole projection in a single TLA+ action, matching
  `run_all_logged` draining all hooks under one `on_success_async`. Partial-commit (some hooks
  applied, some not) is not modeled as a separate adversary; the storage txn's commit atomicity
  is assumed (it is the storage SSI slice's concern).
- **Out of scope here:** the per-request identity/JWT authentication that produces
  `Identity::Authenticated(did)` (the Auth/Jwt slices); the dual-path User-vs-Commits gating
  (the Commits slice). This slice assumes those gates and models only the deferred overlay on
  top of committed ACP.
- **Integrator TODO:** no automated model↔code conformance harness yet (repo-wide policy,
  `proofs/README.md` "Model ≠ code"). Keep anchors in step manually if `txn/context.rs` changes
  the projection-first gate, the owner check, the per-txn `DeferredAcpMutations` allocation in
  `txn_registry.rs`, or the `on_success_async` (commit-only) hook wiring.
- **Integrator TODO (run-all):** add the four `MC_DeferredAcp_*` rows to
  `proofs/tla/run-all.sh` (shared file, intentionally not edited by this slice):

```
  "MC_DeferredAcp_Green.cfg              MC_DeferredAcp_Green.tla              GREEN"  # deferred-acp overlay correct
  "MC_DeferredAcp_Red_SharedOverlay.cfg  MC_DeferredAcp_Red_SharedOverlay.tla  RED"   # deferred-acp: cross-txn projection leak
  "MC_DeferredAcp_Red_RollbackHooks.cfg  MC_DeferredAcp_Red_RollbackHooks.tla  RED"   # deferred-acp: hooks run on rollback
  "MC_DeferredAcp_Red_OwnerBypass.cfg    MC_DeferredAcp_Red_OwnerBypass.tla    RED"   # deferred-acp: projected owner-check bypass
```
