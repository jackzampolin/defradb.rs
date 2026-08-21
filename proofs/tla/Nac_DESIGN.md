# Nac — NAC lifecycle privilege-escalation safety (TLA+)

Backlog item #3 (`proofs/README.md` Coverage map): *NAC lifecycle priv-esc
safety — enable→disable→re-enable: no non-admin mutates admin set; disabled-flag
persists.*

This slice models the **Node Access Control (NAC) lifecycle state machine** and
the privilege-escalation hazards across the
`Enabled → DisabledTemporarily → re-enable` window. It is **distinct from the
Auth slice** (`Auth.tla`), which models the per-request management-channel
signature/JWT + NAC-permission gate at the HTTP/transport boundary. The Auth
slice answers *"is this request from a fresh, authorized admin DID?"*; this slice
answers *"can the lifecycle status machine itself be driven into a state that
lets a non-admin mutate the admin set?"*

## Property

Across the lifecycle, the model proves four invariants:

| Invariant | Statement |
|---|---|
| `INV_NoNonAdminMutatesAdminSet` | No actor that is not a **ground-truth** admin (owner or persisted-admin) ever causes a change to the protected admin set. **Headline.** |
| `INV_NoWriteWhileDisabled` | No admin-set mutation is ever applied while `status = DisabledTemporarily`. |
| `INV_DisabledPersistsAcrossRestart` | Whenever `status = DisabledTemporarily`, the persisted disk flag is set — so a restart recovers Disabled and the disable cannot be skipped by bouncing the node. |
| `INV_ReEnableNeedsPersistedAdmin` | Every `DisabledTemporarily → Enabled` transition was authorized by a ground-truth admin. |

### Independent oracle (why GREEN is not vacuous)

The headline invariant is stated against `IsAdminGT(a)` — an actor is an admin
iff it is the `Owner` or appears in `admins_persisted`. This is the **persisted
relationship set**, computed *independently of the runtime `status`*. It is
**not** the mechanism's own `is_admin` runtime decision. The dangerous runtime
check `IsAdminLive` (permissive whenever `status ≠ Enabled`) is modeled
separately and is exactly what the RED variants abuse. Because the oracle is
ground truth, a buggy live-permissive check cannot make the invariant
vacuously true: when a non-admin mutates the set, `dirty_by_nonadmin` flips
regardless of what the runtime check decided.

The write-block invariant uses a history bit `wrote_while_disabled` set by the
actual `AddAdmin`/`RemoveAdmin` actions exactly when a mutation fires with
`status = DisabledTemporarily`. It therefore has teeth independent of *who* the
writer was — the `BugWriteNotBlocked` variant trips it even if the writer is a
legitimate admin.

**Vacuity self-check (run, observed):** negated-reachability probes confirm GREEN
actually reaches `DisabledTemporarily`, performs admin-set mutations, changes
the admin set, and reaches the `(Disabled ∧ disk_flag set)` state that `Restart`
recovers from. Each probe is reported **violated** by TLC, i.e. the interesting
state is reachable — so the four invariants hold over a non-trivial state space,
not an empty one.

## Source anchors

### Rust (this repo)

| Symbol in model | Code | Anchor |
|---|---|---|
| `status ∈ {NotConfigured,Enabled,DisabledTemporarily}` | `NacStatus` enum | `crates/acp/src/nac/node_acp/mod.rs:51-57` |
| `disk_disabled` (persisted flag) | `DISABLED_RELATION` sentinel persisted in store | `crates/acp/src/nac/node_acp/mod.rs:33` |
| `Restart` recovers status from disk | `load()`: status := Disabled iff `_disabled` relationship present | `crates/acp/src/nac/node_acp/mod.rs:96-143` |
| `Disable` persists the flag | `disable()` stores `_disabled` relationship | `crates/acp/src/nac/node_acp/lifecycle.rs:74-108` (persist at 88-96) |
| `ReEnable` clears the flag | `re_enable()` deletes `_disabled` relationship | `crates/acp/src/nac/node_acp/lifecycle.rs:120-155` (delete at 132-144) |
| `IsAdminLive` (permissive while ≠ Enabled) | `is_admin()` returns `Ok(true)` for everyone when `status != Enabled` | `crates/acp/src/nac/node_acp/operations.rs:72-79` |
| `IsAdminGT` (persisted oracle) | `is_admin_persisted()` checks stored relations regardless of status | `crates/acp/src/nac/node_acp/operations.rs:87-104` |
| write-block while disabled | `add_admin`/`remove_admin`/`add_permission_grant`/`remove_permission_grant` reject with `InvalidPolicy` when `status == DisabledTemporarily` | `operations.rs:110-117, 179-186, 238-250, 307-319` |
| `Disable` auth via LIVE check | `NacManager::disable()` uses `is_admin` | `crates/db/src/nac/lib.rs:235-244` |
| `ReEnable` auth via PERSISTED check | `NacManager::re_enable()` uses `is_admin_persisted` — **the crux** | `crates/db/src/nac/lib.rs:247-256` |

### Go (origin/develop — the live upstream, not the stale checkout)

| Concept | Anchor |
|---|---|
| NodeACP kept `Start()`ed even while disabled, *so re-enable auth can be checked against persisted relations* (comment states the rationale) | `internal/db/acp/nac.go:44-61` |
| `CheckNodeOperationAccess`: `if Status != NACEnabled && perm != NodeReEnableNACPerm { return nil }` — unrestricted while disabled **except** re-enable, which falls through to the real persisted ACP check. Exact Go analog of the Rust `is_admin`/`is_admin_persisted` asymmetry. | `internal/db/acp/check.go:165-176` |
| `DisableNAC`: guards, checks `NodeDisableNACPerm`, sets DisabledTemporarily, `saveNodeACPDesc` | `internal/db/db_nac.go:107-128` |
| `ReEnableNAC`: guards, checks `NodeReEnableNACPerm`, sets Enabled, `saveNodeACPDesc` | `internal/db/db_nac.go:73-95` |
| write-block: `add/deleteNACActorRelationship` reject with `ErrACPOperationButACPNotAvailable` when `Status != NACEnabled` | `internal/db/db_nac.go:189-281` |
| status persisted to systemstore as JSON (`fetch`/`saveNodeACPDesc`) → survives restart | `internal/db/db_nac.go:384-450` |

## Scenarios (red/green)

| Config | Mode | Verdict | Mechanism toggled |
|---|---|---|---|
| `MC_Nac_Green` | `Correct` | **GREEN — all 4 hold** | correct mechanism |
| `MC_Nac_Red_WriteWhileDisabled` | `BugWriteNotBlocked` | **RED** — `INV_NoWriteWhileDisabled` *and* `INV_NoNonAdminMutatesAdminSet` violated | write-block-while-disabled guard removed; live `is_admin` is permissive while disabled, so a non-admin mutates the admin set |
| `MC_Nac_Red_ReEnableLive` | `BugReEnableLive` | **RED** — `INV_ReEnableNeedsPersistedAdmin` violated | `re_enable` authorized by LIVE `is_admin` (true for everyone while disabled) instead of the persisted check; a non-admin re-enables |
| `MC_Nac_Red_NoPersist` | `BugNoPersist` | **RED** — `INV_DisabledPersistsAcrossRestart` violated | `disable()` does not persist the flag; a restart silently recovers Enabled and forgets the disable |

> The `BugReEnableLive` case is *why a dedicated re-enable-authorization
> invariant is required*: under it the headline `INV_NoNonAdminMutatesAdminSet`
> still holds (post-re-enable writes are re-gated by the persisted check, since
> the non-admin who re-enabled is still not a ground-truth admin), so only
> `INV_ReEnableNeedsPersistedAdmin` catches the unauthorized lifecycle
> transition. This is the subtle, real escalation-adjacent bug the slice is
> built to expose.

## Run / verify

```bash
cd proofs/tla
./tools/tlc -metadir states/b_nac_green -config MC_Nac_Green.cfg                 MC_Nac_Green.tla
./tools/tlc -metadir states/b_nac_wd    -config MC_Nac_Red_WriteWhileDisabled.cfg MC_Nac_Red_WriteWhileDisabled.tla
./tools/tlc -metadir states/b_nac_rel   -config MC_Nac_Red_ReEnableLive.cfg       MC_Nac_Red_ReEnableLive.tla
./tools/tlc -metadir states/b_nac_np    -config MC_Nac_Red_NoPersist.cfg          MC_Nac_Red_NoPersist.tla
```

Observed: GREEN reports "No error has been found"; each RED reports the named
invariant "is violated" with a concrete counterexample trace.

## Boundaries

- **Bounded instance:** 1 owner, 1 starting admin, 1 non-admin, ≤2 admin-set
  mutations. This is the minimal witnessing shape; the conclusion is structural
  (the lifecycle guards), not quantity-sensitive.
- **Abstracted:** Zanzibar relation evaluation is collapsed into the
  `admins_persisted` set and `IsAdminGT`; the relation-store / engine is not
  modeled (it is exercised by `--test nac` integration tests and the Auth slice).
- **Out of scope here:** the per-request signature/JWT freshness and
  management-channel entry-point gating — those are the Auth slice's job. This
  slice assumes the Auth gate and models only the lifecycle status machine on
  top of it.
- **Integrator TODO:** no automated model↔code conformance harness yet (repo-wide
  policy, `proofs/README.md` "Model ≠ code"). Keep anchors in step manually if
  `lifecycle.rs` / `operations.rs` / `db-nac/src/lib.rs` change the disable
  persistence, the write-block, or the `is_admin` vs `is_admin_persisted` split.
- **Integrator TODO (run-all):** add the four `MC_Nac_*` rows to
  `proofs/tla/run-all.sh` (shared file, intentionally not edited by this slice).
```
  "MC_Nac_Green.cfg                  MC_Nac_Green.tla                  GREEN"  # nac lifecycle correct
  "MC_Nac_Red_WriteWhileDisabled.cfg MC_Nac_Red_WriteWhileDisabled.tla RED"   # nac: write while disabled
  "MC_Nac_Red_ReEnableLive.cfg       MC_Nac_Red_ReEnableLive.tla       RED"   # nac: re-enable via live is_admin
  "MC_Nac_Red_NoPersist.cfg          MC_Nac_Red_NoPersist.tla          RED"   # nac: disable not persisted
```
