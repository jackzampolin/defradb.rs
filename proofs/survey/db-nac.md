# db-nac — Formal-Modelability Survey

## Purpose

`crates/db-nac/` is a thin **config-aware wrapper** around `acp::nac::NodeACP`.
It adds three things to the DB layer: (1) a `NacConfig` (enabled / dev_mode /
data_path) struct, (2) a startup `initialize()` routine that drives the NAC
lifecycle off config, and (3) an **authorization layer** on lifecycle
transitions (disable/re-enable/purge/grant). The `NacManagerApi` trait
(`trait_impl.rs`) is pure delegation; `factory.rs` wires memory/redb stores.

## State machines

- **NAC lifecycle** (`NacStatus`: NotConfigured → Enabled → DisabledTemporarily
  → Enabled; purge → NotConfigured). The transitions, idempotence, and the
  write-blocking-while-disabled invariant **live in `crates/acp/src/nac/node_acp/
  lifecycle.rs`**, not here. `db-nac::initialize()` only *selects* transitions
  from config + current status.
- **Authorization gate on transitions** (lib.rs): `disable` requires `is_admin`
  (live), `re_enable` requires `is_admin_persisted` (stored), `purge` requires
  `dev_mode && is_admin`. This asymmetry (live vs persisted admin check across a
  disable→re-enable window) is the only non-trivial security logic in the crate.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| NAC lifecycle auth gate | TLA+ | No remote/non-admin actor can disable/re-enable/purge NAC, and the disable→re-enable window cannot be used for privilege escalation (live-`is_admin` vs persisted-`is_admin_persisted` asymmetry is sound) | partial — **Auth** slice (#1012 A2) already models the management-channel gate and NAC enable/disable/re-enable delegating admin checks to NAC | low |
| NAC status transition validity | TLA+ | Status transitions are well-formed (no re-enable from Enabled, no disable from NotConfigured); invariant enforced in `acp` lifecycle.rs | yes — belongs to **acp** crate; covered by acp-crate lifecycle logic + Auth slice | low |

## Verdict

**Plumbing-dominant.** The crate is a config gate + delegation shim; the real
NAC state machine and its security invariants live in `crates/acp`. The one
candidate worth noting (the live-vs-persisted admin-check asymmetry across the
disable/re-enable window) is a marginal extension of the already-built **Auth**
slice, not a new model-worthy concern. Integration tests (`--test nac`) plus the
existing Auth/Acp TLA slices cover the behavior. `model_worthy: false`.
