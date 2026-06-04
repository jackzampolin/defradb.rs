# acp — Formal-Modelability Survey

## Purpose

`crates/acp/` is DefraDB's access-control core. It provides document-level access
control (DAC) and node-level access control (NAC) on top of a Zanzibar relation
engine (re-exported from `crates/zanzibar/`). Responsibilities: DPI policy
validation, relation-tuple storage (`store.rs`/`persistent.rs`/`local.rs`),
`DocumentACP` checks (owner/direct/wildcard tuples, read-implying relations),
P2P export/replace of doc-scoped relationships, and the NAC subsystem
(`nac/`) — a node-scoped policy with an enable/disable/re-enable/purge lifecycle.

## State machines

- **Zanzibar permission evaluation** (`zanzibar` engine): rewrite-closure
  semantics over `This / ComputedUserset / TupleToUserset / Union / Intersection
  / Difference`. Algebraic, deterministic — Lean territory.
- **DAC document gating** (`dac.rs`/`local.rs`): public vs registered docs; dual
  observable surface (User query + commit blocks).
- **NAC lifecycle** (`nac/node_acp/lifecycle.rs`): `NotConfigured → Enabled ⇄
  DisabledTemporarily → (purge) NotConfigured`. Security-critical: writes
  (add/remove admin, grant/revoke) are **blocked while DisabledTemporarily** to
  prevent privilege escalation; the disabled flag is persisted as a relationship
  so it survives restart; `re_enable` uses `is_admin_persisted` (stored) while
  `disable` uses live `is_admin`.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Zanzibar check soundness / no-escalation | Lean | `check=true` iff subject in rule closure; no accepted permission outside closure; positive-fragment removal monotonicity; deterministic eval | yes — **acp** slice (`Acp/Soundness.lean`) | low |
| Tuple revocation + stale positive cache | TLA+ | After revoke propagates, no node grants the revoked tuple; positive cache must be invalidated on revoke | yes — **acp** slice (`MC_Acp_Green` / `MC_Acp_StaleCache_Red`) | low |
| ACP-on-commits dual-path gating | TLA+ | Non-granted reader never obtains a protected doc via User path, local `_commits`, or replicated commit blocks | yes — **commits** slice (`MC_Commits_*`) | low |
| NAC lifecycle privilege-escalation safety | TLA+ | Across `Enabled→DisabledTemporarily→re_enable` no non-admin/remote actor mutates the admin set; write-while-disabled is rejected; persisted disabled-flag survives restart so re-enable is not skippable | partial — **Auth** slice models management-channel gate + admin-delegation, but not the lifecycle status machine + write-block + restart-persistence invariant | medium |
| NAC management-channel auth gate | TLA+ | Every NAC mutation requires a fresh signature-verified admin DID | yes — **Auth** slice | low |

## Verdict

**Model-worthy, but mostly already covered.** Three of this crate's core concerns
(Zanzibar soundness, tuple-revocation/cache, commits dual-path) already have
dedicated Lean/TLA slices, and the NAC management gate is in the Auth slice. The
one incremental gap is the **NAC lifecycle state machine** — the
write-blocked-while-disabled + restart-persistence invariant in
`nac/node_acp/lifecycle.rs`/`operations.rs` — which the Auth slice touches only
at the management-gate level. Medium priority: a small TLA+ extension of Auth
would close it. `model_worthy: true` (single new candidate).
