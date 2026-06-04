# Survey: `crates/http/`

## Purpose
Axum-based HTTP API server exposing Go-compatible DefraDB endpoints (`/api/v1`,
`/api/v0`): GraphQL, REST collections/documents, transactions, P2P management,
ACP/NAC, index, backup, lens, views, blocks. Almost entirely request routing,
header parsing, JSON (de)serialization, and delegation to operation traits
(`AppState` holds boxed trait objects). Real logic lives in `query`, `db`,
`p2p`, `acp`, `nac`, `identity`, `crdt`. The http crate is the enforcement
boundary, not the source of the behaviors being enforced.

## State machines
- **Request auth gate** (`auth_middleware.rs` + `route_permissions.rs` +
  `nac_guard.rs` + `identity_extractor.rs`): per-request transition
  `unverified -> verified -> authorized -> executed | rejected`. Route is
  classified `Exempt | IdentityOnly | Required(perm) | Dynamic`; non-exempt
  routes parse+verify a Bearer JWT (signature, exp/nbf, audience=Host) to a DID,
  then `Required` routes check the NAC grant set (with wildcard fallback only for
  anonymous). This is the only nontrivial state machine in the crate.
- Token extraction (`identity_extractor.rs`): `absent | invalid | valid |
  expired` credential states reduced to `Option<Did>`.
- No persistent/lifecycle status enums of its own; transactions, replicators,
  NAC enable/disable etc. are delegated downstream.

## Candidates
| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Management-auth gate | TLA+ | no remote config mutation without fresh, scope-correct actor-DID authorization; invalid/expired/revoked creds never authorize | yes (`Auth.tla` / `Auth_DESIGN.md`, anchored directly on `auth_middleware.rs`, `identity_extractor.rs`, `nac_guard.rs`, `route_permissions.rs`) | low |
| Route-permission completeness | none | every registered route maps to a permission; unknown -> safe default | covered by exhaustive unit test `all_registered_routes_return_expected_permission` + `Auth.tla` gating assumption | low |
| JSON wire / Go-compat shape | none | response format parity | integration tests (`tools/integration-test`) + FFI Go suite | low |

## Verdict
**Not model-worthy as a new slice.** The single security state machine worth
proving — the request authorization gate — is already formally modeled by the
existing `Auth.tla` slice, which cites this crate's files verbatim as its
grounded facts (green run + red runs for PeerID-only, stale-grant, wrong-scope).
Everything else is routing/serialization plumbing validated by integration and
FFI tests. No new TLA+ or Lean work is warranted here.
