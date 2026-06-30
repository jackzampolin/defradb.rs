# Branchable Collection ACP — A2 (read enforcement) + A3 (P2P sync enforcement)

**Status:** Design — revised after spec review (2026-06-30)
**Branch:** `feat/branchable-collection-acp-a1` (will hold the full A1–A3 epic; PR body to be updated
from "A1 registration" to the full security feature)
**Parity oracle:** Go DefraDB v1.0.0, commit `3f627855` ("feat: Branchable collection ACP (#4990)")
**Predecessor:** A1 (registration) — branchable + permissioned collections register an ACP object
keyed by the stable `collection_id`. A2/A3 *consume* that registration to gate reads.

## Background

A1 made a branchable + permissioned collection register an ACP object whose id is the stable
`collection_id`, gating its collection-level commit DAG the way document registration gates a
document. A1 is registration only — nothing yet *enforces* the gate. This design adds enforcement:

- **A2 — read enforcement** on local read paths (commits query, signature verification).
- **A3 — sync enforcement** on the P2P path so private branchable data is never served to peers
  that cannot read it, plus the encryption-key gate.

The entire Go epic landed as one commit (`3f627855`), introducing exactly **one new rule** —
`CheckDocReadAccess` — wired into five enforcement sites. This design ports that rule faithfully
(approved approach: *faithful port, single pass*; approved sync scope: *full Go parity at the serve
boundary*) into the equivalent Rust sites.

## The core rule: `check_doc_read_access`

One function, expressed once, reused at every site. It returns the final boolean but is built on an
internal verdict distinguishing *explicit* from *public* access:

```rust
struct DocAccess {
    has_access: bool,
    // true when the verdict came from something specific to this actor: an ACP registration of the
    // object (granted/denied a relationship), or a DAC-bypass / node-identity grant. false for
    // access unrestricted for everyone: an unpermissioned collection or a public (unregistered)
    // object.
    explicit: bool,
}
```

**Algorithm** (mirrors Go `internal/db/acp/check.go::CheckDocReadAccessWithIdentityFunc`):

1. If `doc_id` is non-empty, compute `DocAccess` for the document:
   - `explicit && has_access` → **GRANT** (a doc explicitly shared with the actor is readable even
     inside an otherwise-private branchable collection).
   - `!has_access` → **DENY** (an explicit denial on the document always wins).
   - otherwise record `doc_accessible = has_access` and continue.
2. If the collection `is_branchable`, additionally compute `DocAccess` for the **collection object**
   (object id = `collection_id`). If `!has_access` → **DENY**. This is what makes a private
   branchable collection gate its *public* documents and its collection-level DAG.
3. Otherwise → **GRANT**.

A collection-level commit (no docID) skips step 1; the verdict is purely the collection-object check
(a no-op for non-branchable collections).

`DocAccess` itself is the existing single-object check (`check_doc_permission` /
`check_doc_access_with_overlay`) extended to also report `explicit`:

- DAC-bypass (thread-local) or node-identity match → `{true, explicit:true}`.
- Collection has no policy → `{true, explicit:false}`.
- Object not registered (public) → `{true, explicit:false}`.
- Registered → `{backend_decision, explicit:true}`.

### Two backends, one algorithm

The rule must run over both Rust ACP-check entry points without duplicating logic:

- **Overlay-backed** (`query-plan::txn::check_doc_access_with_overlay` + `is_doc_registered_with_overlay`):
  txn-aware; sees same-transaction registrations. Used by the commits query.
- **Direct-backed** (`acp::DocumentACP` trait): no transaction overlay. Used by signature verify,
  KMS, and the P2P serve path.

Defined once over a small internal capability with two thin impls:

```rust
trait ObjectAccessChecker {
    async fn is_registered(&self, object_id: &str) -> acp::Result<bool>;
    async fn check_access(&self, object_id: &str, perm: DocumentPermission) -> acp::Result<bool>;
}
```

This mirrors Go's `identityFunc` parameterization. The rule lives in
`crates/db/src/collection_acp.rs` next to `check_doc_permission`; the overlay impl is wired from
`query-plan`.

**Node-identity shortcut (review finding #6):** the node-identity full-access shortcut is itself Go
behavior (`collection_acp.go::checkAccessOfDoc` short-circuits when the requester DID equals the
node identity). In Go the node identity rides the request context, so it is naturally available at
every site. In Rust, `check_doc_access_with_overlay` currently carries no `node_did`, so the
shortcut is unavailable on the commits-query path. Threading `node_did` uniformly into both backends
is therefore a **Rust plumbing requirement to preserve that behavior**, not a Go-specific rule — the
*shortcut* matches Go; the *threading* is Rust-internal.

## Shared prerequisites (every site)

These were gaps in the first draft, surfaced by spec review; they are explicit requirements now.

- **Version ID → stable collection ID (finding #3).** Block paths expose `schema_version_id`, but A1
  registered ACP objects under the stable `collection_id`. Every A2/A3 site must resolve the block's
  `schema_version_id` to its `CollectionVersion` and check ACP on `collection.collection_id` and
  `collection.is_branchable` — never the version id.
- **Peer DID resolution via a transport-agnostic resolver (finding #2).** The serve-path gates run
  as the *requesting peer*. libp2p already resolves this on the host handle
  (`host::handle::get_peer_identity(peer_id) -> Option<Did>`, handle.rs:232), backed by a verified
  peer-identity cache populated through the identity protocol (`identity::from_token` +
  `verify_auth_token`, the analog of Go's `identityProtocol.GetIdentity` + `VerifyAuthToken`).
  However, this method is **not on the `P2PTransport` trait** (transport.rs:258) and the Iroh path
  only carries a `PeerId` into CAR handling. **Implementation requirement:** introduce a
  `PeerIdentityResolver` abstraction (`async fn resolve(&self, peer_id) -> Option<Did>`), implement
  it for libp2p (delegating to `get_peer_identity`) and Iroh, and inject it into **both** the Bitswap
  filter and the CAR handler so peer-DID resolution is identical across transports.
- **Unresolved DID → Anonymous, not blanket-deny (finding #1, Go parity).** When the resolver
  returns `None`, the serve gate passes `Identity::Anonymous` into `check_doc_read_access` rather
  than denying outright. This matches Go: `hasAccess`'s `identFunc` returns `None` on lookup failure
  and Go still runs `CheckDocReadAccess`, which **grants for public/unregistered objects** while an
  anonymous actor is **denied any registered (private) object**. So private branchable data is still
  withheld from an unverifiable peer, but public blocks remain fetchable — exactly as Go. (A
  stricter fail-closed posture — deny on unresolved DID even for public blocks — is available as a
  *documented deviation from Go* if desired, but is not the default here.)

## How Go's serve boundary actually works (and what "full parity" means)

`hasAccess` (p2p.go:338) decides, in order:

1. No document ACP → allow.
2. Signature block → allow (needed to verify sibling blocks; carries no collection data).
3. Lens blocks (config/module/wasm/chunks) → allow (schema-migration artifacts, no user data).
4. Definition delta (schema/collection definition) → allow.
5. **Replicator passthrough:** requesting peer is a registered replicator for the block's collection
   → **allow, no per-doc check.**
6. Otherwise: resolve `pid → identity` (cache or identity-protocol round-trip, token-verified), then
   per-doc `CheckDocReadAccess` (a block may have several owners; access to any one suffices).

So **"full Go parity at the serve boundary" = preserve replicator passthrough (step 5, which Rust
already implements) + add the non-replicator per-doc `check_doc_read_access` gate (step 6, new) +
the branchable collection-object dimension inside that rule.** Replicator-mediated flows (including
SE/KMS pre-positioning of not-yet-readable blocks) keep working because replicators are passthrough
in both implementations; the new gate hits ad-hoc / pubsub / non-replicator fetchers.

`trySelfHasAccess` (the pull side, in `processPushlogRequest`) mirrors the same rule for the local
node before pulling.

## Enforcement sites

Verified by enumerating every `CheckDocReadAccess*` / serve call in v1.0.0.

| # | Concern | Go site | Rust site | Current Rust state | Change |
|---|---------|---------|-----------|--------------------|--------|
| 1 | Commits query (A2) | `planner/commit.go:325,348` | `query/src/runner/commits.rs::execute_commits_query` | `docID None => continue` — **collection-level DAG ungated**; doc commits filtered per-doc only | Gate collection-level commits on the collection object; route doc commits through `check_doc_read_access` (overlay backend, with `node_did`) so branchable public docs also require collection access |
| 2 | Signature verify (A2) | `internal/db/verify.go:115` | `db/src/block_verify.rs:110` | gates only when `delta.doc_id()` is `Some` — **collection blocks bypass** | Resolve `version_id → CollectionVersion`; use `check_doc_read_access`; gate collection-level blocks on the collection object |
| 3 | P2P serve — Bitswap (A3) | `p2p.go:197` `SetBlockAccessFunc(hasAccess)` | `p2p/src/bitswap/filter.rs::check_access` (libp2p) | replicator-membership gate only; no identity/ACP context (`filter.rs:45` gets only `PeerId`+`Cid`) | Keep replicator passthrough; for non-replicator peers, resolve DID via `get_peer_identity` (fail-closed) and apply per-block `check_doc_read_access`. Inject DB/ACP/identity-resolver into the filter |
| 4 | P2P serve — CAR fetch (A3) | (same `hasAccess`, recursive + selective DAG) | `p2p/src/sync/coordinator/event_handler/car.rs::check_car_fetch_access` | checks only **root collection** access, then serves the whole DAG (`collect_dag_blocks` / `collect_exact_blocks`) | Filter CAR **response blocks per block** through `check_doc_read_access` for both recursive and exact (`collect_exact_blocks`) fetches; preserve replicator passthrough |
| 5 | KMS key gate (A3) | `internal/kms/pubsub.go:577` | `crates/kms` (`doesIdentityHaveDocPermission` analog) | per-doc read check; `DocCollectionInfo` lacks `is_branchable` (`policy.rs:61`) | Add `is_branchable` to `DocCollectionInfo` + its `DocCollectionLookup` producer; switch the gate to `check_doc_read_access` |

### Already implemented — regression coverage only (finding #5)

- **Pushlog/CAR block CID integrity.** `verify_block_cid` already rejects a block whose contents do
  not hash to the advertised CID — `p2p/src/sync/manager/process/pushlog.rs:235` ("finding 06-29")
  and in the CAR handler. This is **not new work**; the plan only adds/confirms regression tests
  (the Go `ErrBlockCIDMismatch` analog).

### Merge handler (receiver side)

With serve-boundary parity, the existing `AcpMergeHandler` (`on_protected_composite`,
`on_encrypted_link`; enforced only under `strict_replicated_doc_access`) is **left as-is** as
receiver-side defense-in-depth and to preserve the pre-position/replay model. A2/A3 do **not** flip
the strict default. If a branchable collection-level block can reach merge without a docID and needs
receiver-side gating, that is the one additive merge change to evaluate during implementation.

## Semantics preserved (parity-critical)

- An **explicit denial** on a document always denies, regardless of collection access.
- An **explicit grant** on a document always grants, even inside a private branchable collection.
- Node-identity full-access and `dac_bypass` shortcuts apply and count as `explicit`.
- Anonymous + registered object → deny.
- Unpermissioned collection or public object on a **non-branchable** collection → grant.
- **Non-branchable collections behave exactly as today** — the rule reduces to the document's own
  read access; no collection-object check.
- **Serve replicator passthrough is preserved** — registered replicators are served without a
  per-doc check, exactly as today and as Go.

## Testing

Primary validation is Rust-native integration tests in `tools/integration-test`, run across a
**cross-implementation matrix** (the harness can pair a Go binary and a Rust binary):

- **rust ↔ rust**, **go ↔ go**, **go ↔ rust** — proves wire-level + enforcement parity, and
  specifically that a Rust producer withholds private branchable blocks from a non-replicator peer
  the same way a Go producer does.

Scenarios ported from Go #4990:

- `acp/dac/branchable/collection_commits_test.go` → commits-query read gating (collection-level DAG
  + doc commits; branchable vs non-branchable).
- `acp/dac/branchable/peer_test.go` → P2P serve-boundary enforcement (non-replicator peer denied
  private collection blocks; replicator still served).
- `encryption/peer_acp_branchable_test.go` → KMS key-gate under branchable ACP.
- `signature/acp_branchable_test.go` → signature-verify read gating.
- `collection_mutation_test.go` → registration/mutation regression guard (mostly A1).

Unit tests:

- `check_doc_read_access` truth table in `crates/db`: {explicit-grant, explicit-deny, public-doc,
  collection-level} × {branchable, non-branchable} × {permissioned, unpermissioned} × {node-identity
  vs other}.
- Serve-filter per-block decision (bitswap + CAR): replicator passthrough; non-replicator with no
  resolvable DID → treated as Anonymous (denied registered/private objects, **allowed** for
  public/unregistered — Go parity); non-replicator without collection access → deny; signature /
  lens / definition blocks → allow.
- KMS gate honors `is_branchable`.
- CID-mismatch rejection regression (bitswap/CAR/pushlog).

## Scope decisions (record)

- **Approach:** faithful single-pass port of Go #4990's one rule into all Rust sites.
- **Sync boundary:** enforce at the **serve boundary** (Go parity), not merge; merge gating stays as
  receiver-side defense-in-depth.
- **Serve block scope:** **full Go parity** = replicator passthrough (existing) + per-doc
  `check_doc_read_access` for non-replicator peers + branchable collection-object dimension. This is
  uniform across collections; non-branchable collections reduce to per-doc read access.
- The contradictory "non-branchable serve gating out of scope" line from the first draft is
  **removed** — non-branchable per-doc serve gating for non-replicator peers is *in scope* as a
  consequence of full Go parity (and was a pre-existing Rust gap).

## Risks

- **Serve-gating vs SE/KMS & replay (top risk).** Full per-doc serve gating could regress flows that
  rely on a node fetching not-yet-readable blocks. Replicator passthrough preserves the primary
  pre-position path, but the implementation **must** run the existing P2P, encryption, SE, and
  identity integration suites; any regression there is a **blocker** that forces a scope re-decision
  (narrow to branchable-only, or to collection-level-DAG-only), not a paper-over.
- **A3 leak boundary.** The serve gate is where private branchable data could leak; it gets a
  **mandatory adversarial re-audit** (a dedicated verification workflow) before merge, covering both
  transports and the pull/pushlog paths.
- **Two-transport drift.** libp2p (bitswap) and Iroh (CAR/coordinator) serve paths must apply
  identical decisions and share peer-DID resolution; mitigated by routing both through the same
  `check_doc_read_access` and the same injected `PeerIdentityResolver` + a shared test.
- **Overlay vs direct backend drift.** Commits-query (overlay) and serve/verify/KMS (direct) must not
  diverge on the rule; mitigated by the single shared algorithm over `ObjectAccessChecker`.
- **Hot-path performance.** Per-block serve checks add ACP lookups; the rule short-circuits for
  unpermissioned/non-branchable + replicator-passthrough (the common cases) before any ACP call or
  identity round-trip.

## Out of scope

- Document create/update/delete mutation gating (unchanged; A1 covers registration).
- Policy-transition logic; NAC behavior.
- Flipping the `strict_replicated_doc_access` merge default.

## Suggested implementation order

1. Fix spec (this document). ✔
2. Build the shared `check_doc_read_access` rule + `ObjectAccessChecker` (overlay + direct), with
   `node_did`, and the `version_id → collection_id` resolution helper. Unit-test the truth table.
3. Wire **A2** local reads: commits query, then signature verify. Integration + unit.
4. Wire **A3**: KMS gate (`is_branchable`), then the serve gates (bitswap, then CAR) with peer-DID
   resolution + fail-closed; confirm Iroh path. Cross-impl integration + adversarial re-audit.
5. Confirm pushlog/CAR CID-integrity regression coverage.
