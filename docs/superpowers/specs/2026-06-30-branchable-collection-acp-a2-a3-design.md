# Branchable Collection ACP — A2 (read enforcement) + A3 (P2P sync enforcement)

**Status:** Design approved (2026-06-30)
**Branch:** `feat/branchable-collection-acp-a1` (will hold the full A1–A3 epic)
**Parity oracle:** Go DefraDB v1.0.0, commit `3f627855` ("feat: Branchable collection ACP (#4990)")
**Predecessor:** A1 (registration) — branchable + permissioned collections are registered as ACP
objects keyed by `collection_id`. A2/A3 *consume* that registration to gate reads.

## Background

A1 made a branchable + permissioned collection register an ACP object whose id is the stable
`collection_id`, gating its collection-level commit DAG the same way document registration gates a
document. A1 is registration only — nothing yet *enforces* that gate. This design adds the
enforcement:

- **A2 — read enforcement** on local read paths (commits query, signature verification).
- **A3 — sync enforcement** on the P2P path so private branchable data is never served to peers
  that cannot read it, plus the encryption-key gate and a block-CID integrity check.

The entire Go epic landed as one commit (`3f627855`). It introduced exactly **one new rule** —
`CheckDocReadAccess` — and wired it into five enforcement sites. This design ports that rule
faithfully (the approved approach: *faithful port, single pass*) and wires it into the equivalent
Rust sites, accounting for where Rust's architecture differs from Go's.

## The core rule: `check_doc_read_access`

One function, expressed once, reused at every site. It returns the final boolean but is built on an
internal verdict that distinguishes *explicit* from *public* access:

```rust
struct DocAccess {
    has_access: bool,
    // true when the verdict came from something specific to this actor: an ACP registration of the
    // object (granted/denied a relationship on it) or a DAC-bypass grant. false for access that is
    // unrestricted for everyone: an unpermissioned collection or a public (unregistered) object.
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

For a collection-level commit (no docID), step 1 is skipped and the verdict is purely the
collection-object check (a no-op for non-branchable collections).

`DocAccess` itself is the existing single-object check (today's `check_doc_permission` /
`check_doc_access_with_overlay`) extended to also report `explicit`:

- DAC-bypass (thread-local / node-ACP) → `{true, explicit:true}`.
- Collection has no policy → `{true, explicit:false}`.
- Object not registered (public) → `{true, explicit:false}`.
- Registered → `{backend_decision, explicit:true}`.

### Two backends, one algorithm

Rust has two ACP-check entry points and the rule must work over both without duplicating the logic:

- **Overlay-backed** (`query-plan::txn::check_doc_access_with_overlay` + `is_doc_registered_with_overlay`):
  txn-aware, sees same-transaction registrations. Used by the commits query.
- **Direct-backed** (`acp::DocumentACP` trait): no transaction overlay. Used by merge, KMS,
  signature verification, and the P2P serve path.

The algorithm is defined once over a small internal capability:

```rust
trait ObjectAccessChecker {
    async fn is_registered(&self, object_id: &str) -> acp::Result<bool>;
    async fn check_access(&self, object_id: &str, perm: DocumentPermission) -> acp::Result<bool>;
}
```

with two thin impls (overlay, direct). This mirrors Go's `identityFunc` parameterization. The rule
lives in `crates/db/src/collection_acp.rs` next to `check_doc_permission`; the overlay impl is
wired from `query-plan`.

## Enforcement sites

Go enforces the read rule at five sites (verified by enumerating every `CheckDocReadAccess*`
call in v1.0.0). The table maps each to its Rust home, noting current state and the architectural
divergence.

| # | Concern | Go site | Rust site | Current Rust state | Change |
|---|---------|---------|-----------|--------------------|--------|
| 1 | Commits query (A2) | `planner/commit.go:325,348` | `query/src/runner/commits.rs::execute_commits_query` | `docID None => continue` — **collection-level DAG ungated**; doc commits filtered per-doc only | Gate collection-level commits on the collection object; route doc commits through `check_doc_read_access` (overlay backend) so branchable public docs also require collection access |
| 2 | Signature verify (A2) | `internal/db/verify.go:115` | `db/src/block_verify.rs:110` | gates only when `delta.doc_id()` is `Some` — **collection blocks bypass** | Resolve collection; use `check_doc_read_access`; gate collection-level blocks on the collection object |
| 3 | P2P serve boundary (A3) | `p2p.go:197` `SetBlockAccessFunc(hasAccess)` + `trySelfHasAccess` (pull) | `p2p/src/bitswap/filter.rs` (libp2p) **and** Iroh serve path (`p2p/src/iroh/*`, via `ReplicatorRegistry`) | gates by **replicator membership per collection** only — no per-block read check | Add per-block `check_doc_read_access` (direct backend) so private branchable blocks are **never served** to a peer that cannot read them — the `hasAccess` analog, on **both** transports |
| 4 | KMS key gate (A3) | `internal/kms/pubsub.go:577` | `crates/kms` (`doesIdentityHaveDocPermission` analog) | per-doc read check | Switch to `check_doc_read_access` so encryption-key access respects collection gating |
| 5 | Pushlog CID integrity (A3) | `p2p.go:617` (`ErrBlockCIDMismatch`) | `p2p/src/sync/coordinator/event_handler/pushlog.rs` | (no equivalent check) | Reject a pushed block whose contents do not hash to the advertised CID — independent hardening bundled in #4990 |

### The architectural divergence (decided)

Go has **no ACP check at merge** (`internal/db/merge.go` is clean). It enforces read access at the
**serve boundary**: the producing node withholds blocks the requester cannot read, so private bytes
never cross the wire.

Rust today does the opposite: its serve filter gates only by *replicator membership*, and per-doc
ACP is enforced **receiver-side at merge** (`AcpMergeHandler`). For branchable collections — the
"private data leaks to peers" boundary — merge-time gating is too late: the bytes have already been
transmitted.

**Decision:** A3 enforces at the **serve boundary, matching Go** (site #3 above), on both the
libp2p bitswap filter and the Iroh serve path. The existing merge-time ACP gating
(`AcpMergeHandler`) is **kept as receiver-side defense-in-depth** and extended to also recognize
collection-level branchable blocks, but the primary leak fix is at serve.

## Semantics preserved (parity-critical)

- An **explicit denial** on a document always denies, regardless of collection access.
- An **explicit grant** on a document always grants, even inside a private branchable collection
  (single-document sharing out of a private collection works).
- Node-identity full-access and `dac_bypass` shortcuts still apply and count as `explicit`.
- Anonymous + registered object → deny.
- Unpermissioned collection or public object on a **non-branchable** collection → grant.
- **Non-branchable collections behave exactly as today** — the rule reduces to the document's own
  read access; no collection-object check is performed.

## Testing

Primary validation is Rust-native integration tests in `tools/integration-test`, run across a
**cross-implementation matrix** (the harness can pair a Go binary and a Rust binary):

- **rust ↔ rust**, **go ↔ go**, **go ↔ rust** — proves wire-level + enforcement parity on the sync
  path, and specifically that a Rust producer withholds private branchable blocks from a peer the
  same way a Go producer does.

Scenarios ported from Go #4990:

- `tests/integration/acp/dac/branchable/collection_commits_test.go` → commits-query read gating
  (collection-level DAG + doc commits, branchable vs non-branchable).
- `collection_mutation_test.go` → mutation/registration interplay (mostly A1, regression guard).
- `peer_test.go` → P2P serve-boundary enforcement (private collection blocks not served).
- `tests/integration/encryption/peer_acp_branchable_test.go` → KMS key-gate under branchable ACP.
- `tests/integration/signature/acp_branchable_test.go` → signature-verify read gating.

Unit tests:

- `check_doc_read_access` truth table in `crates/db`: {explicit-grant, explicit-deny, public-doc,
  collection-level} × {branchable, non-branchable} × {permissioned, unpermissioned}.
- Serve-filter per-block decision (bitswap + iroh): private branchable data block denied to a
  non-authorized replicator; collection-definition blocks still allowed; signature blocks allowed.
- Merge-handler collection-level-block gating (defense-in-depth).
- Pushlog CID-mismatch rejection.

## Out of scope

- Document create/update/delete mutation gating (unchanged; A1 covers registration).
- Policy-transition logic, NAC behavior.
- Retrofitting per-doc serve-boundary gating for **non-branchable** collections (Rust's pre-existing
  merge-time model for ordinary documents is unchanged; this epic adds the serve gate for the
  branchable read rule specifically — though the implementation naturally routes ordinary doc blocks
  through the same `check_doc_read_access`, so the gate applies uniformly where wired).

## Risks

- **A3 leak boundary (highest):** the serve-path gate is where private branchable data could leak to
  peers. It gets a **mandatory adversarial re-audit** (a dedicated verification workflow) before
  merge, covering both transports and the pull/pushlog paths.
- **Two-transport drift:** libp2p and Iroh serve paths must apply identical per-block decisions;
  mitigated by routing both through the same `check_doc_read_access` + a shared test.
- **Overlay vs direct backend drift:** commits-query (overlay) and serve/merge (direct) must not
  diverge on the rule; mitigated by the single shared algorithm over `ObjectAccessChecker`.
- **Performance:** per-block read checks on serve add ACP lookups to the hot sync path; the rule
  short-circuits for unpermissioned/non-branchable collections (the common case) before any ACP
  call.
