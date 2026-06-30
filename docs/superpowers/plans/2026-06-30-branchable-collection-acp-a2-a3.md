# Branchable Collection ACP — A2 + A3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce branchable-collection ACP on local read paths (A2) and the P2P serve boundary (A3), consuming the A1 collection-object registration, at full parity with Go v1.0.0 `3f627855`.

**Architecture:** ONE algorithm — `acp::read_access::check_doc_read_access` — defined over an `ObjectAccessChecker` trait, with two checker impls: `DirectChecker` (in `acp`, used by signature-verify, KMS, and the P2P serve gate) and `OverlayChecker` (in `query-plan`, txn-aware, used by the commits query). It lives in the `acp` crate because `db`, `query-plan`, and `kms` all depend on `acp` (none can depend on `db`). The rule takes primitive params (`policy_id`, `resource_name`, `collection_id`, `is_branchable`, `doc_id`) so every caller — including KMS, which only has a `DocCollectionInfo` — can use it. A transport-agnostic `PeerIdentityResolver` supplies the requesting peer's DID at the serve boundary: libp2p via the existing token protocol, Iroh via direct `NodeId → did:key` derivation.

**Tech Stack:** Rust, async-trait, tokio; crates `acp`, `db`, `query`, `query-plan`, `kms`, `p2p`; integration harness in `tools/integration-test`.

## Global Constraints

- Parity oracle: Go v1.0.0 commit `3f627855`. Behavior must match unless a deviation is explicitly recorded in the spec.
- ACP objects are keyed by the **stable `collection_id`**, never `schema_version_id`. Every site resolves `schema_version_id → CollectionVersion` first and uses `collection.collection_id` + `collection.is_branchable`.
- Spec: `docs/superpowers/specs/2026-06-30-branchable-collection-acp-a2-a3-design.md`.
- `cargo clippy --all -- -D warnings` clean; `cargo fmt --all` applied before each commit.
- Unresolved peer DID at the serve boundary → `Identity::Anonymous` (Go parity), not blanket-deny.
- Replicator passthrough is **per-block, keyed on the block's stable `collection_id`** — never `is_any_replicator` and never the raw `schema_version_id`.
- Do NOT flip the `strict_replicated_doc_access` merge default.
- A2 and A3 in separate commits. The A3 serve-path work requires the Task 13 adversarial re-audit before the PR merges.

---

## File Structure

- `crates/acp/src/read_access.rs` (new) — `DocAccess`, `ObjectAccessChecker` trait, `DirectChecker`, `check_doc_read_access`. **The one algorithm.**
- `crates/acp/src/lib.rs` — module + re-exports.
- `crates/acp/tests/read_access_tests.rs` (new) — truth-table over `DirectChecker`.
- `crates/query-plan/src/txn/read_access.rs` (new) — `OverlayChecker: ObjectAccessChecker`.
- `crates/query-plan/src/txn/context.rs` — thread `node_did` into `check_doc_access_with_overlay`.
- `crates/query/src/runner/commits.rs` — A2 commits gating; `node_did` field on the runner.
- `crates/db/src/block_verify.rs` — A2 signature-verify gating.
- `crates/kms/src/policy.rs` — `is_branchable` on `DocCollectionInfo`; KMS gate calls `acp::check_doc_read_access`.
- `crates/kms/src/nac_dac_policy.rs` — wire the rule into `check_release`.
- `crates/p2p/src/peer_identity.rs` (new) — `PeerIdentityResolver` (libp2p + Iroh impls).
- `crates/p2p/src/bitswap/{filter.rs,read_gate.rs}` — late-bound `BlockReadGate` + per-block serve gate.
- `crates/p2p/src/sync/coordinator/event_handler/car.rs` — CAR per-block serve filtering.
- `tools/integration-test/tests/acp.rs` — cross-impl scenarios.

---

## Task 1: Core rule in the `acp` crate (`ObjectAccessChecker` + `DirectChecker` + `check_doc_read_access`)

**Files:**
- Create: `crates/acp/src/read_access.rs`
- Modify: `crates/acp/src/lib.rs`
- Test: `crates/acp/tests/read_access_tests.rs`

**Interfaces:**
- Consumes: `crate::{DocumentACP, DocumentPermission, Identity}`, `identity::Did`, `defra_core::dac_bypass::get_dac_bypass`.
- Produces:
  ```rust
  pub struct DocAccess { pub has_access: bool, pub explicit: bool }

  #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
  #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
  pub trait ObjectAccessChecker {
      /// Verdict for ONE object (docID or collection_id) under (policy_id, resource_name).
      async fn object_access(&self, policy_id: &str, resource_name: &str, object_id: &str)
          -> crate::Result<DocAccess>;
  }

  pub struct DirectChecker<'a> {
      pub acp: &'a dyn DocumentACP,
      pub identity: &'a Identity,
      pub node_did: Option<&'a Did>,
  }

  /// The branchable read rule. doc_id == "" means a collection-level commit.
  pub async fn check_doc_read_access(
      checker: &dyn ObjectAccessChecker,
      policy_id: &str,
      resource_name: &str,
      collection_id: &str,
      is_branchable: bool,
      doc_id: &str,
  ) -> crate::Result<bool>;
  ```

- [ ] **Step 1: Write the failing truth-table tests**

In `crates/acp/tests/read_access_tests.rs` (use `LocalDocumentACP` + an in-memory store, mirroring existing acp tests):

```rust
use acp::read_access::{check_doc_read_access, DirectChecker};
use acp::{DocumentACP, Identity};

async fn rule(acp: &dyn DocumentACP, ident: &Identity, branchable: bool, doc: &str) -> bool {
    let checker = DirectChecker { acp, identity: ident, node_did: None };
    check_doc_read_access(&checker, "pol", "users", "COL", branchable, doc).await.unwrap()
}

#[tokio::test]
async fn branchable_public_doc_requires_collection() {
    let acp = local_acp();
    let owner = did_a(); let stranger = did_b();
    acp.register_doc_object(&owner, "pol", "users", "COL").await.unwrap(); // collection -> owner
    // public doc + collection access => grant; + no collection access => deny
    assert!(rule(&acp, &Identity::Authenticated(owner.clone()), true, "docA").await);
    assert!(!rule(&acp, &Identity::Authenticated(stranger.clone()), true, "docA").await);
    // collection-level commit ("" doc): purely the collection object
    assert!(rule(&acp, &Identity::Authenticated(owner), true, "").await);
    assert!(!rule(&acp, &Identity::Authenticated(stranger), true, "").await);
}

#[tokio::test]
async fn explicit_doc_grant_overrides_collection() {
    let acp = local_acp();
    let owner = did_a(); let reader = did_b();
    acp.register_doc_object(&owner, "pol", "users", "COL").await.unwrap();   // collection -> owner
    acp.register_doc_object(&reader, "pol", "users", "docS").await.unwrap(); // doc -> reader
    assert!(rule(&acp, &Identity::Authenticated(reader), true, "docS").await); // grant despite no col access
}

#[tokio::test]
async fn nonbranchable_reduces_to_doc() {
    let acp = local_acp();
    let owner = did_a(); let stranger = did_b();
    acp.register_doc_object(&owner, "pol", "users", "docA").await.unwrap();
    assert!(rule(&acp, &Identity::Authenticated(owner), false, "docA").await);
    assert!(!rule(&acp, &Identity::Authenticated(stranger), false, "docA").await);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p acp --test read_access_tests`
Expected: FAIL — module `read_access` not found.

- [ ] **Step 3: Implement the rule + DirectChecker**

In `crates/acp/src/read_access.rs`:

```rust
use async_trait::async_trait;
use identity::Did;
use crate::{DocumentACP, DocumentPermission, Identity, Result};

#[derive(Debug, Clone, Copy)]
pub struct DocAccess { pub has_access: bool, pub explicit: bool }

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ObjectAccessChecker {
    async fn object_access(&self, policy_id: &str, resource_name: &str, object_id: &str)
        -> Result<DocAccess>;
}

pub struct DirectChecker<'a> {
    pub acp: &'a dyn DocumentACP,
    pub identity: &'a Identity,
    pub node_did: Option<&'a Did>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ObjectAccessChecker for DirectChecker<'_> {
    async fn object_access(&self, policy_id: &str, resource_name: &str, object_id: &str)
        -> Result<DocAccess> {
        if defra_core::dac_bypass::get_dac_bypass() {
            return Ok(DocAccess { has_access: true, explicit: true });
        }
        if let (Some(node), Identity::Authenticated(req)) = (self.node_did, self.identity) {
            if node == req { return Ok(DocAccess { has_access: true, explicit: true }); }
        }
        if !self.acp.is_doc_registered(policy_id, resource_name, object_id).await? {
            return Ok(DocAccess { has_access: true, explicit: false });
        }
        let has = self.acp
            .check_doc_access(self.identity, DocumentPermission::Read, policy_id, resource_name, object_id)
            .await?;
        Ok(DocAccess { has_access: has, explicit: true })
    }
}

/// Branchable read rule. Mirrors Go internal/db/acp/check.go::CheckDocReadAccessWithIdentityFunc.
pub async fn check_doc_read_access(
    checker: &dyn ObjectAccessChecker,
    policy_id: &str,
    resource_name: &str,
    collection_id: &str,
    is_branchable: bool,
    doc_id: &str,
) -> Result<bool> {
    if !doc_id.is_empty() {
        let a = checker.object_access(policy_id, resource_name, doc_id).await?;
        if a.explicit && a.has_access { return Ok(true); }   // explicit doc grant wins
        if !a.has_access { return Ok(false); }               // explicit doc denial wins
    }
    if is_branchable {
        let a = checker.object_access(policy_id, resource_name, collection_id).await?;
        if !a.has_access { return Ok(false); }
    }
    Ok(true)
}
```

Add `pub mod read_access;` to `crates/acp/src/lib.rs` and re-export `read_access::{check_doc_read_access, DirectChecker, ObjectAccessChecker, DocAccess}`.

Note: callers with an unpermissioned collection must skip the rule (no `policy_id`); the rule assumes a policy exists. Each call site already branches on `collection.policy.is_some()`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p acp --test read_access_tests`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/acp/src/read_access.rs crates/acp/src/lib.rs crates/acp/tests/read_access_tests.rs
git commit -m "feat(acp): branchable read rule (check_doc_read_access + DirectChecker)"
```

---

## Task 2: Thread `node_did` into the overlay access check

**Files:**
- Modify: `crates/query-plan/src/txn/context.rs:328` (`check_doc_access_with_overlay`) + internal recursive calls (448/463/491)
- Modify: callers in `crates/query-plan/src/plan/permission_filter.rs` and elsewhere
- Test: `crates/query-plan/tests/cross_object_read_path.rs`

**Interfaces:**
- Produces (changed signature): `check_doc_access_with_overlay(acp, identity, permission, policy_id, resource_name, doc_id, node_did: Option<&Did>)`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn overlay_grants_node_identity_full_access() {
    let (acp, node_did) = setup_registered_object_other_owner().await;
    let granted = query_plan::txn::check_doc_access_with_overlay(
        acp.as_ref(), &acp::Identity::Authenticated(node_did.clone()),
        acp::DocumentPermission::Read, "pol", "users", "objX", Some(&node_did),
    ).await.unwrap();
    assert!(granted);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p query-plan --test cross_object_read_path overlay_grants_node_identity_full_access`
Expected: FAIL — arity mismatch / not granted.

- [ ] **Step 3: Add the `node_did` shortcut**

In `crates/query-plan/src/txn/context.rs`, add the trailing `node_did: Option<&Did>` param and, at the top of the function body:

```rust
if let (Some(node), Identity::Authenticated(req)) = (node_did, identity) {
    if node == req { return Ok(true); }
}
```

Pass `node_did` through the internal recursive calls at 448/463/491.

- [ ] **Step 4: Fix all call sites**

Run: `cargo build -p query-plan -p query 2>&1 | grep -E "takes .* arguments|expected .* arguments"`
Add the trailing `node_did` arg at each (pass `None` where not yet plumbed).

- [ ] **Step 5: Verify**

Run: `cargo test -p query-plan --test cross_object_read_path overlay_grants_node_identity_full_access && cargo build -p query`
Expected: PASS + clean.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/query-plan crates/query
git commit -m "feat(acp): thread node_did into overlay access check"
```

---

## Task 3: `OverlayChecker` implementing `ObjectAccessChecker`

**Files:**
- Create: `crates/query-plan/src/txn/read_access.rs`
- Modify: `crates/query-plan/src/txn/mod.rs`, `crates/query/src/txn/mod.rs` (re-export)
- Test: `crates/query-plan/tests/cross_object_read_path.rs`

**Interfaces:**
- Consumes: `acp::read_access::{check_doc_read_access, ObjectAccessChecker, DocAccess}`, `is_doc_registered_with_overlay`, `check_doc_access_with_overlay` (Task 2).
- Produces:
  ```rust
  pub struct OverlayChecker<'a> {
      pub acp: &'a dyn DocumentACP,
      pub identity: &'a Identity,
      pub node_did: Option<&'a Did>,
  }
  // impl acp::read_access::ObjectAccessChecker for OverlayChecker<'_>
  ```

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn overlay_checker_gates_branchable_public_doc() {
    let (acp, owner, stranger) = setup_branchable_registered_collection().await; // COL -> owner
    let c_owner = query_plan::txn::OverlayChecker { acp: acp.as_ref(), identity: &owner_ident, node_did: None };
    let c_other = query_plan::txn::OverlayChecker { acp: acp.as_ref(), identity: &stranger_ident, node_did: None };
    assert!(acp::read_access::check_doc_read_access(&c_owner, "pol","users","COL",true,"docA").await.unwrap());
    assert!(!acp::read_access::check_doc_read_access(&c_other, "pol","users","COL",true,"docA").await.unwrap());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p query-plan --test cross_object_read_path overlay_checker_gates_branchable_public_doc`
Expected: FAIL — `OverlayChecker` not found.

- [ ] **Step 3: Implement `OverlayChecker`**

In `crates/query-plan/src/txn/read_access.rs`:

```rust
use async_trait::async_trait;
use acp::read_access::{DocAccess, ObjectAccessChecker};
use acp::{DocumentACP, DocumentPermission, Identity};
use identity::Did;
use super::context::{check_doc_access_with_overlay, is_doc_registered_with_overlay};

pub struct OverlayChecker<'a> {
    pub acp: &'a dyn DocumentACP,
    pub identity: &'a Identity,
    pub node_did: Option<&'a Did>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ObjectAccessChecker for OverlayChecker<'_> {
    async fn object_access(&self, policy_id: &str, resource_name: &str, object_id: &str)
        -> acp::Result<DocAccess> {
        if let (Some(node), Identity::Authenticated(req)) = (self.node_did, self.identity) {
            if node == req { return Ok(DocAccess { has_access: true, explicit: true }); }
        }
        if !is_doc_registered_with_overlay(self.acp, policy_id, resource_name, object_id).await? {
            return Ok(DocAccess { has_access: true, explicit: false });
        }
        let has = check_doc_access_with_overlay(
            self.acp, self.identity, DocumentPermission::Read,
            policy_id, resource_name, object_id, self.node_did,
        ).await?;
        Ok(DocAccess { has_access: has, explicit: true })
    }
}
```

Add `pub mod read_access;` + re-export `OverlayChecker` in `crates/query-plan/src/txn/mod.rs`, and re-export through `crates/query/src/txn/mod.rs`.

- [ ] **Step 4: Verify**

Run: `cargo test -p query-plan --test cross_object_read_path overlay_checker_gates_branchable_public_doc`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/query-plan crates/query
git commit -m "feat(acp): overlay ObjectAccessChecker for the query path"
```

---

## Task 4: Plumb `node_did` into the commits query runner

**Files:**
- Modify: `crates/query/src/runner/mod.rs` (QueryRunner struct + builder) — add `node_did: Option<Did>`
- Modify: `crates/embedded/src/node.rs` + `crates/defra-node/src/lib.rs` (runner construction) — wire `db.node_did()`
- Test: `crates/query/src/runner/mod.rs` unit test (builder sets the field)

**Interfaces:**
- Produces: `QueryRunner` gains `node_did: Option<Did>` + `with_node_did(Option<Did>) -> Self`; an accessor `fn node_did(&self) -> Option<&Did>`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn runner_carries_node_did() {
    let runner = test_runner().with_node_did(Some(did_a()));
    assert_eq!(runner.node_did(), Some(&did_a()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p query runner_carries_node_did`
Expected: FAIL — no `with_node_did`/`node_did`.

- [ ] **Step 3: Add the field, builder, accessor**

Add `node_did: Option<Did>` to the runner struct (default `None`), a `with_node_did` builder, and a `node_did(&self)` accessor. At every runner construction site (embedded `node.rs`, `defra-node/src/lib.rs`), call `.with_node_did(db.node_did())`.

Run: `cargo build -p query -p embedded -p defra-node`

- [ ] **Step 4: Verify**

Run: `cargo test -p query runner_carries_node_did`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/query/src/runner crates/embedded/src/node.rs crates/defra-node/src/lib.rs
git commit -m "feat(query): carry node_did on the query runner"
```

---

## Task 5: A2 — gate the commits query

**Files:**
- Modify: `crates/query/src/runner/commits.rs:737-839`
- Test: `tools/integration-test/tests/acp.rs` (`mod branchable_commits`)

**Interfaces:**
- Consumes: `OverlayChecker` (Task 3), `acp::read_access::check_doc_read_access`, `self.collections_map()`, `caller_identity`, `self.node_did()` (Task 4).

**Context:** Today commits.rs:771-775 does `docID None => continue` (collection-level commits ungated) and gates doc commits per-doc. Replace with the rule keyed on the resolved `CollectionVersion`.

- [ ] **Step 1: Write the failing integration test**

In `tools/integration-test/tests/acp.rs`, `mod branchable_commits`: identity A creates a branchable `@policy` collection; a public doc is added; identity B (no collection access) queries `_commits` for the collection-level DAG and the doc → expects `[]`; A → expects commits.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p integration-test --test acp -- branchable_commits::`
Expected: FAIL — B sees collection-level commits.

- [ ] **Step 3: Rewrite the ACP filter block**

Replace lines ~737-839 with a per-commit rule check:

```rust
use acp::Identity;
let node_did = self.node_did().cloned();
let identity = Identity::from(caller_identity.clone());
let collections = self.collections_map().await?;
let by_version: std::collections::HashMap<String, schema::CollectionVersion> =
    collections.values().map(|c| (c.version_id.clone(), c.clone())).collect();

let mut keep = Vec::with_capacity(commits.len());
for commit in &commits {
    let version_id = commit.get("collectionVersionId").and_then(|v| v.as_str());
    let doc_id = commit.get("docID").and_then(|v| v.as_str()).unwrap_or("");
    let allowed = match version_id.and_then(|v| by_version.get(v)) {
        None => true,
        Some(col) => match &col.policy {
            None => true,
            Some(policy) => {
                let checker = crate::txn::OverlayChecker {
                    acp: self.acp.as_ref(), identity: &identity, node_did: node_did.as_ref(),
                };
                acp::read_access::check_doc_read_access(
                    &checker, &policy.id, &policy.resource_name,
                    &col.collection_id, col.is_branchable, doc_id,
                ).await.unwrap_or(false)
            }
        },
    };
    keep.push(allowed);
}
let mut it = keep.into_iter();
commits.retain(|_| it.next().unwrap_or(false));
```

Delete the now-dead `policies_by_version` / `denied_commits` machinery it replaces.

- [ ] **Step 4: Verify + full suites**

Run: `cargo test -p integration-test --test acp -- branchable_commits:: && cargo test -p integration-test --test acp && cargo test -p integration-test --test query -- commits`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/query/src/runner/commits.rs tools/integration-test/tests/acp.rs tools/integration-test/tests/acp/
git commit -m "feat(acp): A2 gate commits query on branchable read rule"
```

---

## Task 6: A2 — gate signature verification

**Files:**
- Modify: `crates/db/src/block_verify.rs:110-136`
- Test: `crates/db/tests/block_verify_acp_tests.rs`

**Interfaces:**
- Consumes: `acp::read_access::{check_doc_read_access, DirectChecker}`, `database.get_collection_by_version_id`, `database.node_did()`.

- [ ] **Step 1: Write the failing test**

`crates/db/tests/block_verify_acp_tests.rs`: branchable permissioned collection registered to A; produce a collection-level signed block; `verify_block_signature` as B → `Err("missing permission")`; as A → `Ok`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p db --test block_verify_acp_tests`
Expected: FAIL — B passes (collection-level bypass).

- [ ] **Step 3: Replace the gate**

In `verify_block_signature_with_blockstore`, replace lines 110-136:

```rust
if let Some(schema_version_id) = block.delta.schema_version_id() {
    if let Some(collection) = database
        .get_collection_by_version_id(schema_version_id)
        .map_err(|e| format!("failed to get collection: {}", e))?
    {
        let col = collection.schema();
        if let Some(policy) = &col.policy {
            let doc_id = block.delta.doc_id()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();
            let node_did = database.node_did();
            let checker = acp::read_access::DirectChecker {
                acp: document_acp, identity: caller_identity, node_did: node_did.as_ref(),
            };
            let has = acp::read_access::check_doc_read_access(
                &checker, &policy.id, &policy.resource_name,
                &col.collection_id, col.is_branchable, &doc_id,
            ).await.map_err(|e| format!("ACP check failed: {}", e))?;
            if !has { return Err("missing permission".to_string()); }
        }
    }
}
```

- [ ] **Step 4: Verify**

Run: `cargo test -p db --test block_verify_acp_tests && cargo test -p integration-test --test acp`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/db/src/block_verify.rs crates/db/tests/block_verify_acp_tests.rs
git commit -m "feat(acp): A2 gate signature verification on branchable read rule"
```

---

## Task 7: A3 — KMS key-gate uses the shared rule

**Files:**
- Modify: `crates/kms/src/policy.rs:61` (`DocCollectionInfo` += `is_branchable`)
- Modify: `crates/kms/src/nac_dac_policy.rs:80-111` (`check_release` Document arm)
- Modify: the `DocCollectionLookup` producer (embedded layer; grep `impl DocCollectionLookup`)
- Test: `crates/kms/src/nac_dac_policy.rs` tests

**Interfaces:**
- Produces: `DocCollectionInfo { collection_id, policy_id, resource_name, is_branchable: bool }`.
- Consumes: `acp::read_access::{check_doc_read_access, DirectChecker}` (kms already depends on `acp`).

- [ ] **Step 1: Write the failing tests (corrected semantics — finding #1)**

The rule grants on an explicit doc grant *before* the collection check, so a "doc grant + collection deny" case CANNOT test the collection gate. Use a **public/unregistered doc + denied collection => Deny**, plus an **explicit doc grant => Allow**, plus **non-branchable => doc-only**.

In `crates/kms/src/nac_dac_policy.rs` tests, extend the fakes so `collection_for_doc` returns `is_branchable` and the fake DAC can answer per-object (registered? owner?):

```rust
#[tokio::test]
async fn branchable_public_doc_denied_without_collection_access() {
    // doc "d1" UNREGISTERED (public); collection "COL" registered, actor NOT granted.
    let policy = NacDacPolicy::new(/* dac: deny COL, allow nothing */, lookup_branchable(true));
    let decision = policy.check_release(Some(&actor()), &doc_scope("d1")).await.unwrap();
    assert_eq!(decision, PolicyDecision::Deny); // branchable collection gate denies
}

#[tokio::test]
async fn explicit_doc_grant_allows_release() {
    // doc "d1" registered to actor (explicit grant) => allow regardless of collection.
    let policy = NacDacPolicy::new(/* dac: doc grant */, lookup_branchable(true));
    let decision = policy.check_release(Some(&actor()), &doc_scope("d1")).await.unwrap();
    assert_eq!(decision, PolicyDecision::Allow);
}

#[tokio::test]
async fn nonbranchable_uses_doc_only() {
    let policy = NacDacPolicy::new(/* dac: doc grant */, lookup_branchable(false));
    let decision = policy.check_release(Some(&actor()), &doc_scope("d1")).await.unwrap();
    assert_eq!(decision, PolicyDecision::Allow);
}
```

(The fake `DocumentACP` must implement `is_doc_registered` + `check_doc_access` consistently so the rule’s explicit/public branches are exercised — extend the existing `FakeDac` from a single `allow` bool to a small map of registered objects + granted (object,actor) pairs.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kms nac_dac_policy`
Expected: FAIL — no `is_branchable`; gate ignores the collection object.

- [ ] **Step 3: Add `is_branchable` + call the shared rule**

Add `pub is_branchable: bool` to `DocCollectionInfo`. In `check_release`'s `KeyScope::Document` arm, replace the direct `check_doc_access` with:

```rust
let actor_id: acp::Identity = actor.into();
let checker = acp::read_access::DirectChecker {
    acp: self.doc_acp.as_ref(), identity: &actor_id, node_did: None,
};
let allowed = acp::read_access::check_doc_read_access(
    &checker, &info.policy_id, &info.resource_name, &info.collection_id, info.is_branchable, doc_id,
).await.map_err(classify_acp_error)?;
Ok(if allowed { PolicyDecision::Allow } else { PolicyDecision::Deny })
```

- [ ] **Step 4: Update the producer + build**

Grep `impl DocCollectionLookup`; set `is_branchable` from the resolved `CollectionVersion.is_branchable`.
Run: `cargo build -p kms -p embedded`

- [ ] **Step 5: Verify**

Run: `cargo test -p kms`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/kms crates/embedded
git commit -m "feat(acp): A3 KMS key-gate uses shared branchable read rule"
```

---

## Task 8: A3 — `PeerIdentityResolver` (libp2p token + Iroh NodeId derivation)

**Files:**
- Create: `crates/p2p/src/peer_identity.rs`
- Modify: `crates/p2p/src/lib.rs`; libp2p impl wraps `host::handle::get_peer_identity`; Iroh impl derives `NodeId → did:key`
- Test: `crates/p2p/src/peer_identity.rs` unit tests

**Interfaces:**
- Produces:
  ```rust
  #[async_trait]
  pub trait PeerIdentityResolver: Send + Sync {
      async fn resolve(&self, peer_id: &PeerId) -> Option<identity::Did>;
  }
  // Libp2pPeerIdentityResolver(handle): delegates to get_peer_identity().ok().flatten()
  // IrohPeerIdentityResolver: PeerId string is the ed25519 NodeId -> did:key
  ```

**Iroh note:** the Iroh `PeerId` is the ed25519 NodeId string (`transport.rs:44` `PeerId::new(node_id.to_string())`). Derive the DID from the 32-byte ed25519 public key via the `identity` crate's did:key derivation. This is verifiable (Iroh's QUIC handshake authenticates the NodeId) and needs no exchange protocol. It resolves the peer's **node** DID; ACP-over-Iroh deployments therefore bind the node's DefraDB identity to its ed25519 endpoint key. If the bytes can't be parsed as an ed25519 did:key (e.g. a non-ed25519 node identity), return `None` (→ Anonymous), a documented Iroh constraint.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn iroh_resolver_derives_did_from_node_id() {
    let (node_id_str, expected_did) = ed25519_node_id_and_did();
    let r = IrohPeerIdentityResolver::default();
    assert_eq!(r.resolve(&PeerId::new(node_id_str)).await, Some(expected_did));
}

#[tokio::test]
async fn libp2p_resolver_delegates_to_handle() {
    let r = make_libp2p_resolver_with(known_peer(), known_did());
    assert_eq!(r.resolve(&known_peer()).await, Some(known_did()));
    assert_eq!(r.resolve(&unknown_peer()).await, None);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p p2p peer_identity`
Expected: FAIL — trait/impls not found.

- [ ] **Step 3: Implement trait + both resolvers**

Define the trait. `Libp2pPeerIdentityResolver` holds a `P2PHostHandle` and calls `get_peer_identity(peer_id).await.ok().flatten()`. `IrohPeerIdentityResolver::resolve` parses the `PeerId` string as an `iroh::PublicKey`/`EndpointId`, takes its 32 ed25519 bytes, and derives a `did:key` `identity::Did` (add an `identity::Did::from_ed25519_public_key(&[u8;32])` helper if absent, using the existing did:key derivation). Add `pub mod peer_identity;` + re-exports in `lib.rs`.

- [ ] **Step 4: Verify**

Run: `cargo test -p p2p peer_identity`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/p2p/src/peer_identity.rs crates/p2p/src/lib.rs crates/identity/src
git commit -m "feat(p2p): PeerIdentityResolver (libp2p token + Iroh NodeId did:key)"
```

---

## Task 9: A3 — late-bound `BlockReadGate` + Bitswap per-block serve gate

**Files:**
- Create: `crates/p2p/src/bitswap/read_gate.rs` (`BlockReadGate` trait + `LateBoundReadGate` holder)
- Modify: `crates/p2p/src/bitswap/filter.rs:62-145`, `crates/p2p/src/behaviour.rs:292`
- Modify: node assembly (`crates/embedded/src/node.rs:553-558` wiring) to install the gate via the existing `wire_document_acp` callback
- Test: `crates/p2p/tests/bitswap_acp_filter_tests.rs`

**Interfaces:**
- Produces:
  ```rust
  #[async_trait]
  pub trait BlockReadGate: Send + Sync {
      /// Resolve the block's stable collection_id + doc id(s), then apply the read
      /// rule for `identity`. Returns (collection_id, allow). collection_id lets the
      /// caller do replicator passthrough keyed on the STABLE id (finding #2).
      async fn evaluate(&self, identity: &acp::Identity, block: &DefraBlock)
          -> Option<(String /*collection_id*/, bool /*allow*/)>;
  }

  /// OnceLock-backed holder installed at filter-construction; fail-closed until set.
  pub struct LateBoundReadGate(std::sync::OnceLock<Arc<dyn BlockReadGate>>);
  ```

**Context (finding #3):** the bitswap filter is built at `behaviour.rs:292` before `document_acp` exists. So the filter takes a `LateBoundReadGate` (empty), and the node assembly populates it inside the existing `wire_document_acp` callback (`node.rs:557`). Until populated, the gate denies non-replicator/non-public access (fail-closed).
**Context (finding #2):** never use the raw `schema_version_id` for replicator checks. The gate returns the block's **stable `collection_id`**; the filter uses THAT for `registry.is_replicator`.

- [ ] **Step 1: Write the failing tests**

In `crates/p2p/tests/bitswap_acp_filter_tests.rs`: (a) non-replicator peer whose DID lacks collection access requests a branchable data block → deny; (b) replicator **for that block's collection** → allow (passthrough); (c) peer that is a replicator of a *different* collection → still gated (deny if no access) — guards finding #2; (d) unresolved DID + public/unregistered block → allow (Anonymous); + registered block → deny; (e) gate not yet installed → deny non-replicator.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p p2p --test bitswap_acp_filter_tests`
Expected: FAIL.

- [ ] **Step 3: Rewrite `check_access`**

Keep the signature/definition/lens passthroughs. Then:

```rust
// resolve the read gate's verdict (also yields the STABLE collection_id).
let identity = match resolver.resolve(peer_id).await {
    Some(did) => acp::Identity::Authenticated(did),
    None => acp::Identity::Anonymous,
};
let Some((collection_id, allow)) = read_gate.evaluate(&identity, &defra_block).await else {
    // gate not installed yet => fail closed for non-replicators.
    return false;
};
let peer_str = peer_id.to_string();
// Replicator passthrough keyed on the STABLE collection_id (finding #2).
if registry.is_filtered_replicator(&collection_id, &peer_str) { return false; }
if registry.is_replicator(&collection_id, &peer_str) { return true; }
allow
```

Thread `resolver: Arc<dyn PeerIdentityResolver>` and `read_gate: Arc<LateBoundReadGate>` through `make_peer_block_access_filter` + the closure. `behaviour.rs:292` constructs the `LateBoundReadGate` (empty) and passes a default `resolver` appropriate to the transport (libp2p resolver from the host handle).

- [ ] **Step 4: Implement `BlockReadGate` + install via `wire_document_acp`**

Implement the gate in the node/db layer: decode is already done (pass `&DefraBlock`); resolve `schema_version_id → CollectionVersion`; compute the block's docID(s) (collection-level => `""`); return `(collection.collection_id, check_doc_read_access(DirectChecker{...}) for any owner doc)`. Install it into the `LateBoundReadGate` inside the `wire_document_acp` closure at `crates/embedded/src/node.rs:557` (and the `defra-node` equivalent).

- [ ] **Step 5: Verify**

Run: `cargo test -p p2p --test bitswap_acp_filter_tests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/p2p/src/bitswap crates/p2p/src/behaviour.rs crates/embedded/src/node.rs crates/defra-node/src crates/p2p/tests/bitswap_acp_filter_tests.rs
git commit -m "feat(p2p): A3 bitswap per-block serve gate (late-bound, stable-collection passthrough)"
```

---

## Task 10: A3 — CAR handler per-block serve filtering

**Files:**
- Modify: `crates/p2p/src/sync/coordinator/event_handler/car.rs:83-179`
- Modify: `SyncCoordinator` construction to inject `PeerIdentityResolver` + `BlockReadGate`
- Test: CAR test module under `crates/p2p/src/sync/coordinator/`

**Context (finding #2):** do NOT use `is_any_replicator`. Filter each block, doing replicator passthrough per the block's **stable collection_id** returned by the gate.

- [ ] **Step 1: Write the failing test**

Branchable collection, private doc. A connected non-replicator peer issues a CAR fetch → response excludes blocks it can't read; a replicator **for that collection** → full DAG; a peer that is a replicator of a *different* collection → still gated.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p car_fetch`
Expected: FAIL — CAR serves the full DAG.

- [ ] **Step 3: Filter response blocks per block**

After `let blocks = collected.blocks;` (car.rs:133), before the empty check:

```rust
let identity = match self.peer_identity_resolver.resolve(&peer_id).await {
    Some(did) => acp::Identity::Authenticated(did),
    None => acp::Identity::Anonymous,
};
let peer_str = peer_id.to_string();
let mut kept = Vec::with_capacity(blocks.len());
for (cid, data) in blocks {
    let allow = match DefraBlock::from_dag_cbor(&data) {
        Ok(b) => match self.read_gate.evaluate(&identity, &b).await {
            None => false, // gate not installed => fail closed
            Some((collection_id, rule_allow)) => {
                self.access.replicators.is_replicator(&collection_id, &peer_str)
                    && !self.access.replicators.is_filtered_replicator(&collection_id, &peer_str)
                    || rule_allow
            }
        },
        Err(_) => true, // signature / lens / non-CRDT — passthrough (matches bitswap)
    };
    if allow { kept.push((cid, data)); }
}
let blocks = kept;
```

Hold `peer_identity_resolver: Arc<dyn PeerIdentityResolver>` + `read_gate: Arc<LateBoundReadGate>` on `SyncCoordinator` (inject alongside `authorizer`; populate the gate via the same `wire_document_acp` path).

- [ ] **Step 4: Verify**

Run: `cargo test -p p2p car_fetch`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/p2p/src/sync/coordinator
git commit -m "feat(p2p): A3 CAR per-block serve filtering (stable-collection passthrough)"
```

---

## Task 11: CID-integrity regression coverage (already implemented)

**Files:**
- Test: `crates/p2p/src/sync/manager/process/pushlog.rs` tests + CAR test module

**Context:** `verify_block_cid` already rejects content/CID mismatch (pushlog.rs:235; CAR ingest). No production change — lock it in.

- [ ] **Step 1: Write the tests**

Pushlog: a `PushLogMessage` whose `block` bytes don't hash to `cid` → handler returns the CID-verify error and does not store. CAR: a CARv1 with a mismatched block → rejected on ingest.

- [ ] **Step 2: Verify**

Run: `cargo test -p p2p verify_block_cid`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/p2p/src/sync
git commit -m "test(p2p): regression cover block CID-integrity on pushlog/CAR"
```

---

## Task 12: Cross-implementation integration scenarios

**Files:**
- Modify: `tools/integration-test/tests/acp.rs` (`branchable_commits`, `branchable_peer`)

**Context:** port Go #4990's `collection_commits_test.go` + `peer_test.go`; run rust↔rust, go↔go, go↔rust.

- [ ] **Step 1: Write the serve-boundary scenario**

Node A (owner) hosts a branchable `@policy` collection with a private doc; node B connects as a **non-replicator** and tries to fetch → must NOT receive private blocks; grant B reader → receives. Variant: B as **replicator** → receives (passthrough).

- [ ] **Step 2: Run rust↔rust**

Run: `cargo test -p integration-test --test acp -- branchable_peer::`
Expected: PASS.

- [ ] **Step 3: Run go↔rust matrix**

Run with a Go defradb at `GO_COMPAT_COMMIT` on PATH (see `reference_go_parity_binary`), Go-producer/Rust-consumer and vice-versa. Expected: identical withhold/serve behavior.

- [ ] **Step 4: Commit**

```bash
git add tools/integration-test/tests
git commit -m "test(acp): cross-impl branchable read + serve-boundary parity"
```

---

## Task 13: Adversarial re-audit of the A3 serve boundary (merge gate)

**Files:** none (verification)

- [ ] **Step 1: Regression gate (spec top risk)**

Run:
```bash
cargo test -p integration-test --test p2p
cargo test -p integration-test --test p2p_iroh
cargo test -p integration-test --test encryption
cargo test -p integration-test --test identity
```
Expected: PASS. Any regression here is a **blocker** → revisit serve-scope per the spec.

- [ ] **Step 2: Adversarial review (ultracode Workflow)**

Scope to the A3 diff (Tasks 8–10) + merge handler vs Go `3f627855` `hasAccess`/`trySelfHasAccess`. Hunt: private branchable block served to a non-replicator without access; unresolved-DID granting more than Anonymous; recursive-vs-exact CAR asymmetry; a transport skipping the gate; `is_any_replicator`/`schema_version_id` passthrough leaks (finding #2); block→docID resolution missing collection-level blocks; the `LateBoundReadGate` serving before initialization. Resolve or explicitly accept every CONFIRMED/PLAUSIBLE finding.

- [ ] **Step 3: Update PR body**

Change the PR description from "A1 registration" to the full A1–A3 security feature; note the cross-impl matrix, the Iroh NodeId-derivation constraint, and the adversarial audit outcome.

---

## Self-Review

**Spec coverage:** rule once in `acp` (Tasks 1,3 — direct + overlay checkers) ✔; node_did (Tasks 2,4) ✔; version→collection_id at every site (Tasks 5,6,9,10) ✔; commits (5) ✔; verify (6) ✔; KMS is_branchable + corrected semantics (7) ✔; peer-DID resolver, libp2p + Iroh-native (8) ✔; bitswap late-bound serve gate + stable-collection passthrough (9) ✔; CAR serve filter, no is_any_replicator (10) ✔; unresolved-DID→Anonymous (9,10) ✔; pushlog CID already-done (11) ✔; cross-impl (12) ✔; regression gate + adversarial audit (13) ✔.

**Review findings closed:** #1 KMS test semantics (Task 7 step 1: public-doc+deny → Deny, explicit-grant → Allow) ✔; #2 per-block stable-collection passthrough (Tasks 9,10; Global Constraints) ✔; #3 late-bound `LateBoundReadGate` via `wire_document_acp` (Task 9) ✔; #4 Iroh NodeId→did:key derivation + documented constraint (Task 8) ✔; #5 single rule in `acp`, two checker impls (Tasks 1,3,7 all call `acp::read_access::check_doc_read_access`) ✔; #6 explicit `node_did` field + builder + wiring on the query runner (Task 4) ✔.

**Placeholders:** none — every code step shows real code; the `BlockReadGate`/`DocCollectionLookup` producer impls reference the existing block→docID and version→collection resolution rather than re-deriving, which is concrete guidance.

**Type consistency:** `check_doc_read_access(checker, policy_id, resource_name, collection_id, is_branchable, doc_id)` used identically in Tasks 1,5,6,7; `ObjectAccessChecker::object_access(policy_id, resource_name, object_id) -> DocAccess` consistent across `DirectChecker` (1) and `OverlayChecker` (3); `DocAccess { has_access, explicit }` consistent; `PeerIdentityResolver::resolve -> Option<Did>` consistent (8,9,10); `BlockReadGate::evaluate -> Option<(String, bool)>` consistent (9,10); `DocCollectionInfo.is_branchable` added once (7).
