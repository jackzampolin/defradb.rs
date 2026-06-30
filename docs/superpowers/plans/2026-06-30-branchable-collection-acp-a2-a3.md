# Branchable Collection ACP — A2 + A3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce branchable-collection ACP on local read paths (A2) and the P2P serve boundary (A3), consuming the A1 collection-object registration, at full parity with Go v1.0.0 `3f627855`.

**Architecture:** One new rule, `check_doc_read_access`, with an internal `DocAccess { has_access, explicit }` verdict, defined once over an `ObjectAccessChecker` capability (overlay + direct impls) and wired into five sites: commits query, signature verify, KMS key-gate, Bitswap serve filter, CAR serve handler. A transport-agnostic `PeerIdentityResolver` supplies the requesting peer's DID at the serve boundary.

**Tech Stack:** Rust, async-trait, tokio; crates `acp`, `db`, `query`, `query-plan`, `kms`, `p2p`; integration harness in `tools/integration-test`.

## Global Constraints

- Parity oracle: Go v1.0.0 commit `3f627855`. Behavior must match unless a deviation is explicitly recorded in the spec.
- ACP objects are keyed by the **stable `collection_id`**, never `schema_version_id`. Every site resolves `schema_version_id → CollectionVersion` first.
- Spec: `docs/superpowers/specs/2026-06-30-branchable-collection-acp-a2-a3-design.md`.
- `cargo clippy --all -- -D warnings` must stay clean; `cargo fmt --all` applied before each commit.
- Unresolved peer DID at the serve boundary → treat as `Identity::Anonymous` (Go parity), not blanket-deny.
- Replicator passthrough at the serve boundary is preserved (registered replicators served without per-doc check).
- Do NOT flip the `strict_replicated_doc_access` merge default; merge gating stays as-is.
- Commit after each task. Keep A2 and A3 in separate commits. The A3 serve-path work requires an adversarial re-audit (Task 13) before the PR merges.

---

## File Structure

- `crates/acp/src/error.rs` — (existing) reused error variants.
- `crates/db/src/collection_acp.rs` — home of `DocAccess`, `check_doc_access_ext`, `check_doc_read_access`, `ObjectAccessChecker` + direct impl. **Primary new logic.**
- `crates/db/tests/collection_acp_tests.rs` — unit truth-table for the rule.
- `crates/query-plan/src/txn/context.rs` — overlay `ObjectAccessChecker` impl; thread `node_did`.
- `crates/query/src/runner/commits.rs` — A2 commits gating.
- `crates/db/src/block_verify.rs` — A2 signature-verify gating.
- `crates/kms/src/policy.rs` — add `is_branchable` to `DocCollectionInfo`.
- `crates/kms/src/nac_dac_policy.rs` — KMS gate uses the read rule.
- `crates/p2p/src/transport.rs` + `crates/p2p/src/peer_identity.rs` (new) — `PeerIdentityResolver`.
- `crates/p2p/src/bitswap/filter.rs` — Bitswap per-doc serve gate.
- `crates/p2p/src/sync/coordinator/event_handler/car.rs` — CAR per-block serve filtering.
- `tools/integration-test/tests/acp.rs` (+ submodules) — cross-impl integration scenarios.

---

## Task 1: `DocAccess` verdict + explicit-reporting single-object check

**Files:**
- Modify: `crates/db/src/collection_acp.rs` (add after `check_doc_permission`, ~line 84)
- Test: `crates/db/tests/collection_acp_tests.rs`

**Interfaces:**
- Consumes: `acp::{DocumentACP, DocumentPermission, Identity}`, `identity::Did`, `schema::CollectionVersion`, existing `defra_core::dac_bypass::get_dac_bypass()`.
- Produces:
  ```rust
  pub struct DocAccess { pub has_access: bool, pub explicit: bool }
  // Single-object check that also reports whether the verdict was explicit.
  pub async fn check_object_access(
      acp: &dyn DocumentACP,
      identity: &Identity,
      permission: DocumentPermission,
      policy_id: &str,
      resource_name: &str,
      object_id: &str,
      node_identity: Option<&Did>,
  ) -> acp::Result<DocAccess>;
  ```

- [ ] **Step 1: Write the failing test**

In `crates/db/tests/collection_acp_tests.rs` add:

```rust
use db::collection_acp::{check_object_access, DocAccess};

#[tokio::test]
async fn check_object_access_reports_explicit() {
    let store = std::sync::Arc::new(MemoryAcpStore::new());
    let acp = LocalDocumentACP::new(store);
    let owner = test_did();
    let policy = ("polid".to_string(), "users".to_string());

    // Unregistered object => public, not explicit.
    let pub_access = check_object_access(
        &acp, &Identity::Anonymous, DocumentPermission::Read,
        &policy.0, &policy.1, "objX", None,
    ).await.unwrap();
    assert_eq!(pub_access.has_access, true);
    assert_eq!(pub_access.explicit, false);

    // Registered to owner => owner has explicit access.
    acp.register_doc_object(&owner, &policy.0, &policy.1, "objX").await.unwrap();
    let owner_access = check_object_access(
        &acp, &Identity::Authenticated(owner.clone()), DocumentPermission::Read,
        &policy.0, &policy.1, "objX", None,
    ).await.unwrap();
    assert_eq!(owner_access.has_access, true);
    assert_eq!(owner_access.explicit, true);

    // Anonymous against a registered object => explicit denial.
    let anon = check_object_access(
        &acp, &Identity::Anonymous, DocumentPermission::Read,
        &policy.0, &policy.1, "objX", None,
    ).await.unwrap();
    assert_eq!(anon.has_access, false);
    assert_eq!(anon.explicit, true);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p db --test collection_acp_tests check_object_access_reports_explicit`
Expected: FAIL — `check_object_access` / `DocAccess` not found.

- [ ] **Step 3: Implement `DocAccess` + `check_object_access`**

In `crates/db/src/collection_acp.rs`:

```rust
/// Outcome of a single-object access check: the verdict plus whether it was
/// decided by something specific to the actor (an ACP registration or a
/// DAC-bypass / node-identity grant) vs. unrestricted-for-everyone access.
/// Mirrors Go `internal/db/acp/check.go::docAccess`.
#[derive(Debug, Clone, Copy)]
pub struct DocAccess {
    pub has_access: bool,
    pub explicit: bool,
}

/// Single-object access check that also reports `explicit`. Used as the
/// building block of [`check_doc_read_access`]. `policy_id`/`resource_name`
/// come from the collection's policy; `object_id` is a docID or a collection_id.
pub async fn check_object_access(
    acp: &dyn DocumentACP,
    identity: &Identity,
    permission: DocumentPermission,
    policy_id: &str,
    resource_name: &str,
    object_id: &str,
    node_identity: Option<&Did>,
) -> acp::Result<DocAccess> {
    if defra_core::dac_bypass::get_dac_bypass() {
        return Ok(DocAccess { has_access: true, explicit: true });
    }
    if let (Some(node_did), Identity::Authenticated(req_did)) = (node_identity, identity) {
        if node_did == req_did {
            return Ok(DocAccess { has_access: true, explicit: true });
        }
    }
    if !acp.is_doc_registered(policy_id, resource_name, object_id).await? {
        return Ok(DocAccess { has_access: true, explicit: false });
    }
    let has_access = acp
        .check_doc_access(identity, permission, policy_id, resource_name, object_id)
        .await?;
    Ok(DocAccess { has_access, explicit: true })
}
```

Ensure `pub mod collection_acp;` re-exports are present (the crate already exposes `check_doc_permission`; add `DocAccess` and the new fn to the same `pub use` if one exists in `crates/db/src/lib.rs`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p db --test collection_acp_tests check_object_access_reports_explicit`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/db/src/collection_acp.rs crates/db/src/lib.rs crates/db/tests/collection_acp_tests.rs
git commit -m "feat(acp): add DocAccess + explicit-reporting object access check"
```

---

## Task 2: `check_doc_read_access` rule + direct backend

**Files:**
- Modify: `crates/db/src/collection_acp.rs`
- Test: `crates/db/tests/collection_acp_tests.rs`

**Interfaces:**
- Consumes: `check_object_access`, `DocAccess` (Task 1), `schema::CollectionVersion`.
- Produces:
  ```rust
  pub async fn check_doc_read_access(
      acp: &dyn DocumentACP,
      identity: &Identity,
      collection: &CollectionVersion,
      doc_id: &str,                 // "" for a collection-level commit
      node_identity: Option<&Did>,
  ) -> acp::Result<bool>;
  ```

- [ ] **Step 1: Write the failing tests** (truth table)

In `crates/db/tests/collection_acp_tests.rs`:

```rust
use db::collection_acp::check_doc_read_access;

#[tokio::test]
async fn read_access_branchable_public_doc_requires_collection() {
    let acp = LocalDocumentACP::new(std::sync::Arc::new(MemoryAcpStore::new()));
    let owner = test_did();
    let stranger = test_did2();
    let col = branchable_collection_with_policy(); // is_branchable + policy
    let policy = col.policy.clone().unwrap();

    // Register the collection object to owner; leave the doc public (unregistered).
    acp.register_doc_object(&owner, &policy.id, &policy.resource_name, &col.collection_id)
        .await.unwrap();

    // Owner: public doc + collection access => GRANT.
    assert!(check_doc_read_access(
        &acp, &Identity::Authenticated(owner.clone()), &col, "docA", None,
    ).await.unwrap());

    // Stranger: public doc but NO collection access => DENY (branchable gates public docs).
    assert!(!check_doc_read_access(
        &acp, &Identity::Authenticated(stranger.clone()), &col, "docA", None,
    ).await.unwrap());

    // Collection-level commit (empty doc_id): gated purely on collection object.
    assert!(check_doc_read_access(
        &acp, &Identity::Authenticated(owner), &col, "", None,
    ).await.unwrap());
    assert!(!check_doc_read_access(
        &acp, &Identity::Authenticated(stranger), &col, "", None,
    ).await.unwrap());
}

#[tokio::test]
async fn read_access_explicit_doc_grant_overrides_collection() {
    let acp = LocalDocumentACP::new(std::sync::Arc::new(MemoryAcpStore::new()));
    let owner = test_did();
    let reader = test_did2();
    let col = branchable_collection_with_policy();
    let policy = col.policy.clone().unwrap();

    // Collection owned by owner (reader has no collection access).
    acp.register_doc_object(&owner, &policy.id, &policy.resource_name, &col.collection_id)
        .await.unwrap();
    // Doc registered to reader explicitly (reader IS the doc owner).
    acp.register_doc_object(&reader, &policy.id, &policy.resource_name, "docShared")
        .await.unwrap();

    // Reader has an explicit grant on docShared => GRANT despite no collection access.
    assert!(check_doc_read_access(
        &acp, &Identity::Authenticated(reader), &col, "docShared", None,
    ).await.unwrap());
}

#[tokio::test]
async fn read_access_nonbranchable_reduces_to_doc() {
    let acp = LocalDocumentACP::new(std::sync::Arc::new(MemoryAcpStore::new()));
    let owner = test_did();
    let stranger = test_did2();
    let col = collection_with_policy(); // permissioned, NOT branchable
    let policy = col.policy.clone().unwrap();
    acp.register_doc_object(&owner, &policy.id, &policy.resource_name, "docA")
        .await.unwrap();

    assert!(check_doc_read_access(
        &acp, &Identity::Authenticated(owner), &col, "docA", None,
    ).await.unwrap());
    assert!(!check_doc_read_access(
        &acp, &Identity::Authenticated(stranger), &col, "docA", None,
    ).await.unwrap());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p db --test collection_acp_tests read_access_`
Expected: FAIL — `check_doc_read_access` not found.

- [ ] **Step 3: Implement the rule**

In `crates/db/src/collection_acp.rs`:

```rust
/// Reports whether `identity` may read the object identified by `doc_id` on
/// `collection`. Gates the object's entire commit DAG, not just current field
/// values. `doc_id` is "" for a collection-level commit (branchable only).
///
/// GRANT if EITHER an explicit grant on the document, OR read access to every
/// object the document relates to: the document itself and — for a branchable
/// collection — the collection object (keyed by collection_id). Explicit denial
/// on the document always denies. Mirrors Go
/// `internal/db/acp/check.go::CheckDocReadAccessWithIdentityFunc`.
pub async fn check_doc_read_access(
    acp: &dyn DocumentACP,
    identity: &Identity,
    collection: &CollectionVersion,
    doc_id: &str,
    node_identity: Option<&Did>,
) -> acp::Result<bool> {
    // Unpermissioned collection => unrestricted (matches check_object_access's
    // public path, but short-circuit here to avoid needing a policy below).
    let Some(policy) = &collection.policy else {
        return Ok(true);
    };

    let mut doc_accessible = true;
    if !doc_id.is_empty() {
        let access = check_object_access(
            acp, identity, DocumentPermission::Read,
            &policy.id, &policy.resource_name, doc_id, node_identity,
        ).await?;
        if access.explicit && access.has_access {
            return Ok(true); // explicit grant on the document is sufficient
        }
        if !access.has_access {
            return Ok(false); // explicit denial on the document always wins
        }
        doc_accessible = access.has_access;
    }
    if !doc_accessible {
        return Ok(false);
    }

    if collection.is_branchable {
        let access = check_object_access(
            acp, identity, DocumentPermission::Read,
            &policy.id, &policy.resource_name, &collection.collection_id, node_identity,
        ).await?;
        if !access.has_access {
            return Ok(false);
        }
    }
    Ok(true)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p db --test collection_acp_tests read_access_`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/db/src/collection_acp.rs crates/db/tests/collection_acp_tests.rs
git commit -m "feat(acp): add check_doc_read_access branchable read rule"
```

---

## Task 3: Thread `node_did` into the overlay access check

**Files:**
- Modify: `crates/query-plan/src/txn/context.rs:328` (`check_doc_access_with_overlay`)
- Modify: call sites in `crates/query-plan/src/plan/permission_filter.rs` and `crates/query/src/runner/commits.rs`
- Test: `crates/query-plan/tests/cross_object_read_path.rs`

**Interfaces:**
- Produces (changed signature):
  ```rust
  pub async fn check_doc_access_with_overlay(
      acp: &dyn DocumentACP,
      identity: &Identity,
      permission: DocumentPermission,
      policy_id: &str,
      resource_name: &str,
      doc_id: &str,
      node_did: Option<&Did>,        // NEW trailing param
  ) -> AcpResult<bool>;
  ```

- [ ] **Step 1: Write the failing test**

In `crates/query-plan/tests/cross_object_read_path.rs`, add a case asserting the node identity is granted access to a registered object it does not own:

```rust
#[tokio::test]
async fn overlay_grants_node_identity_full_access() {
    // build acp with object registered to some other owner; node_did = NODE.
    let (acp, node_did, other_owner) = setup_registered_object().await;
    let granted = query_plan::txn::check_doc_access_with_overlay(
        acp.as_ref(),
        &acp::Identity::Authenticated(node_did.clone()),
        acp::DocumentPermission::Read,
        "polid", "users", "objX",
        Some(&node_did),
    ).await.unwrap();
    assert!(granted, "node identity must get full access via the shortcut");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p query-plan --test cross_object_read_path overlay_grants_node_identity_full_access`
Expected: FAIL — arity mismatch (function takes 6 args) / not granted.

- [ ] **Step 3: Add the `node_did` shortcut to the overlay check**

In `crates/query-plan/src/txn/context.rs`, change the signature and add the shortcut before the projected-registration logic:

```rust
pub async fn check_doc_access_with_overlay(
    acp: &dyn DocumentACP,
    identity: &Identity,
    permission: DocumentPermission,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
    node_did: Option<&Did>,
) -> AcpResult<bool> {
    if let (Some(node), Identity::Authenticated(req)) = (node_did, identity) {
        if node == req {
            return Ok(true);
        }
    }
    if let Some(mutations) = current_deferred_acp_mutations() {
        // ... unchanged projected-registration block ...
    }
    acp.check_doc_access(identity, permission, policy_id, resource_name, doc_id).await
}
```

Update the internal recursive calls at context.rs:448/463/491 to pass `node_did` through.

- [ ] **Step 4: Update other call sites**

In `crates/query-plan/src/plan/permission_filter.rs` and any other caller, pass the node DID where available (thread it from the planner/registry; pass `None` where not yet plumbed — behavior unchanged there). Compile to find all call sites:

Run: `cargo build -p query-plan -p query 2>&1 | grep "this function takes"`
Fix each by adding the trailing `node_did` argument.

- [ ] **Step 5: Run test + build to verify**

Run: `cargo test -p query-plan --test cross_object_read_path overlay_grants_node_identity_full_access && cargo build -p query`
Expected: PASS + clean build.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/query-plan crates/query
git commit -m "feat(acp): thread node_did into overlay access check"
```

---

## Task 4: Overlay `ObjectAccessChecker` + read rule over overlay

**Files:**
- Create: `crates/query-plan/src/txn/read_access.rs`
- Modify: `crates/query-plan/src/txn/mod.rs` (re-export)
- Test: `crates/query-plan/tests/cross_object_read_path.rs`

**Interfaces:**
- Consumes: `check_doc_access_with_overlay` (Task 3, with `node_did`), `is_doc_registered_with_overlay`.
- Produces:
  ```rust
  // Overlay-aware read rule used by the commits query.
  pub async fn check_doc_read_access_overlay(
      acp: &dyn DocumentACP,
      identity: &Identity,
      collection: &CollectionVersion,
      doc_id: &str,
      node_did: Option<&Did>,
  ) -> AcpResult<bool>;
  ```

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn overlay_read_rule_gates_branchable_public_doc() {
    let (acp, owner, stranger, col) = setup_branchable_registered_collection().await;
    assert!(query_plan::txn::check_doc_read_access_overlay(
        acp.as_ref(), &Identity::Authenticated(owner), &col, "docA", None,
    ).await.unwrap());
    assert!(!query_plan::txn::check_doc_read_access_overlay(
        acp.as_ref(), &Identity::Authenticated(stranger), &col, "docA", None,
    ).await.unwrap());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p query-plan --test cross_object_read_path overlay_read_rule_gates_branchable_public_doc`
Expected: FAIL — function not found.

- [ ] **Step 3: Implement the overlay read rule**

In `crates/query-plan/src/txn/read_access.rs`, replicate the Task-2 algorithm but call the overlay-aware `is_doc_registered_with_overlay` + `check_doc_access_with_overlay` for each object. Factor the per-object verdict into a local helper mirroring `check_object_access`:

```rust
async fn object_access_overlay(
    acp: &dyn DocumentACP, identity: &Identity, policy_id: &str, resource_name: &str,
    object_id: &str, node_did: Option<&Did>,
) -> AcpResult<(bool /*has*/, bool /*explicit*/)> {
    if let (Some(node), Identity::Authenticated(req)) = (node_did, identity) {
        if node == req { return Ok((true, true)); }
    }
    if !is_doc_registered_with_overlay(acp, policy_id, resource_name, object_id).await? {
        return Ok((true, false));
    }
    let has = check_doc_access_with_overlay(
        acp, identity, DocumentPermission::Read, policy_id, resource_name, object_id, node_did,
    ).await?;
    Ok((has, true))
}

pub async fn check_doc_read_access_overlay(
    acp: &dyn DocumentACP, identity: &Identity, collection: &CollectionVersion,
    doc_id: &str, node_did: Option<&Did>,
) -> AcpResult<bool> {
    let Some(policy) = &collection.policy else { return Ok(true); };
    if !doc_id.is_empty() {
        let (has, explicit) = object_access_overlay(
            acp, identity, &policy.id, &policy.resource_name, doc_id, node_did).await?;
        if explicit && has { return Ok(true); }
        if !has { return Ok(false); }
    }
    if collection.is_branchable {
        let (has, _) = object_access_overlay(
            acp, identity, &policy.id, &policy.resource_name,
            &collection.collection_id, node_did).await?;
        if !has { return Ok(false); }
    }
    Ok(true)
}
```

Add `pub mod read_access;` and re-export `check_doc_read_access_overlay` in `crates/query-plan/src/txn/mod.rs`, and re-export through `crates/query/src/txn/mod.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p query-plan --test cross_object_read_path overlay_read_rule_gates_branchable_public_doc`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/query-plan crates/query
git commit -m "feat(acp): overlay-aware check_doc_read_access for query path"
```

---

## Task 5: A2 — gate the commits query

**Files:**
- Modify: `crates/query/src/runner/commits.rs:737-839` (the ACP filtering block in `execute_commits_query`)
- Test: `tools/integration-test/tests/acp.rs` (new module `branchable_commits`)

**Interfaces:**
- Consumes: `check_doc_read_access_overlay` (Task 4), `self.collections_map()`, `caller_identity`, `self.node_did()` (add accessor if absent — the registry already holds `db.node_did()`).

**Context:** Today the block at commits.rs:771-775 does `docID None => continue`, so collection-level commits are never gated; doc commits are gated per-doc via `check_doc_access_with_overlay` against the version's policy. Replace both with the read rule keyed on the resolved `CollectionVersion`.

- [ ] **Step 1: Write the failing integration test**

In `tools/integration-test/tests/acp.rs`, add `mod branchable_commits;` and create the scenario (driving the real node via the harness): a branchable `@policy` collection created by identity A; a public doc added; identity B (no collection access) queries `_commits` for both the collection-level DAG and the doc — expects **empty**; identity A expects the commits. Use the existing harness helpers (mirror `acp::basic`). Assert B sees `[]` and A sees non-empty.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p integration-test --test acp -- branchable_commits::`
Expected: FAIL — B currently sees collection-level commits (ungated).

- [ ] **Step 3: Rewrite the ACP filter block**

In `execute_commits_query`, replace the per-doc-only filter with a per-commit read-rule check. Resolve each commit's `collectionVersionId` to its `CollectionVersion` (via `self.collections_map()` keyed by `version_id`) and call the rule; treat an empty/absent `docID` as a collection-level commit (`""`):

```rust
let node_did = self.node_did(); // Option<Did>
let identity = Identity::from(caller_identity.clone());
let collections = self.collections_map().await?;
let by_version: HashMap<String, CollectionVersion> = collections
    .values().map(|c| (c.version_id.clone(), c.clone())).collect();

let mut keep: Vec<bool> = Vec::with_capacity(commits.len());
for commit in &commits {
    let version_id = commit.get("collectionVersionId").and_then(|v| v.as_str());
    let doc_id = commit.get("docID").and_then(|v| v.as_str()).unwrap_or("");
    let allowed = match version_id.and_then(|v| by_version.get(v)) {
        None => true, // unknown version: no policy to enforce here
        Some(col) => match crate::txn::check_doc_read_access_overlay(
            self.acp.as_ref(), &identity, col, doc_id, node_did.as_ref(),
        ).await {
            Ok(granted) => granted,
            Err(e) => {
                tracing::debug!(target: "acp::audit", event = "commits_acp_check_error",
                    doc_id = %doc_id, error = %e, "denying commit on ACP error");
                false
            }
        },
    };
    keep.push(allowed);
}
let mut iter = keep.into_iter();
commits.retain(|_| iter.next().unwrap_or(false));
```

Remove the now-dead `policies_by_version` / `denied_commits` machinery (lines ~744-838) it replaces. Keep the `has_protected_collections` short-circuit if cheap, but the rule already no-ops unpermissioned collections.

- [ ] **Step 4: Run integration test to verify it passes**

Run: `cargo test -p integration-test --test acp -- branchable_commits::`
Expected: PASS — B sees `[]`, A sees commits.

- [ ] **Step 5: Run the full ACP + query suites (no regressions)**

Run: `cargo test -p integration-test --test acp && cargo test -p integration-test --test query -- commits`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/query/src/runner/commits.rs tools/integration-test/tests/acp.rs tools/integration-test/tests/acp/
git commit -m "feat(acp): A2 gate commits query on branchable read rule"
```

---

## Task 6: A2 — gate signature verification

**Files:**
- Modify: `crates/db/src/block_verify.rs:110-136`
- Test: `crates/db/tests/` (add a block-verify ACP test) or `tools/integration-test/tests/acp.rs::branchable_commits` extension.

**Interfaces:**
- Consumes: `check_doc_read_access` (Task 2), `database.get_collection_by_version_id`, `database.node_did()`.

**Context:** Today verify only checks when `delta.doc_id()` is `Some`; collection-level blocks bypass. Replace with the read rule, resolving the collection from `schema_version_id` and using `""` for collection-level blocks.

- [ ] **Step 1: Write the failing test**

Add a `crates/db/tests/block_verify_acp_tests.rs` test: build a DB with a branchable permissioned collection registered to owner A; produce a collection-level signed block; call `verify_block_signature` as stranger B → expect `Err("missing permission")`; as A → expect `Ok`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p db --test block_verify_acp_tests`
Expected: FAIL — B currently passes (collection-level block bypasses the gate).

- [ ] **Step 3: Replace the gate with the read rule**

In `verify_block_signature_with_blockstore`, replace lines 110-136:

```rust
// Read-access gate: a block's signature is only verifiable by an actor who can
// read the block's document(s) / collection DAG. Collection-level blocks have
// no docID and are gated on the collection object for a branchable collection.
if let Some(schema_version_id) = block.delta.schema_version_id() {
    if let Some(collection) = database
        .get_collection_by_version_id(schema_version_id)
        .map_err(|e| format!("failed to get collection: {}", e))?
    {
        let doc_id = block
            .delta
            .doc_id()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default();
        let node_did = database.node_did();
        let has_permission = crate::collection_acp::check_doc_read_access(
            document_acp,
            caller_identity,
            collection.schema(),
            &doc_id,
            node_did.as_ref(),
        )
        .await
        .map_err(|e| format!("ACP check failed: {}", e))?;
        if !has_permission {
            return Err("missing permission".to_string());
        }
    }
}
```

- [ ] **Step 4: Run test + ACP suite**

Run: `cargo test -p db --test block_verify_acp_tests && cargo test -p integration-test --test acp`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/db/src/block_verify.rs crates/db/tests/block_verify_acp_tests.rs
git commit -m "feat(acp): A2 gate signature verification on branchable read rule"
```

---

## Task 7: A3 — KMS key-gate uses the read rule

**Files:**
- Modify: `crates/kms/src/policy.rs:61` (`DocCollectionInfo`) and its `DocCollectionLookup`
- Modify: `crates/kms/src/nac_dac_policy.rs:80-111` (`check_release` Document arm)
- Modify: the `DocCollectionLookup` producer (in the embedded-node layer, "Phase K"): grep `impl DocCollectionLookup`
- Test: `crates/kms/src/nac_dac_policy.rs` tests (extend fakes)

**Interfaces:**
- Produces (changed struct):
  ```rust
  pub struct DocCollectionInfo {
      pub collection_id: String,
      pub policy_id: String,
      pub resource_name: String,
      pub is_branchable: bool,   // NEW
  }
  ```

- [ ] **Step 1: Write the failing test**

In `crates/kms/src/nac_dac_policy.rs` tests, extend the fake `collection_for_doc` to return `is_branchable: true` and have the fake DAC grant the doc but deny the collection object; assert `check_release` returns `Deny` (the branchable collection gate must apply). Add a second case `is_branchable: false` → `Allow` (doc-only).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p kms nac_dac_policy`
Expected: FAIL — struct has no `is_branchable`; gate ignores collection object.

- [ ] **Step 3: Add `is_branchable` and switch the gate**

Add the field to `DocCollectionInfo`. In `check_release`'s `KeyScope::Document` arm, replace the direct `check_doc_access` call with a read-rule check. Since KMS depends on `acp` (not `db`), inline the same algorithm using the `DocCollectionInfo` (it has policy_id/resource_name/collection_id/is_branchable):

```rust
let actor_id: acp::Identity = actor.into();
let read_allowed = kms_check_doc_read_access(
    self.doc_acp.as_ref(), &actor_id, &info, doc_id,
).await.map_err(classify_acp_error)?;
Ok(if read_allowed { PolicyDecision::Allow } else { PolicyDecision::Deny })
```

Add a small helper in `crates/kms/src/policy.rs`:

```rust
/// KMS-local read rule mirroring db::collection_acp::check_doc_read_access,
/// expressed over DocCollectionInfo (kms cannot depend on the db crate).
pub async fn kms_check_doc_read_access(
    acp: &dyn acp::DocumentACP,
    identity: &acp::Identity,
    info: &DocCollectionInfo,
    doc_id: &str,
) -> acp::Result<bool> {
    let object = |id: &str| async move {
        if !acp.is_doc_registered(&info.policy_id, &info.resource_name, id).await? {
            return Ok::<(bool, bool), acp::Error>((true, false));
        }
        let has = acp.check_doc_access(
            identity, acp::DocumentPermission::Read,
            &info.policy_id, &info.resource_name, id,
        ).await?;
        Ok((has, true))
    };
    if !doc_id.is_empty() {
        let (has, explicit) = object(doc_id).await?;
        if explicit && has { return Ok(true); }
        if !has { return Ok(false); }
    }
    if info.is_branchable {
        let (has, _) = object(&info.collection_id).await?;
        if !has { return Ok(false); }
    }
    Ok(true)
}
```

(No node-identity shortcut here: KMS release already resolves node-level access via `check_node_release`; the document arm matches Go's `pubsub.go` which calls `CheckDocReadAccess` without a separate node bypass.)

- [ ] **Step 4: Update the producer**

Grep `impl DocCollectionLookup` (embedded-node layer) and populate `is_branchable` from the resolved `CollectionVersion.is_branchable`.

Run: `cargo build -p kms -p embedded`

- [ ] **Step 5: Run tests**

Run: `cargo test -p kms`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/kms crates/embedded
git commit -m "feat(acp): A3 KMS key-gate honors branchable read rule"
```

---

## Task 8: A3 — `PeerIdentityResolver` abstraction

**Files:**
- Create: `crates/p2p/src/peer_identity.rs`
- Modify: `crates/p2p/src/lib.rs` (module + re-export), `crates/p2p/src/host/handle.rs` (impl for libp2p), Iroh endpoint (impl/wiring)
- Test: `crates/p2p/src/peer_identity.rs` unit test with a fake

**Interfaces:**
- Produces:
  ```rust
  #[async_trait]
  pub trait PeerIdentityResolver: Send + Sync {
      /// Resolve a transport peer id to a verified DefraDB DID, or None when
      /// unknown/unverifiable. Callers treat None as Anonymous (Go parity).
      async fn resolve(&self, peer_id: &PeerId) -> Option<identity::Did>;
  }
  ```

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn libp2p_resolver_delegates_to_handle() {
    // fake handle returning Some(did) for a known peer, None otherwise
    let resolver = make_test_resolver();
    assert_eq!(resolver.resolve(&known_peer()).await, Some(known_did()));
    assert_eq!(resolver.resolve(&unknown_peer()).await, None);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p peer_identity`
Expected: FAIL — trait not found.

- [ ] **Step 3: Define the trait + libp2p impl**

In `crates/p2p/src/peer_identity.rs` define the trait. Implement it for a wrapper holding a `P2PHostHandle`, delegating to `get_peer_identity(peer_id).await.ok().flatten()`. For Iroh, implement a wrapper over the Iroh endpoint's identity mechanism (or, if Iroh shares the `peer_identities` cache, expose an equivalent `get_peer_identity` and delegate). Add `pub mod peer_identity;` + re-export in `lib.rs`.

- [ ] **Step 4: Run test**

Run: `cargo test -p p2p peer_identity`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/p2p/src/peer_identity.rs crates/p2p/src/lib.rs crates/p2p/src/host crates/p2p/src/iroh
git commit -m "feat(p2p): add transport-agnostic PeerIdentityResolver"
```

---

## Task 9: A3 — Bitswap serve filter per-doc gate

**Files:**
- Modify: `crates/p2p/src/bitswap/filter.rs:62-145` (`check_access`, `make_peer_block_access_filter`)
- Test: `crates/p2p/tests/bitswap_acp_filter_tests.rs`

**Interfaces:**
- Consumes: `PeerIdentityResolver` (Task 8), `check_doc_read_access` (Task 2), a way to resolve a block's `schema_version_id → CollectionVersion` + its docID(s). The filter must be given a `ReadAccessContext` (DB/ACP handle) at construction. Define:
  ```rust
  #[async_trait]
  pub trait BlockReadGate: Send + Sync {
      // Resolve the block's collection + doc id(s) and apply check_doc_read_access
      // for `identity`. Empty doc id => collection-level. Returns true if ANY owner doc grants.
      async fn may_read_block(&self, identity: &acp::Identity, block: &DefraBlock) -> bool;
  }
  ```
  implemented in the db/embedded layer (it has DB + ACP). The filter holds `Arc<dyn BlockReadGate>` + `Arc<dyn PeerIdentityResolver>`.

- [ ] **Step 1: Write the failing test**

In `crates/p2p/tests/bitswap_acp_filter_tests.rs` add: non-replicator peer whose resolved DID lacks collection access requests a branchable data block → filter denies; replicator peer → allowed (passthrough); non-replicator with no resolvable DID requesting a public/unregistered block → allowed (Anonymous, Go parity); requesting a registered/private block → denied.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p --test bitswap_acp_filter_tests`
Expected: FAIL — filter has no read gate; non-replicator currently always denied.

- [ ] **Step 3: Extend `check_access`**

After the existing definition/lens/signature passthroughs and the replicator check, when the peer is NOT a replicator, resolve identity and apply the read gate instead of returning false:

```rust
// (unchanged) signature / definition / lens passthroughs ...
let peer_str = peer_id.to_string();
if registry.is_filtered_replicator(collection_id, &peer_str) { /* unchanged deny */ }
if registry.is_replicator(collection_id, &peer_str) {
    return true; // replicator passthrough (Go parity)
}
// Non-replicator: Go's hasAccess falls through to a per-doc read check using the
// requesting peer's resolved identity. Unresolved DID => Anonymous (Go parity).
let identity = match resolver.resolve(peer_id).await {
    Some(did) => acp::Identity::Authenticated(did),
    None => acp::Identity::Anonymous,
};
return read_gate.may_read_block(&identity, &defra_block).await;
```

Thread `resolver: Arc<dyn PeerIdentityResolver>` and `read_gate: Arc<dyn BlockReadGate>` through `make_peer_block_access_filter` and the closure (clone into the async block). Update `behaviour.rs:292` construction to pass them (the embedded/node assembly provides the `BlockReadGate` impl).

- [ ] **Step 4: Implement `BlockReadGate` in the db/embedded layer**

Where the filter is constructed (node assembly), build a gate that: decodes the block (already decoded by the filter — pass the `DefraBlock`), resolves `schema_version_id → CollectionVersion`, computes the block's docID(s) (collection-level => `""`), and returns true if `check_doc_read_access` grants for any. Reuse the existing block→docID resolution used elsewhere on the merge path.

- [ ] **Step 5: Run tests**

Run: `cargo test -p p2p --test bitswap_acp_filter_tests`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/p2p/src/bitswap crates/p2p/src/behaviour.rs crates/p2p/tests/bitswap_acp_filter_tests.rs
git commit -m "feat(p2p): A3 bitswap serve filter enforces branchable read rule"
```

---

## Task 10: A3 — CAR handler per-block serve filtering

**Files:**
- Modify: `crates/p2p/src/sync/coordinator/event_handler/car.rs:83-179` (`handle_car_fetch_request`)
- Test: `crates/p2p/src/sync/coordinator/access_tests.rs` (or a new CAR test module)

**Interfaces:**
- Consumes: `PeerIdentityResolver`, `BlockReadGate` (Task 9), the existing `collected.blocks` Vec.

**Context:** `check_car_fetch_access` gates only root collection access, then `collect_dag_blocks`/`collect_exact_blocks` returns the whole DAG. Add a per-block filter pass over `blocks` before encoding, for **both** recursive and exact fetches, when the peer is not a replicator.

- [ ] **Step 1: Write the failing test**

A branchable collection with a private doc; a connected-but-non-replicator peer issues a CAR fetch for the DAG → response must exclude blocks the peer can't read; a replicator peer → full DAG.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p p2p car_fetch`
Expected: FAIL — CAR serves the full DAG regardless of per-block read access.

- [ ] **Step 3: Filter response blocks**

After `let blocks = collected.blocks;` (car.rs:133) and before the empty check, when the peer is not a replicator for the collection, resolve identity once and retain only readable blocks:

```rust
let blocks = if self.access.replicators.is_any_replicator(peer_id.as_str()) {
    blocks // replicator passthrough
} else {
    let identity = match self.peer_identity_resolver.resolve(&peer_id).await {
        Some(did) => acp::Identity::Authenticated(did),
        None => acp::Identity::Anonymous,
    };
    let mut kept = Vec::with_capacity(blocks.len());
    for (cid, data) in blocks {
        let allow = match DefraBlock::from_dag_cbor(&data) {
            Ok(b) => self.read_gate.may_read_block(&identity, &b).await,
            Err(_) => true, // non-CRDT (signature/lens) — passthrough, matches bitswap
        };
        if allow { kept.push((cid, data)); }
    }
    kept
};
```

Hold `peer_identity_resolver` + `read_gate` on the `SyncCoordinator` (inject at construction alongside the existing `authorizer`).

- [ ] **Step 4: Run tests**

Run: `cargo test -p p2p car_fetch`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/p2p/src/sync/coordinator
git commit -m "feat(p2p): A3 CAR handler filters served blocks by read access"
```

---

## Task 11: CID-integrity regression coverage (already implemented)

**Files:**
- Test: `crates/p2p/src/sync/manager/process/pushlog.rs` tests + CAR test module

**Context:** `verify_block_cid` already rejects content/CID mismatch (pushlog.rs:235; CAR handler). No production change — add explicit regression tests so the `ErrBlockCIDMismatch` analog can never silently regress.

- [ ] **Step 1: Write the test**

Pushlog: craft a `PushLogMessage` whose `block` bytes do not hash to `cid` → assert the handler returns the CID-verify error and does not store the block. CAR: a CAR response containing a block whose bytes mismatch its CID → assert it is rejected on ingest.

- [ ] **Step 2: Run test to verify behavior**

Run: `cargo test -p p2p verify_block_cid`
Expected: PASS (confirms existing behavior).

- [ ] **Step 3: Commit**

```bash
git add crates/p2p/src/sync
git commit -m "test(p2p): regression cover block CID-integrity on pushlog/CAR"
```

---

## Task 12: Cross-implementation integration scenarios

**Files:**
- Modify: `tools/integration-test/tests/acp.rs` (`branchable_commits`, new `branchable_peer`)
- Modify: `tools/integration-test/tests/p2p.rs` (cross-impl serve scenario) if the matrix harness lives there

**Context:** Port Go #4990's `collection_commits_test.go` and `peer_test.go`. Run the matrix the harness supports: rust↔rust, go↔go, go↔rust.

- [ ] **Step 1: Write the serve-boundary scenario**

Two nodes; node A (owner) hosts a branchable `@policy` collection with a private doc; node B connects as a **non-replicator** and attempts to sync/fetch → B must NOT receive the private blocks; then grant B a reader relationship → B receives them. Add a variant where B is a **replicator** → B receives (passthrough).

- [ ] **Step 2: Run rust↔rust**

Run: `cargo test -p integration-test --test acp -- branchable_peer::`
Expected: PASS.

- [ ] **Step 3: Run go↔rust matrix**

Run the harness with a Go binary as producer and Rust as consumer and vice-versa (per the harness's cross-impl switch; see `reference_go_parity_binary` — Go defradb at `GO_COMPAT_COMMIT` on PATH). Expected: identical withhold/serve behavior in both directions.

- [ ] **Step 4: Commit**

```bash
git add tools/integration-test/tests
git commit -m "test(acp): cross-impl branchable read + serve-boundary parity"
```

---

## Task 13: Adversarial re-audit of the A3 serve boundary (gate before merge)

**Files:** none (verification)

**Context:** The serve gate is the private-data leak boundary. Before the PR merges, run a dedicated adversarial review (a Workflow) over the A3 diff (Tasks 8–10) + the merge handler, specifically hunting: a path that serves a private branchable block to a non-replicator without read access; an unresolved-DID path that grants more than Anonymous; a recursive-vs-exact CAR asymmetry; a transport (libp2p vs Iroh) that skips the gate; a block→docID resolution that misses collection-level blocks.

- [ ] **Step 1: Run the existing P2P/encryption/SE/identity suites (regression gate from the spec's top risk)**

Run:
```bash
cargo test -p integration-test --test p2p
cargo test -p integration-test --test p2p_iroh
cargo test -p integration-test --test encryption
cargo test -p integration-test --test identity
```
Expected: PASS. Any regression here is a **blocker** → revisit serve-scope per the spec.

- [ ] **Step 2: Adversarial review**

Run a multi-agent review (ultracode Workflow) scoped to the A3 diff vs Go `3f627855` `hasAccess`/`trySelfHasAccess`. Resolve or explicitly accept every CONFIRMED/PLAUSIBLE finding.

- [ ] **Step 3: Update PR body**

Change the PR description from "A1 registration" to the full A1–A3 security feature; note the cross-impl matrix and the adversarial audit outcome.

---

## Self-Review

**Spec coverage:** core rule (Tasks 1–2,4) ✔; node_did (Task 3) ✔; version→collection_id (Tasks 5,6,9,10 resolve via `get_collection_by_version_id`/`collections_map`) ✔; commits query (5) ✔; signature verify (6) ✔; KMS is_branchable (7) ✔; peer-DID resolver (8) ✔; bitswap serve (9) ✔; CAR serve (10) ✔; unresolved-DID→Anonymous (9,10) ✔; replicator passthrough (9,10) ✔; pushlog CID already-done (11) ✔; cross-impl tests (12) ✔; top-risk regression gate + adversarial audit (13) ✔.

**Placeholders:** none — every code step shows real code; the two block→docID/`BlockReadGate` impls (Tasks 9 step 4) reference the existing merge-path resolution rather than re-deriving it, which is concrete guidance, not a placeholder.

**Type consistency:** `check_doc_read_access(acp, identity, &CollectionVersion, doc_id, node_identity)` used identically in Tasks 2/6; overlay variant `check_doc_read_access_overlay` with the same shape in Tasks 4/5; `DocAccess { has_access, explicit }` consistent; `PeerIdentityResolver::resolve -> Option<Did>` consistent across Tasks 8/9/10; `DocCollectionInfo.is_branchable` added once (7) and consumed in `kms_check_doc_read_access`.
