# DB-Layer NAC Enforcement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add NAC permission checks at the database layer so that all code paths (HTTP, FFI, P2P, internal) are gated, matching Go's `checkNodeAccess` pattern.

**Architecture:** Add a `check_node_access` method to `DB<S>` that delegates to the existing `NacManagerApi`. Every public DB method that mutates or reads user data calls this at the top. Identity is threaded via an `Option<&Did>` parameter added to methods that need it, or extracted from the existing options pattern where applicable.

**Tech Stack:** Rust, async_trait, existing `NacManagerApi` trait in `crates/acp/src/nac/`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `crates/db/src/nac_guard.rs` | New — `check_node_access` helper on `DB<S>` |
| `crates/db/src/database.rs` | Modified — add `nac_manager` field, wire up in `open()` |
| `crates/db/src/collection/crud.rs` | Modified — add NAC checks to create/update/delete |
| `crates/db/src/collection/index_ops.rs` | Modified — add NAC checks to index create/delete/list |
| `crates/db/src/patch/store.rs` | Modified — add NAC check to patch_collection |
| `crates/db/src/migration/mod.rs` | Modified — add NAC check to set_migration |
| `crates/db/src/auto_commit_mutator/` | Modified — add NAC checks to document mutations |

---

### Task 1: Add `nac_manager` to `DB<S>` and create `check_node_access`

**Files:**
- Create: `crates/db/src/nac_guard.rs`
- Modify: `crates/db/src/database.rs`
- Modify: `crates/db/src/lib.rs`

- [ ] **Step 1: Create `nac_guard.rs` with the check function**

```rust
// crates/db/src/nac_guard.rs
use crate::database::DB;
use crate::error::{Error, Result};
use crate::nac::NacManagerApi;
use acp::nac::permission::NodePermission;
use defra_core::identity::Did;
use std::sync::Arc;

impl<S: datastore::Store> DB<S> {
    /// Check NAC permission before proceeding with an operation.
    /// Matches Go's `db.checkNodeAccess()` pattern.
    ///
    /// Returns Ok(()) if:
    /// - NAC is not enabled (all operations allowed)
    /// - The identity has the required permission
    ///
    /// Returns Err if the identity lacks the required permission.
    pub async fn check_node_access(
        &self,
        identity: Option<&Did>,
        permission: NodePermission,
    ) -> Result<()> {
        let nac = match &self.nac_manager {
            Some(nac) => nac,
            None => return Ok(()), // NAC not configured, allow all
        };

        if !nac.is_enabled().await {
            return Ok(());
        }

        let did = identity.cloned().unwrap_or_else(Did::wildcard);
        let allowed = nac
            .check_permission(&did, permission)
            .await?;

        if allowed {
            Ok(())
        } else {
            Err(Error::NotAuthorized {
                permission: permission.as_str().to_string(),
            })
        }
    }
}
```

- [ ] **Step 2: Add `NotAuthorized` variant to Error enum**

In `crates/db/src/error.rs`, add:
```rust
#[error("not authorized to perform operation. Permission: {permission}")]
NotAuthorized { permission: String },
```

- [ ] **Step 3: Add `nac_manager` field to `DB<S>`**

In `crates/db/src/database.rs`, add the field:
```rust
pub struct DB<S: Store> {
    // ... existing fields ...
    pub(crate) nac_manager: Option<Arc<dyn NacManagerApi>>,
}
```

Initialize it as `None` in `open()` and `open_with_options()`. Add a public setter:
```rust
pub fn set_nac_manager(&self, nac: Arc<dyn NacManagerApi>) {
    // Store in the field — this requires interior mutability or init-once pattern
}
```

Alternatively, accept it in the constructor options via `DbOptions`.

- [ ] **Step 4: Register the module in `lib.rs`**

Add `mod nac_guard;` to `crates/db/src/lib.rs`.

- [ ] **Step 5: Verify compilation**

Run: `cargo build -p db`
Expected: Compiles with no errors

- [ ] **Step 6: Commit**

```bash
git add crates/db/src/nac_guard.rs crates/db/src/database.rs crates/db/src/error.rs crates/db/src/lib.rs
git commit -m "feat(db): add check_node_access for DB-layer NAC enforcement"
```

---

### Task 2: Gate collection schema operations

**Files:**
- Modify: `crates/db/src/patch/store.rs`
- Modify: `crates/db/src/migration/mod.rs`
- Modify: `crates/db/src/collection/read.rs` or wherever `get_collection` lives

- [ ] **Step 1: Add NAC check to `patch_collection`**

At the top of the patch_collection method, add:
```rust
self.db.check_node_access(identity, NodePermission::CollectionPatch).await?;
```

The `identity` parameter needs to be threaded from the caller. Check how the method is invoked from FFI/HTTP and ensure `Option<&Did>` is available.

- [ ] **Step 2: Add NAC check to `set_migration`**

```rust
self.db.check_node_access(identity, NodePermission::MigrationSet).await?;
```

- [ ] **Step 3: Add NAC check to `get_collection_by_name` / `get_collections`**

```rust
self.check_node_access(identity, NodePermission::CollectionGet).await?;
```

- [ ] **Step 4: Add NAC check to `truncate_collection`**

```rust
self.check_node_access(identity, NodePermission::CollectionTruncate).await?;
```

- [ ] **Step 5: Verify compilation and tests**

Run: `cargo test -p db --lib`
Expected: All pass (NAC manager is None, so checks are no-ops)

- [ ] **Step 6: Commit**

```bash
git add crates/db/src/patch/store.rs crates/db/src/migration/mod.rs crates/db/src/collection/
git commit -m "feat(db): add NAC checks to collection schema operations"
```

---

### Task 3: Gate document CRUD operations

**Files:**
- Modify: `crates/db/src/auto_commit_mutator/create.rs`
- Modify: `crates/db/src/auto_commit_mutator/update.rs`
- Modify: `crates/db/src/auto_commit_mutator/delete.rs`
- Modify: `crates/db/src/collection/crud.rs`

- [ ] **Step 1: Add NAC check to document create**

In the create path (auto_commit_mutator/create.rs or collection/crud.rs):
```rust
db.check_node_access(identity, NodePermission::DocumentUpdate).await?;
```

Note: Go uses `DocumentUpdate` for create operations (not a separate "create" permission).

- [ ] **Step 2: Add NAC check to document update**

```rust
db.check_node_access(identity, NodePermission::DocumentUpdate).await?;
```

- [ ] **Step 3: Add NAC check to document delete**

```rust
db.check_node_access(identity, NodePermission::DocumentDelete).await?;
```

- [ ] **Step 4: Add NAC check to document read/get**

```rust
db.check_node_access(identity, NodePermission::DocumentRead).await?;
```

- [ ] **Step 5: Verify compilation and tests**

Run: `cargo test -p db --lib`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add crates/db/src/auto_commit_mutator/ crates/db/src/collection/
git commit -m "feat(db): add NAC checks to document CRUD operations"
```

---

### Task 4: Gate index and encryption operations

**Files:**
- Modify: `crates/db/src/collection/index_ops.rs`
- Modify: `crates/db/src/collection/` (encrypted index files)

- [ ] **Step 1: Add NAC checks to index operations**

```rust
// create_index
db.check_node_access(identity, NodePermission::IndexCreate).await?;

// delete_index
db.check_node_access(identity, NodePermission::IndexDelete).await?;

// list_indexes
db.check_node_access(identity, NodePermission::IndexList).await?;
```

- [ ] **Step 2: Add NAC checks to encrypted index operations**

```rust
// add_encrypted_index
db.check_node_access(identity, NodePermission::EncryptedIndexAdd).await?;

// delete_encrypted_index
db.check_node_access(identity, NodePermission::EncryptedIndexDelete).await?;

// list_encrypted_indexes
db.check_node_access(identity, NodePermission::EncryptedIndexList).await?;

// list_all_encrypted_indexes
db.check_node_access(identity, NodePermission::EncryptedIndexListAll).await?;
```

- [ ] **Step 3: Verify compilation and tests**

Run: `cargo test -p db --lib`
Expected: All pass

- [ ] **Step 4: Commit**

```bash
git add crates/db/src/collection/
git commit -m "feat(db): add NAC checks to index and encrypted index operations"
```

---

### Task 5: Gate view and lens operations

**Files:**
- Modify: wherever `add_view`, `refresh_views`, `add_lens`, `list_lenses` live in `crates/db/src/`

- [ ] **Step 1: Add NAC checks to view operations**

```rust
// add_view
db.check_node_access(identity, NodePermission::ViewAdd).await?;

// refresh_views
db.check_node_access(identity, NodePermission::ViewRefresh).await?;
```

- [ ] **Step 2: Add NAC checks to lens operations**

```rust
// add_lens (set_migration)
db.check_node_access(identity, NodePermission::LensCreate).await?;

// list_lenses
db.check_node_access(identity, NodePermission::LensList).await?;
```

- [ ] **Step 3: Verify compilation and tests**

Run: `cargo test -p db --lib`
Expected: All pass

- [ ] **Step 4: Commit**

```bash
git add crates/db/src/
git commit -m "feat(db): add NAC checks to view and lens operations"
```

---

### Task 6: Gate DAC and NAC management operations

**Files:**
- Modify: wherever DAC policy add/relationship add/delete live in `crates/db/src/`
- Modify: NAC enable/disable/re-enable/purge/status

- [ ] **Step 1: Add NAC checks to DAC operations**

```rust
// add_dac_policy
db.check_node_access(identity, NodePermission::DacPolicyAdd).await?;

// add_dac_actor_relationship
db.check_node_access(identity, NodePermission::DacRelationAdd).await?;

// delete_dac_actor_relationship
db.check_node_access(identity, NodePermission::DacRelationDelete).await?;
```

- [ ] **Step 2: Add NAC checks to NAC management operations**

```rust
// re_enable_nac
db.check_node_access(identity, NodePermission::NacReEnable).await?;

// disable_nac
db.check_node_access(identity, NodePermission::NacDisable).await?;

// get_nac_status
db.check_node_access(identity, NodePermission::NacStatus).await?;

// add_nac_actor_relationship
db.check_node_access(identity, NodePermission::NacRelationAdd).await?;

// delete_nac_actor_relationship
db.check_node_access(identity, NodePermission::NacRelationDelete).await?;
```

- [ ] **Step 3: Verify compilation and tests**

Run: `cargo test -p db --lib`
Expected: All pass

- [ ] **Step 4: Commit**

```bash
git add crates/db/src/
git commit -m "feat(db): add NAC checks to DAC and NAC management operations"
```

---

### Task 7: Gate P2P operations

**Files:**
- Modify: wherever P2P operations are exposed in `crates/db/src/` or `crates/embedded/src/`

- [ ] **Step 1: Add NAC checks to all P2P operations**

Every P2P method exposed through the database needs its corresponding permission check. There are 14 P2P permissions:
```rust
P2pPeerInfo, P2pPeerConnect, P2pPeerActive,
P2pReplicatorAdd, P2pReplicatorDelete, P2pReplicatorList,
P2pCollectionAdd, P2pCollectionDelete, P2pCollectionList,
P2pDocumentAdd, P2pDocumentDelete, P2pDocumentList,
P2pSyncDocuments, P2pSyncCollectionVersions, P2pSyncBranchableCollection
```

- [ ] **Step 2: Verify compilation and tests**

Run: `cargo test -p db -p embedded --lib`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add crates/db/src/ crates/embedded/src/
git commit -m "feat(db): add NAC checks to P2P operations"
```

---

### Task 8: Wire up NacManager in embedded node

**Files:**
- Modify: `crates/embedded/src/node.rs` or wherever the node is assembled
- Modify: `crates/ffi/src/node.rs` — pass nac_manager to DB on node creation

- [ ] **Step 1: Pass NacManager to DB during node creation**

When the embedded node creates the `DB<S>`, set the `nac_manager` field:
```rust
let db = DB::open_with_options(store, opts).await?;
if let Some(nac) = &self.nac_manager {
    db.set_nac_manager(nac.clone());
}
```

- [ ] **Step 2: Verify FFI tests still pass**

Run: `cargo test -p ffi --lib`
Expected: All pass

- [ ] **Step 3: Verify full build**

Run: `cargo clippy --all -- -D warnings`
Expected: Clean

- [ ] **Step 4: Commit**

```bash
git add crates/embedded/src/ crates/ffi/src/
git commit -m "feat(embedded): wire NacManager into DB for DB-layer enforcement"
```

---

### Task 9: Add compile-time audit test

**Files:**
- Create: `crates/db/tests/nac_coverage_test.rs`

- [ ] **Step 1: Write a test that scans all public DB methods for NAC checks**

```rust
/// This test ensures that every public method on DB that should have
/// a NAC check actually has one. It greps the source files.
#[test]
fn all_public_db_methods_have_nac_checks() {
    // List of methods that are exempt from NAC (lifecycle, accessors)
    let exempt = &["open", "close", "is_closed", "options", "store",
                    "node_identity", "has_node_identity", "new_txn",
                    "with_txn", "with_txn_async", "from_arc",
                    "open_from_arc", "open_with_options", "set_nac_manager",
                    "check_node_access", "event_bus"];

    // Verify that the exempt list is intentional and complete
    // by checking it against the actual public API
    // This is a documentation test, not a code coverage test
    assert!(!exempt.is_empty(), "exempt list must be maintained");
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/db/tests/
git commit -m "test(db): add NAC coverage audit test"
```

---

## Notes

- **Identity threading is the main challenge.** Many Rust DB methods don't currently accept an identity parameter. Each method needs `identity: Option<&Did>` added, and all callers need to be updated. This is the bulk of the mechanical work.
- **The check is a no-op when NAC is not configured.** The `nac_manager` field is `Option<Arc<dyn NacManagerApi>>` — when `None`, all checks return `Ok(())` immediately.
- **Go uses the `opt.Identity` pattern.** Rust should follow the same approach — extract identity from options at the API boundary and pass it down.
- **P2P operations are the trickiest** because they're called from internal code paths (merge handlers) that don't have identity context. These may need special handling (e.g., the merge handler uses the node's own identity).
