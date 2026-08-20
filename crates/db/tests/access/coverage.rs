//! NAC coverage audit.
//!
//! Every `NodePermission` must have a deliberate enforcement decision. This
//! test partitions all permissions into exactly five categories and asserts
//! the partition is complete and disjoint. Adding a new permission breaks this
//! test until it is categorized here — forcing a conscious NAC-coverage choice
//! rather than a silent gap.
//!
//! This is a documentation/invariant test, not a code-coverage scan: the
//! actual enforcement is exercised end-to-end by `access/enforcement.rs`
//! (DB-layer allow/deny) and the `integration-test` `nac`/`acp` suites.

use acp::nac::NodePermission::{self, *};

/// The operation's raw DB method(s) call `check_node_access`, so even a direct
/// `DB<S>` handle holder is gated (no bypass). Verified locations:
/// - `CollectionPatch`: gated on the raw DB methods (`create_collection`/
///   `create_collections_atomic`/`delete_collection`/`delete_collections`/
///   `delete_collection_version`/`delete_collection_versions_batch`/
///   `set_active_collection_version`/`patch_collection` in `collection_ops/` +
///   `patch/mod.rs`); `add_schema_in_txn` also gates on the registry.
/// - `CollectionTruncate`: `truncate_collection` (`collection_ops/delete.rs`).
/// - `ActionList`: `list_actions` (`action.rs`).
/// - `MigrationSet`: raw `set_migration` (`migration/set_migration.rs`) +
///   `set_migration_in_txn` (registry).
/// - `DocumentUpdate`: `doc_mutator` create/update, `auto_commit_mutator`
///   create/update/create_many/batch. (Go uses update-document for create too.)
/// - `DocumentDelete`: `doc_mutator` delete, `auto_commit_mutator` delete/batch.
const DB_METHOD_GATED: &[NodePermission] = &[
    CollectionPatch,
    CollectionTruncate,
    ActionList,
    MigrationSet,
    DocumentUpdate, // Go uses update-document for both create and update
    DocumentDelete,
];

/// Gated at a boundary that holds a DB handle (cli adapter, `TxnRegistry` txn
/// entry, or via the type-erased `db::NodeAccessChecker`), NOT on the raw DB
/// method. The underlying raw DB write may be ungated because it has an
/// internal/startup/commit/scheduler caller that must not be gated — e.g.
/// `DB::add_lens` (commit callback from `add_lens_in_txn`), `refresh_views`
/// (view-refresh scheduler), `gc_downsample_histories` (GC).
///
/// `ViewGc`: gated in the CLI `view_adapter` (`check_node_access(ViewGc)`), but
/// FFI `gc_downsample_histories` is NOT gated — it has no `identity_did` param
/// and no Go/cbindings binding, so gating it requires a coordinated Go-side
/// signature change (follow-up). The CLI path is ViewGc-gated.
const ADAPTER_GATED: &[NodePermission] = &[
    LensCreate, // add_lens_in_txn gates (registry); raw DB::add_lens ungated (commit caller)
    LensList,
    ViewAdd,
    ViewRefresh, // raw refresh_views ungated (scheduler caller); adapter gates
    ViewGc,      // CLI adapter gates; FFI gc_downsample_histories ungated — see note above
    IndexCreate,
    IndexDelete,
    IndexList,
    EncryptedIndexAdd,
    EncryptedIndexDelete,
    EncryptedIndexList,
    EncryptedIndexListAll,
    DacRelationAdd,
    DacRelationDelete,
    DacPolicyAdd, // ACP adapter gates via db::NodeAccessChecker
    P2pPeerConnect,
    P2pPeerDisconnect,
    P2pPeerActive,
    P2pReplicatorAdd,
    P2pReplicatorDelete,
    P2pReplicatorList,
    P2pCollectionAdd,
    P2pCollectionDelete,
    P2pCollectionList,
    P2pDocumentAdd,
    P2pDocumentDelete,
    P2pDocumentList,
];

/// Authorization enforced inside `NacManager` itself: each method takes an
/// explicit `requestor: &Did` and checks `is_admin`/ownership. A DB-layer
/// `check_node_access` here would be redundant and could conflict.
const NAC_MANAGER_ENFORCED: &[NodePermission] = &[
    NacReEnable,
    NacDisable,
    NacPurge,
    NacStatus,
    NacRelationAdd,
    NacRelationDelete,
];

/// Enforced only at the HTTP boundary (`auth_middleware` + `route_permissions`,
/// plus per-handler `require_permission`). DB-layer gating is either unsafe
/// (pervasive internal callers — reads) or not a discrete adapter op. Mirrors
/// Go, which leaves the internal collection/document fetch ungated and relies
/// on DAC. `P2pPeerInfo` is served by transport-info handlers (`local_peer_id`/
/// `listen_addresses`/`shareable_address`), not a gated management op, so it
/// stays HTTP-boundary-only.
const HTTP_BOUNDARY_ONLY: &[NodePermission] = &[
    CollectionGet, // pervasive internal getter; gating it would break internal lookups
    DocumentRead,  // query fetch is DAC-gated, like Go's ungated fetch
    DacStatus,
    DacBypass, // resolved during request setup, not a gated DB op
    DacEnable,
    DacDisable,
    DacPurge,
    SignatureVerify,
    P2pPeerInfo, // transport-info handlers, not a discrete management op
];

/// Ungated by design, mirroring Go: the merge/replication apply path and the
/// peer-driven sync ops are not `checkNodeAccess`-gated in Go (`Merge()` even
/// clears identity). Only document-level DAC applies during merge.
const UNGATED_BY_DESIGN: &[NodePermission] = &[
    P2pSyncDocuments,
    P2pSyncCollectionVersions,
    P2pSyncBranchableCollection,
];

fn category_of(perm: NodePermission) -> Vec<&'static str> {
    let mut found = Vec::new();
    if DB_METHOD_GATED.contains(&perm) {
        found.push("DB_METHOD_GATED");
    }
    if ADAPTER_GATED.contains(&perm) {
        found.push("ADAPTER_GATED");
    }
    if NAC_MANAGER_ENFORCED.contains(&perm) {
        found.push("NAC_MANAGER_ENFORCED");
    }
    if HTTP_BOUNDARY_ONLY.contains(&perm) {
        found.push("HTTP_BOUNDARY_ONLY");
    }
    if UNGATED_BY_DESIGN.contains(&perm) {
        found.push("UNGATED_BY_DESIGN");
    }
    found
}

#[test]
fn every_permission_has_exactly_one_enforcement_decision() {
    for &perm in NodePermission::all() {
        let cats = category_of(perm);
        assert_eq!(
            cats.len(),
            1,
            "permission `{perm}` must be in exactly one NAC-coverage category, found {cats:?}. \
             Add it to the appropriate list in crates/db/tests/access/coverage.rs."
        );
    }
}

#[test]
fn categories_account_for_all_permissions() {
    let total = DB_METHOD_GATED.len()
        + ADAPTER_GATED.len()
        + NAC_MANAGER_ENFORCED.len()
        + HTTP_BOUNDARY_ONLY.len()
        + UNGATED_BY_DESIGN.len();
    assert_eq!(
        total,
        NodePermission::all().len(),
        "NAC-coverage categories must partition every NodePermission with no overlap"
    );
}
