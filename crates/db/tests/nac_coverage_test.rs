//! NAC coverage audit.
//!
//! Every `NodePermission` must have a deliberate enforcement decision. This
//! test partitions all permissions into exactly four categories and asserts
//! the partition is complete and disjoint. Adding a new permission breaks this
//! test until it is categorized here — forcing a conscious NAC-coverage choice
//! rather than a silent gap.
//!
//! This is a documentation/invariant test, not a code-coverage scan: the
//! actual enforcement is exercised end-to-end by `nac_enforcement_test.rs`
//! (DB-layer allow/deny) and the `integration-test` `nac`/`acp` suites.

use acp::nac::NodePermission::{self, *};

/// Gated at the DB layer via `DB::check_node_access` — either on a `db`-crate
/// method (document mutators) or at an adapter boundary that delegates through
/// the type-erased `db::NodeAccessChecker` (ACP policy add + P2P management ops,
/// whose adapters are store-typed/type-erased and hold the checker instead of
/// an `Arc<DB<S>>`). These fire for the HTTP/CLI path and, where the gate is on
/// a `db`-crate method (document writes) or a shared `defra-p2p-adapter` op, for
/// the embedded path too.
const DB_LAYER_GATED: &[NodePermission] = &[
    CollectionPatch,
    CollectionTruncate,
    MigrationSet,
    DocumentUpdate, // Go uses update-document for both create and update
    DocumentDelete,
    IndexCreate,
    IndexDelete,
    IndexList,
    EncryptedIndexAdd,
    EncryptedIndexDelete,
    EncryptedIndexList,
    EncryptedIndexListAll,
    ViewAdd,
    ViewRefresh,
    ViewGc,
    LensCreate,
    LensList,
    DacRelationAdd,
    DacRelationDelete,
    DacPolicyAdd, // ACP adapter gates via db::NodeAccessChecker
    P2pPeerConnect,
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
    if DB_LAYER_GATED.contains(&perm) {
        found.push("DB_LAYER_GATED");
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
             Add it to the appropriate list in nac_coverage_test.rs."
        );
    }
}

#[test]
fn categories_account_for_all_permissions() {
    let total = DB_LAYER_GATED.len()
        + NAC_MANAGER_ENFORCED.len()
        + HTTP_BOUNDARY_ONLY.len()
        + UNGATED_BY_DESIGN.len();
    assert_eq!(
        total,
        NodePermission::all().len(),
        "NAC-coverage categories must partition every NodePermission with no overlap"
    );
}
