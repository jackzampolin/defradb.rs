//! Node-level permission types for NAC.
//!
//! Defines the node-level permissions that control access to
//! database operations when Node Access Control is enabled.

use serde::{Deserialize, Serialize};

/// Node-level permissions.
///
/// These permissions control access to node-level operations when NAC is enabled.
/// By default (NAC disabled), all operations are allowed without authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum NodePermission {
    // =========================================================================
    // DAC (Document Access Control) Operations
    // =========================================================================
    /// Bypass DAC checks entirely (super-admin only)
    DacBypass,

    /// Enable DAC on the node
    DacEnable,

    /// Disable DAC on the node
    DacDisable,

    /// Purge all DAC data (dev mode only)
    DacPurge,

    /// View DAC status (used by GET /api/v0/acp/policy and GET /api/v0/acp/policy/:id)
    DacStatus,

    /// Add DAC relation on a document
    DacRelationAdd,

    /// Delete DAC relation on a document
    DacRelationDelete,

    /// Add a new DAC policy (used by POST /api/v0/acp/policy)
    DacPolicyAdd,

    // =========================================================================
    // NAC (Node Access Control) Operations
    // =========================================================================
    /// Re-enable NAC after temporary disable
    NacReEnable,

    /// Temporarily disable NAC
    NacDisable,

    /// Purge all NAC data (dev mode only)
    NacPurge,

    /// View NAC status (used by GET /api/v0/nac/status)
    NacStatus,

    /// Add NAC relation - grant permission to another identity (used by POST /api/v0/nac/admin)
    NacRelationAdd,

    /// Delete NAC relation - revoke permission from another identity (used by DELETE /api/v0/nac/admin)
    NacRelationDelete,

    // =========================================================================
    // Collection Operations
    // =========================================================================
    /// Patch/update collection schema (used by POST /api/v0/schema)
    CollectionPatch,

    /// Get collection information (used by GET /api/v0/collections, GET /api/v0/schema)
    CollectionGet,

    /// Truncate a collection (delete all documents, preserve schema)
    CollectionTruncate,

    // =========================================================================
    // Document Operations
    // =========================================================================
    /// Read documents (used by GET /api/v0/collections/:name/document/:doc_id, GET /api/v0/graphql, GET /api/v0/backup/export)
    DocumentRead,

    /// Update documents (used by POST/PATCH document endpoints, POST /api/v0/graphql, transaction endpoints)
    DocumentUpdate,

    /// Delete documents (used by DELETE /api/v0/collections/:name/document/:doc_id)
    DocumentDelete,

    // =========================================================================
    // Index Operations
    // =========================================================================
    /// List indexes (used by GET /api/v0/collections/:name/indexes)
    IndexList,

    /// Create an index (used by POST /api/v0/collections/:name/indexes)
    IndexCreate,

    /// Delete an index (used by DELETE /api/v0/collections/:name/indexes/:index)
    IndexDelete,

    /// Add an encrypted index (used by POST /api/v0/collections/:name/encrypted-indexes)
    EncryptedIndexAdd,

    /// List encrypted indexes for a collection (used by GET /api/v0/collections/:name/encrypted-indexes)
    EncryptedIndexList,

    /// List all encrypted indexes across collections (used by GET /api/v0/encrypted-indexes)
    EncryptedIndexListAll,

    /// Delete an encrypted index (used by DELETE /api/v0/collections/:name/encrypted-indexes/:field)
    EncryptedIndexDelete,

    // =========================================================================
    // P2P Operations
    // =========================================================================
    /// Get P2P peer info (used by GET /api/v0/p2p/info)
    P2pPeerInfo,

    /// Connect to a peer (used by P2P connect and list endpoints)
    P2pPeerConnect,

    /// List active peers (used by GET /api/v0/p2p/active-peers)
    P2pPeerActive,

    /// Create a replicator (used by POST /api/v0/p2p/replicators)
    P2pReplicatorAdd,

    /// Delete a replicator (used by DELETE /api/v0/p2p/replicators)
    P2pReplicatorDelete,

    /// List replicators (used by GET /api/v0/p2p/replicators)
    P2pReplicatorList,

    /// Add collection to P2P (used by POST /api/v0/p2p/collections)
    P2pCollectionAdd,

    /// Remove collection from P2P (used by DELETE /api/v0/p2p/collections)
    P2pCollectionDelete,

    /// List P2P collections (used by GET /api/v0/p2p/collections)
    P2pCollectionList,

    /// Add document to P2P replication (used by POST /api/v0/p2p/documents)
    P2pDocumentAdd,

    /// Remove document from P2P replication (used by DELETE /api/v0/p2p/documents)
    P2pDocumentDelete,

    /// List P2P replicated documents (used by GET /api/v0/p2p/documents)
    P2pDocumentList,

    /// Sync specific documents from peers (used by POST /api/v0/p2p/documents/sync)
    P2pSyncDocuments,

    /// Sync collection versions from peers (used by POST /api/v0/p2p/collections/sync-versions)
    P2pSyncCollectionVersions,

    /// Sync branchable collection from peers (used by POST /api/v0/p2p/collections/sync-branchable)
    P2pSyncBranchableCollection,

    // =========================================================================
    // Other Operations
    // =========================================================================
    /// Verify signatures
    SignatureVerify,

    // =========================================================================
    // Lens Operations
    // =========================================================================
    /// Create a lens migration (used by POST /api/v0/lens)
    LensCreate,

    /// List lens transforms (used by GET /api/v0/lens)
    LensList,

    // =========================================================================
    // View Operations
    // =========================================================================
    /// Refresh materialized views (used by POST /api/v0/views/refresh)
    ViewRefresh,

    /// Run explicit downsample history GC (used by POST /api/v0/views/gc)
    ViewGc,

    /// Add a view (used by POST /api/v0/views)
    ViewAdd,

    // =========================================================================
    // Migration Operations
    // =========================================================================
    /// Set a lens migration between schema versions (used by POST /api/v0/lens/set)
    MigrationSet,
}

impl NodePermission {
    /// Returns the string representation used in policy definitions.
    ///
    /// These names match Go DefraDB's `RequiredResourcePermissionsForNode`
    /// in `acp/types/types.go` (verb-noun format).
    pub fn as_str(&self) -> &'static str {
        match self {
            // DAC operations
            Self::DacBypass => "bypass-dac",
            Self::DacEnable => "enable-dac",
            Self::DacDisable => "disable-dac",
            Self::DacPurge => "purge-dac",
            Self::DacStatus => "get-dac-status",
            Self::DacRelationAdd => "add-dac-relation",
            Self::DacRelationDelete => "delete-dac-relation",
            Self::DacPolicyAdd => "add-dac-policy",

            // NAC operations
            Self::NacReEnable => "re-enable-nac",
            Self::NacDisable => "disable-nac",
            Self::NacPurge => "purge-nac",
            Self::NacStatus => "get-nac-status",
            Self::NacRelationAdd => "add-nac-relation",
            Self::NacRelationDelete => "delete-nac-relation",

            // Collection operations
            Self::CollectionPatch => "patch-collection",
            Self::CollectionGet => "get-collection",
            Self::CollectionTruncate => "truncate-collection",

            // Document operations
            Self::DocumentRead => "read-document",
            Self::DocumentUpdate => "update-document",
            Self::DocumentDelete => "delete-document",

            // Index operations
            Self::IndexList => "list-index",
            Self::IndexCreate => "new-index",
            Self::IndexDelete => "delete-index",
            Self::EncryptedIndexAdd => "new-encrypted-index",
            Self::EncryptedIndexList => "list-encrypted-index",
            Self::EncryptedIndexListAll => "list-all-encrypted-index",
            Self::EncryptedIndexDelete => "delete-encrypted-index",

            // P2P operations
            Self::P2pPeerInfo => "get-p2p-peer-info",
            Self::P2pPeerConnect => "connect-p2p-peer",
            Self::P2pPeerActive => "get-p2p-active-peers",
            Self::P2pReplicatorAdd => "add-p2p-replicator",
            Self::P2pReplicatorDelete => "delete-p2p-replicator",
            Self::P2pReplicatorList => "list-p2p-replicator",
            Self::P2pCollectionAdd => "add-p2p-collection",
            Self::P2pCollectionDelete => "delete-p2p-collection",
            Self::P2pCollectionList => "list-p2p-collection",
            Self::P2pDocumentAdd => "add-p2p-document",
            Self::P2pDocumentDelete => "delete-p2p-document",
            Self::P2pDocumentList => "list-p2p-document",
            Self::P2pSyncDocuments => "sync-p2p-documents",
            Self::P2pSyncCollectionVersions => "sync-p2p-collection-versions",
            Self::P2pSyncBranchableCollection => "sync-p2p-branchable-collection",

            // Other
            Self::SignatureVerify => "verify-signature",

            // Lens
            Self::LensCreate => "add-lens",
            Self::LensList => "list-lens",

            // View
            Self::ViewRefresh => "refresh-view",
            Self::ViewGc => "view-gc",
            Self::ViewAdd => "add-view",

            // Migration
            Self::MigrationSet => "set-migration",
        }
    }

    /// Returns all node permissions.
    pub fn all() -> &'static [NodePermission] {
        &[
            // DAC
            Self::DacBypass,
            Self::DacEnable,
            Self::DacDisable,
            Self::DacPurge,
            Self::DacStatus,
            Self::DacRelationAdd,
            Self::DacRelationDelete,
            Self::DacPolicyAdd,
            // NAC
            Self::NacReEnable,
            Self::NacDisable,
            Self::NacPurge,
            Self::NacStatus,
            Self::NacRelationAdd,
            Self::NacRelationDelete,
            // Collection
            Self::CollectionPatch,
            Self::CollectionGet,
            Self::CollectionTruncate,
            // Document
            Self::DocumentRead,
            Self::DocumentUpdate,
            Self::DocumentDelete,
            // Index
            Self::IndexList,
            Self::IndexCreate,
            Self::IndexDelete,
            Self::EncryptedIndexAdd,
            Self::EncryptedIndexList,
            Self::EncryptedIndexListAll,
            Self::EncryptedIndexDelete,
            // P2P
            Self::P2pPeerInfo,
            Self::P2pPeerConnect,
            Self::P2pPeerActive,
            Self::P2pReplicatorAdd,
            Self::P2pReplicatorDelete,
            Self::P2pReplicatorList,
            Self::P2pCollectionAdd,
            Self::P2pCollectionDelete,
            Self::P2pCollectionList,
            Self::P2pDocumentAdd,
            Self::P2pDocumentDelete,
            Self::P2pDocumentList,
            Self::P2pSyncDocuments,
            Self::P2pSyncCollectionVersions,
            Self::P2pSyncBranchableCollection,
            // Other
            Self::SignatureVerify,
            // Lens
            Self::LensCreate,
            Self::LensList,
            // View
            Self::ViewRefresh,
            Self::ViewGc,
            Self::ViewAdd,
            // Migration
            Self::MigrationSet,
        ]
    }

    /// Parse a permission from its string representation.
    ///
    /// Accepts Go-compatible verb-noun format names.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            // DAC
            "bypass-dac" => Self::DacBypass,
            "enable-dac" => Self::DacEnable,
            "disable-dac" => Self::DacDisable,
            "purge-dac" => Self::DacPurge,
            "get-dac-status" => Self::DacStatus,
            "add-dac-relation" => Self::DacRelationAdd,
            "delete-dac-relation" => Self::DacRelationDelete,
            "add-dac-policy" => Self::DacPolicyAdd,
            // NAC
            "re-enable-nac" => Self::NacReEnable,
            "disable-nac" => Self::NacDisable,
            "purge-nac" => Self::NacPurge,
            "get-nac-status" => Self::NacStatus,
            "add-nac-relation" => Self::NacRelationAdd,
            "delete-nac-relation" => Self::NacRelationDelete,
            // Collection
            "patch-collection" => Self::CollectionPatch,
            "get-collection" => Self::CollectionGet,
            "truncate-collection" => Self::CollectionTruncate,
            // Document
            "read-document" => Self::DocumentRead,
            "update-document" => Self::DocumentUpdate,
            "delete-document" => Self::DocumentDelete,
            // Index
            "list-index" => Self::IndexList,
            "new-index" => Self::IndexCreate,
            "delete-index" => Self::IndexDelete,
            "new-encrypted-index" => Self::EncryptedIndexAdd,
            "list-encrypted-index" => Self::EncryptedIndexList,
            "list-all-encrypted-index" => Self::EncryptedIndexListAll,
            "delete-encrypted-index" => Self::EncryptedIndexDelete,
            // P2P
            "get-p2p-peer-info" => Self::P2pPeerInfo,
            "connect-p2p-peer" => Self::P2pPeerConnect,
            "get-p2p-active-peers" => Self::P2pPeerActive,
            "add-p2p-replicator" => Self::P2pReplicatorAdd,
            "delete-p2p-replicator" => Self::P2pReplicatorDelete,
            "list-p2p-replicator" => Self::P2pReplicatorList,
            "add-p2p-collection" => Self::P2pCollectionAdd,
            "delete-p2p-collection" => Self::P2pCollectionDelete,
            "list-p2p-collection" => Self::P2pCollectionList,
            "add-p2p-document" => Self::P2pDocumentAdd,
            "delete-p2p-document" => Self::P2pDocumentDelete,
            "list-p2p-document" => Self::P2pDocumentList,
            "sync-p2p-documents" => Self::P2pSyncDocuments,
            "sync-p2p-collection-versions" => Self::P2pSyncCollectionVersions,
            "sync-p2p-branchable-collection" => Self::P2pSyncBranchableCollection,
            // Other
            "verify-signature" => Self::SignatureVerify,
            // Lens
            "add-lens" => Self::LensCreate,
            "list-lens" => Self::LensList,
            // View
            "refresh-view" => Self::ViewRefresh,
            "view-gc" => Self::ViewGc,
            "add-view" => Self::ViewAdd,
            // Migration
            "set-migration" => Self::MigrationSet,
            _ => return None,
        })
    }

    /// Check if this permission is admin-only (requires owner or admin relation).
    ///
    /// In Go DefraDB, node permissions are defined with `expr: owner + admin`,
    /// meaning they all require either the owner or admin relation to be granted.
    pub fn is_admin_only(&self) -> bool {
        true
    }
}

impl std::fmt::Display for NodePermission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_permissions_count() {
        assert_eq!(NodePermission::all().len(), 49);
    }

    #[test]
    fn test_permission_roundtrip() {
        for perm in NodePermission::all() {
            let s = perm.as_str();
            let parsed = NodePermission::parse(s).expect("should parse");
            assert_eq!(*perm, parsed);
        }
    }

    #[test]
    fn test_permission_display() {
        assert_eq!(format!("{}", NodePermission::DacBypass), "bypass-dac");
        assert_eq!(
            format!("{}", NodePermission::P2pReplicatorAdd),
            "add-p2p-replicator"
        );
    }

    #[test]
    fn test_admin_only_permissions() {
        for perm in NodePermission::all() {
            assert!(
                perm.is_admin_only(),
                "permission {} should be admin-only",
                perm
            );
        }
    }

    #[test]
    fn test_invalid_permission_str() {
        assert!(NodePermission::parse("invalid").is_none());
        assert!(NodePermission::parse("").is_none());
    }
}
