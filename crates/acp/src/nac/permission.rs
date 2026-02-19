//! Node-level permission types for NAC.
//!
//! Defines the 48 node-level permissions that control access to
//! database operations when Node Access Control is enabled.

use serde::{Deserialize, Serialize};

/// Node-level permissions (matches Go DefraDB's 48 node permissions).
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
    /// Read documents (used by GET /api/v0/collections/:name/:doc_id, GET /api/v0/graphql, GET /api/v0/backup/export)
    DocumentRead,

    /// Update documents (used by POST/PATCH document endpoints, POST /api/v0/graphql, transaction endpoints)
    DocumentUpdate,

    /// Delete documents (used by DELETE /api/v0/collections/:name/:doc_id)
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
    pub fn as_str(&self) -> &'static str {
        match self {
            // DAC operations
            Self::DacBypass => "dac-bypass",
            Self::DacEnable => "dac-enable",
            Self::DacDisable => "dac-disable",
            Self::DacPurge => "dac-purge",
            Self::DacStatus => "dac-status",
            Self::DacRelationAdd => "dac-relation-add",
            Self::DacRelationDelete => "dac-relation-delete",
            Self::DacPolicyAdd => "dac-policy-add",

            // NAC operations
            Self::NacReEnable => "nac-re-enable",
            Self::NacDisable => "nac-disable",
            Self::NacPurge => "nac-purge",
            Self::NacStatus => "nac-status",
            Self::NacRelationAdd => "nac-relation-add",
            Self::NacRelationDelete => "nac-relation-delete",

            // Collection operations
            Self::CollectionPatch => "collection-patch",
            Self::CollectionGet => "collection-get",
            Self::CollectionTruncate => "collection-truncate",

            // Document operations
            Self::DocumentRead => "document-read",
            Self::DocumentUpdate => "document-update",
            Self::DocumentDelete => "document-delete",

            // Index operations
            Self::IndexList => "index-list",
            Self::IndexCreate => "index-create",
            Self::IndexDelete => "index-delete",
            Self::EncryptedIndexAdd => "encrypted-index-add",
            Self::EncryptedIndexList => "encrypted-index-list",
            Self::EncryptedIndexListAll => "encrypted-index-list-all",
            Self::EncryptedIndexDelete => "encrypted-index-delete",

            // P2P operations
            Self::P2pPeerInfo => "p2p-peer-info",
            Self::P2pPeerConnect => "p2p-peer-connect",
            Self::P2pPeerActive => "p2p-peer-active",
            Self::P2pReplicatorAdd => "p2p-replicator-add",
            Self::P2pReplicatorDelete => "p2p-replicator-delete",
            Self::P2pReplicatorList => "p2p-replicator-list",
            Self::P2pCollectionAdd => "p2p-collection-add",
            Self::P2pCollectionDelete => "p2p-collection-delete",
            Self::P2pCollectionList => "p2p-collection-list",
            Self::P2pDocumentAdd => "p2p-document-add",
            Self::P2pDocumentDelete => "p2p-document-delete",
            Self::P2pDocumentList => "p2p-document-list",
            Self::P2pSyncDocuments => "p2p-sync-documents",
            Self::P2pSyncCollectionVersions => "p2p-sync-collection-versions",
            Self::P2pSyncBranchableCollection => "p2p-sync-branchable-collection",

            // Other
            Self::SignatureVerify => "signature-verify",

            // Lens
            Self::LensCreate => "lens-create",
            Self::LensList => "lens-list",

            // View
            Self::ViewRefresh => "view-refresh",
            Self::ViewAdd => "view-add",

            // Migration
            Self::MigrationSet => "migration-set",
        }
    }

    /// Returns all 48 node permissions.
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
            Self::ViewAdd,
            // Migration
            Self::MigrationSet,
        ]
    }

    /// Parse a permission from its string representation.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            // DAC
            "dac-bypass" => Self::DacBypass,
            "dac-enable" => Self::DacEnable,
            "dac-disable" => Self::DacDisable,
            "dac-purge" => Self::DacPurge,
            "dac-status" => Self::DacStatus,
            "dac-relation-add" => Self::DacRelationAdd,
            "dac-relation-delete" => Self::DacRelationDelete,
            "dac-policy-add" => Self::DacPolicyAdd,
            // NAC
            "nac-re-enable" => Self::NacReEnable,
            "nac-disable" => Self::NacDisable,
            "nac-purge" => Self::NacPurge,
            "nac-status" => Self::NacStatus,
            "nac-relation-add" => Self::NacRelationAdd,
            "nac-relation-delete" => Self::NacRelationDelete,
            // Collection
            "collection-patch" => Self::CollectionPatch,
            "collection-get" => Self::CollectionGet,
            "collection-truncate" => Self::CollectionTruncate,
            // Document
            "document-read" => Self::DocumentRead,
            "document-update" => Self::DocumentUpdate,
            "document-delete" => Self::DocumentDelete,
            // Index
            "index-list" => Self::IndexList,
            "index-create" => Self::IndexCreate,
            "index-delete" => Self::IndexDelete,
            "encrypted-index-add" => Self::EncryptedIndexAdd,
            "encrypted-index-list" => Self::EncryptedIndexList,
            "encrypted-index-list-all" => Self::EncryptedIndexListAll,
            "encrypted-index-delete" => Self::EncryptedIndexDelete,
            // P2P
            "p2p-peer-info" => Self::P2pPeerInfo,
            "p2p-peer-connect" => Self::P2pPeerConnect,
            "p2p-peer-active" => Self::P2pPeerActive,
            "p2p-replicator-add" => Self::P2pReplicatorAdd,
            "p2p-replicator-delete" => Self::P2pReplicatorDelete,
            "p2p-replicator-list" => Self::P2pReplicatorList,
            "p2p-collection-add" => Self::P2pCollectionAdd,
            "p2p-collection-delete" => Self::P2pCollectionDelete,
            "p2p-collection-list" => Self::P2pCollectionList,
            "p2p-document-add" => Self::P2pDocumentAdd,
            "p2p-document-delete" => Self::P2pDocumentDelete,
            "p2p-document-list" => Self::P2pDocumentList,
            "p2p-sync-documents" => Self::P2pSyncDocuments,
            "p2p-sync-collection-versions" => Self::P2pSyncCollectionVersions,
            "p2p-sync-branchable-collection" => Self::P2pSyncBranchableCollection,
            // Other
            "signature-verify" => Self::SignatureVerify,
            // Lens
            "lens-create" => Self::LensCreate,
            "lens-list" => Self::LensList,
            // View
            "view-refresh" => Self::ViewRefresh,
            "view-add" => Self::ViewAdd,
            // Migration
            "migration-set" => Self::MigrationSet,
            _ => return None,
        })
    }

    /// Check if this permission is admin-only (requires owner or admin relation).
    ///
    /// In Go DefraDB, all 48 node permissions are defined with `expr: owner + admin`,
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
        assert_eq!(NodePermission::all().len(), 48);
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
        assert_eq!(format!("{}", NodePermission::DacBypass), "dac-bypass");
        assert_eq!(
            format!("{}", NodePermission::P2pReplicatorAdd),
            "p2p-replicator-add"
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
