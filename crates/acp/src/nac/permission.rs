//! Node-level permission types for NAC.
//!
//! Defines the 33 node-level permissions that control access to
//! database operations when Node Access Control is enabled.

use serde::{Deserialize, Serialize};

/// Node-level permissions (matches Go DefraDB's 33 node permissions).
///
/// These permissions control access to node-level operations when NAC is enabled.
/// By default (NAC disabled), all operations are allowed without authentication.
///
/// # Implementation Status
///
/// **Currently implemented (20 permissions):**
/// - Collection: `CollectionGet`, `CollectionPatch`
/// - Document: `DocumentRead`, `DocumentUpdate`, `DocumentDelete`
/// - Index: `IndexList`, `IndexCreate`, `IndexDrop`
/// - P2P: `P2pPeerConnect`, `P2pReplicatorCreate`, `P2pReplicatorDelete`, `P2pReplicatorList`,
///   `P2pCollectionCreate`, `P2pCollectionDelete`, `P2pCollectionList`
/// - ACP: `DacPolicyAdd`, `DacStatus`
/// - NAC: `NacStatus`, `NacRelationAdd`, `NacRelationDelete`
///
/// **Not yet implemented (13 permissions):**
/// - DAC management: `DacBypass`, `DacEnable`, `DacDisable`, `DacPurge`,
///   `DacRelationAdd`, `DacRelationDelete`
/// - NAC management: `NacReEnable`, `NacDisable`, `NacPurge`
/// - P2P document replication: `P2pDocumentCreate`, `P2pDocumentDelete`, `P2pDocumentList`
/// - Other: `SignatureVerify`
///
/// These permissions are defined for Go DefraDB compatibility but do not yet have
/// corresponding HTTP endpoints in the Rust implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum NodePermission {
    // =========================================================================
    // DAC (Document Access Control) Operations
    // =========================================================================
    /// Bypass DAC checks entirely (super-admin only)
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    DacBypass,

    /// Enable DAC on the node
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    DacEnable,

    /// Disable DAC on the node
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    DacDisable,

    /// Purge all DAC data (dev mode only)
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    DacPurge,

    /// View DAC status (used by GET /api/v0/acp/policy and GET /api/v0/acp/policy/:id)
    DacStatus,

    /// Add DAC relation on a document
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    DacRelationAdd,

    /// Delete DAC relation on a document
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    DacRelationDelete,

    /// Add a new DAC policy (used by POST /api/v0/acp/policy)
    DacPolicyAdd,

    // =========================================================================
    // NAC (Node Access Control) Operations
    // =========================================================================
    /// Re-enable NAC after temporary disable
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    NacReEnable,

    /// Temporarily disable NAC
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    NacDisable,

    /// Purge all NAC data (dev mode only)
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
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
    /// Note: This permission covers both create and update operations.
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

    /// Drop an index (used by DELETE /api/v0/collections/:name/indexes/:index)
    IndexDrop,

    // =========================================================================
    // P2P Operations
    // =========================================================================
    /// Connect to a peer (used by P2P info, list, and connect endpoints)
    P2pPeerConnect,

    /// Create a replicator (used by POST /api/v0/p2p/replicators)
    P2pReplicatorCreate,

    /// Delete a replicator (used by DELETE /api/v0/p2p/replicators)
    P2pReplicatorDelete,

    /// List replicators (used by GET /api/v0/p2p/replicators)
    P2pReplicatorList,

    /// Add collection to P2P (used by POST /api/v0/p2p/collections)
    P2pCollectionCreate,

    /// Remove collection from P2P (used by DELETE /api/v0/p2p/collections)
    P2pCollectionDelete,

    /// List P2P collections (used by GET /api/v0/p2p/collections)
    P2pCollectionList,

    /// Add document to P2P replication
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    P2pDocumentCreate,

    /// Remove document from P2P replication
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    P2pDocumentDelete,

    /// List P2P replicated documents
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    P2pDocumentList,

    // =========================================================================
    // Other Operations
    // =========================================================================
    /// Verify signatures
    /// **NOT YET IMPLEMENTED** - No HTTP endpoint exists
    SignatureVerify,
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
            Self::IndexDrop => "index-drop",

            // P2P operations
            Self::P2pPeerConnect => "p2p-peer-connect",
            Self::P2pReplicatorCreate => "p2p-replicator-create",
            Self::P2pReplicatorDelete => "p2p-replicator-delete",
            Self::P2pReplicatorList => "p2p-replicator-list",
            Self::P2pCollectionCreate => "p2p-collection-create",
            Self::P2pCollectionDelete => "p2p-collection-delete",
            Self::P2pCollectionList => "p2p-collection-list",
            Self::P2pDocumentCreate => "p2p-document-create",
            Self::P2pDocumentDelete => "p2p-document-delete",
            Self::P2pDocumentList => "p2p-document-list",

            // Other
            Self::SignatureVerify => "signature-verify",
        }
    }

    /// Returns all 34 node permissions.
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
            Self::IndexDrop,
            // P2P
            Self::P2pPeerConnect,
            Self::P2pReplicatorCreate,
            Self::P2pReplicatorDelete,
            Self::P2pReplicatorList,
            Self::P2pCollectionCreate,
            Self::P2pCollectionDelete,
            Self::P2pCollectionList,
            Self::P2pDocumentCreate,
            Self::P2pDocumentDelete,
            Self::P2pDocumentList,
            // Other
            Self::SignatureVerify,
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
            "index-drop" => Self::IndexDrop,
            // P2P
            "p2p-peer-connect" => Self::P2pPeerConnect,
            "p2p-replicator-create" => Self::P2pReplicatorCreate,
            "p2p-replicator-delete" => Self::P2pReplicatorDelete,
            "p2p-replicator-list" => Self::P2pReplicatorList,
            "p2p-collection-create" => Self::P2pCollectionCreate,
            "p2p-collection-delete" => Self::P2pCollectionDelete,
            "p2p-collection-list" => Self::P2pCollectionList,
            "p2p-document-create" => Self::P2pDocumentCreate,
            "p2p-document-delete" => Self::P2pDocumentDelete,
            "p2p-document-list" => Self::P2pDocumentList,
            // Other
            "signature-verify" => Self::SignatureVerify,
            _ => return None,
        })
    }

    /// Check if this permission is admin-only (requires owner or admin relation).
    ///
    /// In Go DefraDB, all 34 node permissions are defined with `expr: owner + admin`,
    /// meaning they all require either the owner or admin relation to be granted.
    /// This matches that behavior.
    pub fn is_admin_only(&self) -> bool {
        // All node permissions require owner or admin relation per Go implementation
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
        assert_eq!(NodePermission::all().len(), 34);
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
            format!("{}", NodePermission::P2pReplicatorCreate),
            "p2p-replicator-create"
        );
    }

    #[test]
    fn test_admin_only_permissions() {
        // All 34 permissions require owner or admin relation (matches Go behavior)
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
