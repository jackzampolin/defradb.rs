//! Node-level permission types for NAC.
//!
//! Defines the 33 node-level permissions that control access to
//! database operations when Node Access Control is enabled.

use serde::{Deserialize, Serialize};

/// Node-level permissions (matches Go DefraDB's 33 node permissions).
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

    /// View DAC status
    DacStatus,

    /// Add DAC relation on a document
    DacRelationAdd,

    /// Delete DAC relation on a document
    DacRelationDelete,

    /// Add a new DAC policy
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

    /// View NAC status
    NacStatus,

    /// Add NAC relation (grant permission to another identity)
    NacRelationAdd,

    /// Delete NAC relation (revoke permission from another identity)
    NacRelationDelete,

    // =========================================================================
    // Collection Operations
    // =========================================================================
    /// Patch/update collection schema
    CollectionPatch,

    /// Get collection information
    CollectionGet,

    // =========================================================================
    // Document Operations
    // =========================================================================
    /// Read documents
    DocumentRead,

    /// Update documents
    DocumentUpdate,

    /// Delete documents
    DocumentDelete,

    // =========================================================================
    // Index Operations
    // =========================================================================
    /// List indexes
    IndexList,

    /// Create an index
    IndexCreate,

    /// Drop an index
    IndexDrop,

    // =========================================================================
    // P2P Operations
    // =========================================================================
    /// Connect to a peer
    P2pPeerConnect,

    /// Create a replicator
    P2pReplicatorCreate,

    /// Delete a replicator
    P2pReplicatorDelete,

    /// List replicators
    P2pReplicatorList,

    /// Add collection to P2P
    P2pCollectionCreate,

    /// Remove collection from P2P
    P2pCollectionDelete,

    /// List P2P collections
    P2pCollectionList,

    /// Add document to P2P replication
    P2pDocumentCreate,

    /// Remove document from P2P replication
    P2pDocumentDelete,

    /// List P2P replicated documents
    P2pDocumentList,

    // =========================================================================
    // Other Operations
    // =========================================================================
    /// Verify signatures
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

    /// Returns all 33 node permissions.
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
    pub fn is_admin_only(&self) -> bool {
        matches!(
            self,
            Self::DacBypass
                | Self::DacEnable
                | Self::DacDisable
                | Self::DacPurge
                | Self::NacReEnable
                | Self::NacDisable
                | Self::NacPurge
                | Self::NacRelationAdd
                | Self::NacRelationDelete
        )
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
        assert_eq!(NodePermission::all().len(), 33);
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
        assert!(NodePermission::DacBypass.is_admin_only());
        assert!(NodePermission::NacPurge.is_admin_only());
        assert!(!NodePermission::DocumentRead.is_admin_only());
        assert!(!NodePermission::P2pReplicatorList.is_admin_only());
    }

    #[test]
    fn test_invalid_permission_str() {
        assert!(NodePermission::parse("invalid").is_none());
        assert!(NodePermission::parse("").is_none());
    }
}
