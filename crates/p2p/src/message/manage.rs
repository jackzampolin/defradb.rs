//! P2P management channel operation enums and their NAC-permission mapping.
//!
//! `ManageMutateOp` / `ManageQueryOp` describe the verbs the management channel
//! exposes; `permission()` maps each to the `acp::NodePermission` it requires.

use serde::{Deserialize, Serialize};

/// A document reference for P2P document ops (maps to `P2pDocumentRequest`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManageDocRef {
    #[serde(rename = "Collection")]
    pub collection: String,
    #[serde(rename = "DocID")]
    pub doc_id: String,
}

/// Mutating management operations (ack reply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageMutateOp {
    ReplicatorAdd {
        #[serde(rename = "Addresses")]
        addresses: Vec<String>,
        #[serde(rename = "CollectionIDs", default)]
        collection_ids: Vec<String>,
    },
    ReplicatorDelete {
        #[serde(rename = "Addresses", default)]
        addresses: Vec<String>,
        #[serde(rename = "CollectionIDs", default)]
        collection_ids: Vec<String>,
    },
    CollectionAdd {
        #[serde(rename = "CollectionIDs")]
        collection_ids: Vec<String>,
    },
    CollectionRemove {
        #[serde(rename = "CollectionIDs")]
        collection_ids: Vec<String>,
    },
    DocumentAdd {
        #[serde(rename = "Docs")]
        docs: Vec<ManageDocRef>,
    },
    DocumentRemove {
        #[serde(rename = "Docs")]
        docs: Vec<ManageDocRef>,
    },
    PeerConnect {
        #[serde(rename = "Address")]
        address: String,
    },
}

/// Read-only management operations (typed reply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageQueryOp {
    ReplicatorList,
    CollectionList,
}

/// Typed payload for a `manage_query` reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageQueryResult {
    Replicators {
        #[serde(rename = "Replicators")]
        replicators: Vec<crate::replicator::ReplicatorInfo>,
    },
    Strings {
        #[serde(rename = "Values")]
        values: Vec<String>,
    },
}

impl ManageMutateOp {
    pub fn permission(&self) -> acp::NodePermission {
        use acp::NodePermission as P;
        match self {
            ManageMutateOp::ReplicatorAdd { .. } => P::P2pReplicatorAdd,
            ManageMutateOp::ReplicatorDelete { .. } => P::P2pReplicatorDelete,
            ManageMutateOp::CollectionAdd { .. } => P::P2pCollectionAdd,
            ManageMutateOp::CollectionRemove { .. } => P::P2pCollectionDelete,
            ManageMutateOp::DocumentAdd { .. } => P::P2pDocumentAdd,
            ManageMutateOp::DocumentRemove { .. } => P::P2pDocumentDelete,
            ManageMutateOp::PeerConnect { .. } => P::P2pPeerConnect,
        }
    }
}

impl ManageQueryOp {
    pub fn permission(&self) -> acp::NodePermission {
        use acp::NodePermission as P;
        match self {
            ManageQueryOp::ReplicatorList => P::P2pReplicatorList,
            ManageQueryOp::CollectionList => P::P2pCollectionList,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutate_op_cbor_round_trip() {
        let op = ManageMutateOp::CollectionAdd {
            collection_ids: vec!["c1".into()],
        };
        assert_eq!(
            op,
            serde_cbor::from_slice(&serde_cbor::to_vec(&op).unwrap()).unwrap()
        );
    }

    #[test]
    fn query_op_cbor_round_trip() {
        let op = ManageQueryOp::ReplicatorList;
        assert_eq!(
            op,
            serde_cbor::from_slice(&serde_cbor::to_vec(&op).unwrap()).unwrap()
        );
    }

    #[test]
    fn query_result_strings_cbor_round_trip() {
        let result = ManageQueryResult::Strings {
            values: vec!["col-a".into(), "col-b".into()],
        };
        assert_eq!(
            result,
            serde_cbor::from_slice(&serde_cbor::to_vec(&result).unwrap()).unwrap()
        );
    }

    #[test]
    fn ops_map_to_permissions() {
        use acp::NodePermission as P;
        assert_eq!(
            ManageMutateOp::PeerConnect {
                address: "x".into()
            }
            .permission(),
            P::P2pPeerConnect
        );
        assert_eq!(ManageQueryOp::ReplicatorList.permission(), P::P2pReplicatorList);
        assert_eq!(
            ManageMutateOp::CollectionRemove {
                collection_ids: vec![]
            }
            .permission(),
            P::P2pCollectionDelete
        );
        assert_eq!(
            ManageMutateOp::DocumentRemove { docs: vec![] }.permission(),
            P::P2pDocumentDelete
        );
        assert_eq!(
            ManageMutateOp::ReplicatorAdd {
                addresses: vec![],
                collection_ids: vec![],
            }
            .permission(),
            P::P2pReplicatorAdd
        );
    }
}
