//! P2P management channel operation enums, wire envelopes, and NAC-permission mapping.
//!
//! `ManageMutateOp` / `ManageQueryOp` describe the verbs the management channel
//! exposes; `permission()` maps each to the `acp::NodePermission` it requires.
//! `ManageRequest` / `ManageReply` are the wire envelopes for mutating operations;
//! `ManageQueryRequest` / `ManageQueryReply` are the wire envelopes for read-only operations.

use serde::{Deserialize, Serialize};

use super::cbor::{nullable_bytes, optional_bytes};
use super::traits::Message;
use crate::protocol::MESSAGE_VERSION;

/// A document reference for P2P document ops (maps to `P2pDocumentRequest`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManageDocRef {
    #[serde(rename = "Collection")]
    pub collection: String,
    #[serde(rename = "DocID")]
    pub doc_id: String,
}

/// Mutating management operations (ack reply).
// NOTE: keep in sync with defra_http::RemoteManageOp (the http-native mirror).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageMutateOp {
    ReplicatorAdd {
        #[serde(rename = "Addresses")]
        addresses: Vec<String>,
        #[serde(rename = "CollectionIDs", default)]
        collection_ids: Vec<String>,
        #[serde(
            rename = "Filters",
            default,
            skip_serializing_if = "crate::replicator::no_replication_filters"
        )]
        filters: crate::replicator::ReplicationFilters,
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
    PeerDisconnect {
        #[serde(rename = "Address")]
        address: String,
    },
}

/// Read-only management operations (typed reply).
// NOTE: keep in sync with defra_http::RemoteManageQueryOp (the http-native mirror).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageQueryOp {
    ReplicatorList,
    CollectionList,
    DocumentList,
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
    Documents {
        #[serde(rename = "Documents")]
        documents: Vec<ManageDocRef>,
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
            ManageMutateOp::PeerDisconnect { .. } => P::P2pPeerConnect,
        }
    }
}

impl ManageQueryOp {
    pub fn permission(&self) -> acp::NodePermission {
        use acp::NodePermission as P;
        match self {
            ManageQueryOp::ReplicatorList => P::P2pReplicatorList,
            ManageQueryOp::CollectionList => P::P2pCollectionList,
            ManageQueryOp::DocumentList => P::P2pDocumentList,
        }
    }
}

/// Wire envelope for a mutating management-channel request.
///
/// The six MetaData fields are byte-identical to the SE message envelopes for
/// compatibility with the shared `signing`/`verify_message` path.
///
/// Note: We don't use `#[serde(flatten)]` because serde_cbor produces
/// indefinite-length maps when flatten is used (CBOR major type 0xbf).
/// Go's fxamacker/cbor produces definite-length maps, causing signature
/// verification to fail. Instead, we duplicate the fields for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManageRequest {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,

    /// Signed actor auth token (JWT). Authenticates the actor DID for NAC.
    #[serde(rename = "AuthToken", with = "serde_bytes")]
    pub auth_token: Vec<u8>,

    /// The management operation to perform.
    #[serde(rename = "Op")]
    pub op: ManageMutateOp,
}

impl ManageRequest {
    /// Create a new ManageRequest.
    pub fn new(op: ManageMutateOp, auth_token: Vec<u8>) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: String::new(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            auth_token,
            op,
        }
    }
}

impl Message for ManageRequest {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

/// Ack reply for a mutating management-channel request.
///
/// Matches `PushSEArtifactsReply` in shape — six MetaData fields, no payload.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManageReply {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,
}

impl ManageReply {
    /// Create a successful ManageReply.
    pub fn success(request_message_id: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
        }
    }

    /// Create an error ManageReply.
    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
        }
    }
}

impl Message for ManageReply {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

/// Wire envelope for a read-only management-channel request.
///
/// The six MetaData fields are byte-identical to the SE message envelopes for
/// compatibility with the shared `signing`/`verify_message` path.
///
/// Note: We don't use `#[serde(flatten)]` because serde_cbor produces
/// indefinite-length maps when flatten is used (CBOR major type 0xbf).
/// Go's fxamacker/cbor produces definite-length maps, causing signature
/// verification to fail. Instead, we duplicate the fields for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManageQueryRequest {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,

    /// Signed actor auth token (JWT). Authenticates the actor DID for NAC.
    #[serde(rename = "AuthToken", with = "serde_bytes")]
    pub auth_token: Vec<u8>,

    /// The read-only management operation to perform.
    #[serde(rename = "Op")]
    pub op: ManageQueryOp,
}

impl ManageQueryRequest {
    /// Create a new ManageQueryRequest.
    pub fn new(op: ManageQueryOp, auth_token: Vec<u8>) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: String::new(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            auth_token,
            op,
        }
    }
}

impl Message for ManageQueryRequest {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

/// Reply to a read-only management-channel request with a typed result payload.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManageQueryReply {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,

    /// Typed result payload (absent on error).
    #[serde(rename = "Result", skip_serializing_if = "Option::is_none", default)]
    pub result: Option<ManageQueryResult>,
}

impl ManageQueryReply {
    /// Create a successful ManageQueryReply with a typed result.
    pub fn success(request_message_id: &str, result: ManageQueryResult) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            result: Some(result),
        }
    }

    /// Create an error ManageQueryReply.
    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
            result: None,
        }
    }
}

impl Message for ManageQueryReply {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::super::Message;
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
        assert_eq!(
            ManageQueryOp::ReplicatorList.permission(),
            P::P2pReplicatorList
        );
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
                filters: Default::default(),
            }
            .permission(),
            P::P2pReplicatorAdd
        );
    }

    #[test]
    fn request_round_trip_and_trait() {
        let mut req = ManageRequest::new(
            ManageMutateOp::DocumentRemove { docs: vec![] },
            b"jwt".to_vec(),
        );
        req.set_message_id("mid-1".into());
        let back: ManageRequest =
            serde_cbor::from_slice(&serde_cbor::to_vec(&req).unwrap()).unwrap();
        assert_eq!(back.message_id(), "mid-1");
        assert_eq!(back.auth_token, b"jwt");
    }

    #[test]
    fn replies_build() {
        assert!(ManageReply::success("m").err_message().is_none());
        assert_eq!(
            ManageReply::error("m", "unauthorized").err_message(),
            Some("unauthorized")
        );
        let q = ManageQueryReply::success(
            "m",
            ManageQueryResult::Strings {
                values: vec!["c".into()],
            },
        );
        assert!(matches!(q.result, Some(ManageQueryResult::Strings { .. })));
    }
}
