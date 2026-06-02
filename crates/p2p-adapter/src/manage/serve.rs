//! Inbound management-request handlers: verify actor token, check NAC, dispatch.
//!
//! Security order is invariant: verify the actor JWT (bound to this node's
//! peer-id for replay protection), then check the actor's DID against the NAC
//! engine for the op's permission, and only THEN dispatch to the controller.
//! On any auth failure, no mutating `P2POperations` method is called.
//!
//! Replies are returned UNSIGNED; the runtime signs them before sending.

use defra_http::router::P2pDocumentRequest;
use defra_http::P2POperations;
use p2p::message::{
    ManageDocRef, ManageMutateOp, ManageQueryOp, ManageQueryReply, ManageQueryRequest,
    ManageQueryResult, ManageReply, ManageRequest,
};

use super::auth::verify_actor_token;

/// Authorize and apply a mutating request; returns an UNSIGNED reply.
pub async fn build_manage_reply(
    ops: &dyn P2POperations,
    nac: &dyn db::NacManagerApi,
    request: ManageRequest,
) -> ManageReply {
    let mid = request.message_id.clone();
    match authorize_and_apply(ops, nac, &request).await {
        Ok(()) => ManageReply::success(&mid),
        Err(e) => ManageReply::error(&mid, &e),
    }
}

async fn authorize_and_apply(
    ops: &dyn P2POperations,
    nac: &dyn db::NacManagerApi,
    request: &ManageRequest,
) -> Result<(), String> {
    let audience = ops.local_peer_id().await.map_err(|e| e.to_string())?;
    let actor = verify_actor_token(&request.auth_token, &audience)?;
    if !nac
        .check_permission(&actor, request.op.permission())
        .await
        .map_err(|e| e.to_string())?
    {
        return Err("unauthorized".into());
    }
    let did_str = actor.to_string();
    match &request.op {
        ManageMutateOp::ReplicatorAdd {
            addresses,
            collection_ids,
        } => ops
            .add_replicator(
                collection_ids.clone(),
                addresses.first().map(|s| s.as_str()),
                vec![],
                Some(did_str.as_str()),
            )
            .await
            .map_err(|e| e.to_string()),
        ManageMutateOp::ReplicatorDelete {
            addresses,
            collection_ids,
        } => ops
            .remove_replicator(
                collection_ids.clone(),
                addresses.first().map(|s| s.as_str()),
            )
            .await
            .map_err(|e| e.to_string()),
        ManageMutateOp::CollectionAdd { collection_ids } => ops
            .add_collections(collection_ids.clone())
            .await
            .map_err(|e| e.to_string()),
        ManageMutateOp::CollectionRemove { collection_ids } => ops
            .remove_collections(collection_ids.clone())
            .await
            .map_err(|e| e.to_string()),
        ManageMutateOp::DocumentAdd { docs } => ops
            .add_documents(to_doc_reqs(docs))
            .await
            .map_err(|e| e.to_string()),
        ManageMutateOp::DocumentRemove { docs } => ops
            .remove_documents(to_doc_reqs(docs))
            .await
            .map_err(|e| e.to_string()),
        ManageMutateOp::PeerConnect { address } => {
            ops.connect_peer(address).await.map_err(|e| e.to_string())
        }
    }
}

fn to_doc_reqs(docs: &[ManageDocRef]) -> Vec<P2pDocumentRequest> {
    docs.iter()
        .map(|d| P2pDocumentRequest {
            collection: d.collection.clone(),
            doc_id: d.doc_id.clone(),
        })
        .collect()
}

/// Authorize and run a read-only query request; returns an UNSIGNED reply.
pub async fn build_manage_query_reply(
    ops: &dyn P2POperations,
    nac: &dyn db::NacManagerApi,
    request: ManageQueryRequest,
) -> ManageQueryReply {
    let mid = request.message_id.clone();
    let run = async {
        let audience = ops.local_peer_id().await.map_err(|e| e.to_string())?;
        let actor = verify_actor_token(&request.auth_token, &audience)?;
        if !nac
            .check_permission(&actor, request.op.permission())
            .await
            .map_err(|e| e.to_string())?
        {
            return Err("unauthorized".to_string());
        }
        Ok(match request.op {
            ManageQueryOp::ReplicatorList => ManageQueryResult::Replicators {
                replicators: ops
                    .get_replicators()
                    .await
                    .map_err(|e| e.to_string())?
                    .into_iter()
                    .map(to_p2p_replicator_info)
                    .collect(),
            },
            ManageQueryOp::CollectionList => ManageQueryResult::Strings {
                values: ops.get_collections().await.map_err(|e| e.to_string())?,
            },
        })
    };
    match run.await {
        Ok(result) => ManageQueryReply::success(&mid, result),
        Err(e) => ManageQueryReply::error(&mid, &e),
    }
}

/// Convert the HTTP-facing replicator info into the P2P wire type expected by
/// `ManageQueryResult::Replicators`.
///
/// The HTTP type carries a single best `address` and an `Option<u8>` status,
/// whereas the wire type carries an `addresses` list and a typed
/// `ReplicatorStatus`. An unknown status byte falls back to the default. The
/// HTTP type does not round-trip a parseable timestamp, so `last_status_change`
/// is left at the Go zero value (via `from_raw`).
fn to_p2p_replicator_info(info: defra_http::router::ReplicatorInfo) -> p2p::ReplicatorInfo {
    let mut out = p2p::ReplicatorInfo::from_raw(
        info.id.unwrap_or_default(),
        info.collections,
        info.address.into_iter().collect(),
    );
    out.status = info
        .status
        .map(|s| p2p::ReplicatorStatus::try_from(s).unwrap_or_default())
        .unwrap_or_default();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use defra_http::router::{
        ExplicitReplayCapabilityInput, P2pDocumentInfo, P2pDocumentRequest, ReplicatorInfo,
    };
    use defra_http::P2PResult;
    use identity::Did;
    use p2p::message::Message;
    use std::sync::Mutex;

    /// Records mutating calls so tests can prove no side effect occurred.
    struct MockOps {
        peer_id: String,
        added_collections: Mutex<Vec<String>>,
    }

    impl MockOps {
        fn new(peer_id: &str) -> Self {
            Self {
                peer_id: peer_id.to_string(),
                added_collections: Mutex::new(Vec::new()),
            }
        }

        fn added_collections(&self) -> Vec<String> {
            self.added_collections.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl P2POperations for MockOps {
        async fn local_peer_id(&self) -> P2PResult<String> {
            Ok(self.peer_id.clone())
        }
        async fn listen_addresses(&self) -> P2PResult<Vec<String>> {
            unimplemented!()
        }
        async fn connected_peers(&self) -> P2PResult<Vec<String>> {
            unimplemented!()
        }
        async fn connect_peer(&self, _addr: &str) -> P2PResult<()> {
            unimplemented!()
        }
        async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
            unimplemented!()
        }
        async fn add_replicator(
            &self,
            _collections: Vec<String>,
            _addr: Option<&str>,
            _explicit_replay_capabilities: Vec<ExplicitReplayCapabilityInput>,
            _expected_authorizer_did: Option<&str>,
        ) -> P2PResult<()> {
            unimplemented!()
        }
        async fn remove_replicator(
            &self,
            _collections: Vec<String>,
            _addr: Option<&str>,
        ) -> P2PResult<()> {
            unimplemented!()
        }
        async fn get_collections(&self) -> P2PResult<Vec<String>> {
            unimplemented!()
        }
        async fn add_collections(&self, collections: Vec<String>) -> P2PResult<()> {
            self.added_collections.lock().unwrap().extend(collections);
            Ok(())
        }
        async fn remove_collections(&self, _collections: Vec<String>) -> P2PResult<()> {
            unimplemented!()
        }
        async fn get_documents(&self) -> P2PResult<Vec<P2pDocumentInfo>> {
            unimplemented!()
        }
        async fn add_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
            unimplemented!()
        }
        async fn remove_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
            unimplemented!()
        }
        async fn republish_document(&self, _collection_name: &str, _doc_id: &str) -> P2PResult<()> {
            unimplemented!()
        }
        async fn sync_documents(
            &self,
            _collection_name: &str,
            _doc_ids: Vec<String>,
        ) -> P2PResult<()> {
            unimplemented!()
        }
        async fn sync_branchable_collection(&self, _collection_id: &str) -> P2PResult<()> {
            unimplemented!()
        }
        async fn sync_collection_versions(&self, _version_ids: Vec<String>) -> P2PResult<()> {
            unimplemented!()
        }
    }

    /// NAC mock that denies every permission check.
    struct DenyNac;

    #[async_trait]
    impl db::NacManagerApi for DenyNac {
        async fn check_permission(
            &self,
            _identity: &Did,
            _permission: acp::NodePermission,
        ) -> db_nac::Result<bool> {
            Ok(false)
        }
        async fn initialize(&self, _owner_identity: Option<&Did>) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn status(&self) -> acp::nac::NacStatus {
            unimplemented!()
        }
        async fn owner(&self) -> Option<Did> {
            unimplemented!()
        }
        async fn is_enabled(&self) -> bool {
            unimplemented!()
        }
        async fn is_admin(&self, _identity: &Did) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn is_admin_persisted(&self, _identity: &Did) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn is_owner(&self, _identity: &Did) -> bool {
            unimplemented!()
        }
        async fn enable(&self, _owner: &Did) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn disable(&self, _requestor: &Did) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn re_enable(&self, _requestor: &Did) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn purge(&self, _requestor: &Did) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn add_admin(&self, _requestor: &Did, _target: &Did) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn remove_admin(&self, _requestor: &Did, _target: &Did) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn add_permission_grant(
            &self,
            _requestor: &Did,
            _target: &Did,
            _permission: acp::NodePermission,
        ) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn remove_permission_grant(
            &self,
            _requestor: &Did,
            _target: &Did,
            _permission: acp::NodePermission,
        ) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn info(&self) -> db_nac::NacInfo {
            unimplemented!()
        }
    }

    /// NAC mock that allows every permission check.
    struct AllowNac;

    #[async_trait]
    impl db::NacManagerApi for AllowNac {
        async fn check_permission(
            &self,
            _identity: &Did,
            _permission: acp::NodePermission,
        ) -> db_nac::Result<bool> {
            Ok(true)
        }
        async fn initialize(&self, _owner_identity: Option<&Did>) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn status(&self) -> acp::nac::NacStatus {
            unimplemented!()
        }
        async fn owner(&self) -> Option<Did> {
            unimplemented!()
        }
        async fn is_enabled(&self) -> bool {
            unimplemented!()
        }
        async fn is_admin(&self, _identity: &Did) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn is_admin_persisted(&self, _identity: &Did) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn is_owner(&self, _identity: &Did) -> bool {
            unimplemented!()
        }
        async fn enable(&self, _owner: &Did) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn disable(&self, _requestor: &Did) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn re_enable(&self, _requestor: &Did) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn purge(&self, _requestor: &Did) -> db_nac::Result<()> {
            unimplemented!()
        }
        async fn add_admin(&self, _requestor: &Did, _target: &Did) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn remove_admin(&self, _requestor: &Did, _target: &Did) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn add_permission_grant(
            &self,
            _requestor: &Did,
            _target: &Did,
            _permission: acp::NodePermission,
        ) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn remove_permission_grant(
            &self,
            _requestor: &Did,
            _target: &Did,
            _permission: acp::NodePermission,
        ) -> db_nac::Result<bool> {
            unimplemented!()
        }
        async fn info(&self) -> db_nac::NacInfo {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn unauthorized_rejected_before_side_effects() {
        let ops = MockOps::new("12D3KooW-THIS");
        let token = crate::manage::auth::mint_token_for("12D3KooW-THIS").0;
        let req = ManageRequest::new(
            ManageMutateOp::CollectionAdd {
                collection_ids: vec!["c1".into()],
            },
            token,
        );
        let reply = build_manage_reply(&ops, &DenyNac, req).await;
        assert_eq!(reply.err_message(), Some("unauthorized"));
        assert!(
            ops.added_collections().is_empty(),
            "no side effect on denial"
        );
    }

    #[tokio::test]
    async fn invalid_token_rejected_before_side_effects() {
        let ops = MockOps::new("12D3KooW-THIS");
        // Token minted for a different node must not authorize against this one,
        // and AllowNac must never be reached.
        let token = crate::manage::auth::mint_token_for("12D3KooW-OTHER").0;
        let req = ManageRequest::new(
            ManageMutateOp::CollectionAdd {
                collection_ids: vec!["c1".into()],
            },
            token,
        );
        let reply = build_manage_reply(&ops, &AllowNac, req).await;
        assert!(reply.err_message().is_some());
        assert!(
            ops.added_collections().is_empty(),
            "no side effect on token rejection"
        );
    }

    #[tokio::test]
    async fn authorized_dispatches_to_controller() {
        let ops = MockOps::new("12D3KooW-THIS");
        let token = crate::manage::auth::mint_token_for("12D3KooW-THIS").0;
        let req = ManageRequest::new(
            ManageMutateOp::CollectionAdd {
                collection_ids: vec!["c1".into()],
            },
            token,
        );
        let reply = build_manage_reply(&ops, &AllowNac, req).await;
        assert_eq!(reply.err_message(), None);
        assert_eq!(ops.added_collections(), vec!["c1".to_string()]);
    }
}
