//! Inbound management-request handlers: verify actor token, check NAC, dispatch.
//!
//! Security order is invariant: verify the actor JWT (bound to this node's
//! peer-id for replay protection), then check the actor's DID against the NAC
//! engine for the op's permission, and only THEN dispatch to the controller.
//! On any auth failure, no mutating `P2POperations` method is called.
//!
//! Replies are returned UNSIGNED; the runtime signs them before sending.

use defra_http::router::P2pDocumentRequest;
use defra_http::{P2POperations, MANAGE_UNAUTHORIZED};
use p2p::message::{
    ManageDocRef, ManageMutateOp, ManageQueryOp, ManageQueryReply, ManageQueryRequest,
    ManageQueryResult, ManageReply, ManageRequest,
};
use p2p::transport::PeerId;
use p2p::P2PTransport;

use super::auth::verify_actor_token;
use super::hooks::ManageHooks;

/// Serve an inbound manage MUTATE request: authorize+dispatch (unsigned reply),
/// sign once with this node's transport, then send the response. The
/// sign-then-send ordering is security-sensitive and lives only here. Logs and
/// drops on send failure; silently drops if signing fails (mirrors the SE serve
/// pattern in `db_merge::se::serve`).
pub async fn serve_manage_request<T: P2PTransport>(
    hooks: &ManageHooks,
    transport: &T,
    peer_id: &PeerId,
    request: ManageRequest,
) {
    let mut reply = build_manage_reply(hooks.ops.as_ref(), hooks.nac.as_ref(), request).await;
    if p2p::signing::sign_with_transport(transport, &mut reply).is_ok() {
        if let Err(error) = transport.send_manage_response(peer_id, reply).await {
            tracing::warn!(error = %error, "failed to send manage response");
        }
    }
}

/// Serve an inbound manage QUERY request: authorize+dispatch (unsigned reply),
/// sign once with this node's transport, then send the response. Same
/// sign-then-send invariant and failure handling as [`serve_manage_request`].
pub async fn serve_manage_query_request<T: P2PTransport>(
    hooks: &ManageHooks,
    transport: &T,
    peer_id: &PeerId,
    request: ManageQueryRequest,
) {
    let mut reply = build_manage_query_reply(hooks.ops.as_ref(), hooks.nac.as_ref(), request).await;
    if p2p::signing::sign_with_transport(transport, &mut reply).is_ok() {
        if let Err(error) = transport.send_manage_query_response(peer_id, reply).await {
            tracing::warn!(error = %error, "failed to send manage query response");
        }
    }
}

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
        return Err(MANAGE_UNAUTHORIZED.into());
    }
    let did_str = actor.to_string();
    match &request.op {
        ManageMutateOp::ReplicatorAdd {
            addresses,
            collection_ids,
        } => {
            if addresses.len() > 1 {
                return Err("replicator add supports at most one address".to_string());
            }
            ops.add_replicator(
                collection_ids.clone(),
                addresses.first().map(|s| s.as_str()),
                vec![],
                Some(did_str.as_str()),
            )
            .await
            .map_err(|e| e.to_string())
        }
        ManageMutateOp::ReplicatorDelete {
            addresses,
            collection_ids,
        } => {
            if addresses.len() > 1 {
                return Err("replicator delete supports at most one address".to_string());
            }
            ops.remove_replicator(
                collection_ids.clone(),
                addresses.first().map(|s| s.as_str()),
            )
            .await
            .map_err(|e| e.to_string())
        }
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
            return Err(MANAGE_UNAUTHORIZED.to_string());
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
/// single `address` is lossy at the HTTP boundary (multi-address replicators
/// collapse to one); that loss is upstream of this conversion.
///
/// The HTTP `last_status_change` is an RFC3339 string produced by
/// `last_status_change_go_string()`; we parse it back into the wire type's
/// `DateTime<Utc>` so the timestamp survives the round-trip. An absent or
/// unparseable value leaves the Go zero default from `from_raw`.
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
    if let Some(ts) = info
        .last_status_change
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
    {
        out.last_status_change = ts.with_timezone(&chrono::Utc);
    }
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
        added_replicators: Mutex<Vec<(Vec<String>, Option<String>)>>,
    }

    impl MockOps {
        fn new(peer_id: &str) -> Self {
            Self {
                peer_id: peer_id.to_string(),
                added_collections: Mutex::new(Vec::new()),
                added_replicators: Mutex::new(Vec::new()),
            }
        }

        fn added_collections(&self) -> Vec<String> {
            self.added_collections.lock().unwrap().clone()
        }

        fn added_replicators(&self) -> Vec<(Vec<String>, Option<String>)> {
            self.added_replicators.lock().unwrap().clone()
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
            collections: Vec<String>,
            addr: Option<&str>,
            _explicit_replay_capabilities: Vec<ExplicitReplayCapabilityInput>,
            _expected_authorizer_did: Option<&str>,
        ) -> P2PResult<()> {
            self.added_replicators
                .lock()
                .unwrap()
                .push((collections, addr.map(|s| s.to_string())));
            Ok(())
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

    /// NAC mock whose `check_permission` returns the wrapped bool; every other
    /// method is unreachable in these tests. `BoolNac(false)` denies,
    /// `BoolNac(true)` allows.
    struct BoolNac(bool);

    #[async_trait]
    impl db::NacManagerApi for BoolNac {
        async fn check_permission(
            &self,
            _identity: &Did,
            _permission: acp::NodePermission,
        ) -> db_nac::Result<bool> {
            Ok(self.0)
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

    #[test]
    fn replicator_timestamp_survives_round_trip() {
        let http = ReplicatorInfo {
            id: Some("12D3KooW-PEER".into()),
            collections: vec!["c1".into()],
            address: Some("/ip4/1.2.3.4/tcp/9000".into()),
            status: Some(1),
            last_status_change: Some("2024-03-14T15:09:26.535Z".into()),
        };
        let out = to_p2p_replicator_info(http);
        assert_eq!(
            out.last_status_change_go_string(),
            "2024-03-14T15:09:26.535Z"
        );
        assert_eq!(out.status, p2p::ReplicatorStatus::Inactive);
        assert_eq!(out.addresses_str(), &["/ip4/1.2.3.4/tcp/9000".to_string()]);
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
        let reply = build_manage_reply(&ops, &BoolNac(false), req).await;
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
        let reply = build_manage_reply(&ops, &BoolNac(true), req).await;
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
        let reply = build_manage_reply(&ops, &BoolNac(true), req).await;
        assert_eq!(reply.err_message(), None);
        assert_eq!(ops.added_collections(), vec!["c1".to_string()]);
    }

    #[tokio::test]
    async fn replicator_add_multi_address_rejected_without_side_effects() {
        let ops = MockOps::new("12D3KooW-THIS");
        let token = crate::manage::auth::mint_token_for("12D3KooW-THIS").0;
        let req = ManageRequest::new(
            ManageMutateOp::ReplicatorAdd {
                addresses: vec![
                    "/ip4/1.1.1.1/tcp/9000".into(),
                    "/ip4/2.2.2.2/tcp/9000".into(),
                ],
                collection_ids: vec!["c1".into()],
            },
            token,
        );
        let reply = build_manage_reply(&ops, &BoolNac(true), req).await;
        let msg = reply.err_message().expect("expected an error reply");
        assert_ne!(msg, "unauthorized");
        assert!(
            msg.contains("at most one address"),
            "error should explain the address-count limit, got: {msg}"
        );
        assert!(
            ops.added_replicators().is_empty(),
            "no side effect when rejecting multi-address replicator add"
        );
    }

    #[tokio::test]
    async fn replicator_add_single_address_dispatches() {
        let ops = MockOps::new("12D3KooW-THIS");
        let token = crate::manage::auth::mint_token_for("12D3KooW-THIS").0;
        let req = ManageRequest::new(
            ManageMutateOp::ReplicatorAdd {
                addresses: vec!["/ip4/1.1.1.1/tcp/9000".into()],
                collection_ids: vec!["c1".into()],
            },
            token,
        );
        let reply = build_manage_reply(&ops, &BoolNac(true), req).await;
        assert_eq!(reply.err_message(), None);
        assert_eq!(
            ops.added_replicators(),
            vec![(
                vec!["c1".to_string()],
                Some("/ip4/1.1.1.1/tcp/9000".to_string())
            )]
        );
    }
}
