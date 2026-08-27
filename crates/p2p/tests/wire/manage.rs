use p2p::message::{
    ManageMutateOp, ManageQueryOp, ManageQueryReply, ManageQueryResult, ManageReply, ManageRequest,
    Message,
};

#[test]
fn mutate_op_cbor_round_trip() {
    let op = ManageMutateOp::CollectionAdd {
        collection_ids: vec!["c1".into()],
    };
    assert_eq!(
        op,
        defra_core::cbor::from_slice(&defra_core::cbor::to_vec(&op).unwrap()).unwrap()
    );
}

#[test]
fn query_op_cbor_round_trip() {
    let op = ManageQueryOp::ReplicatorList;
    assert_eq!(
        op,
        defra_core::cbor::from_slice(&defra_core::cbor::to_vec(&op).unwrap()).unwrap()
    );
}

#[test]
fn query_result_strings_cbor_round_trip() {
    let result = ManageQueryResult::Strings {
        values: vec!["col-a".into(), "col-b".into()],
    };
    assert_eq!(
        result,
        defra_core::cbor::from_slice(&defra_core::cbor::to_vec(&result).unwrap()).unwrap()
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
        ManageMutateOp::PeerDisconnect {
            address: "x".into()
        }
        .permission(),
        P::P2pPeerDisconnect
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
        defra_core::cbor::from_slice(&defra_core::cbor::to_vec(&req).unwrap()).unwrap();
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
