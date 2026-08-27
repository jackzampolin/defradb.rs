use p2p::message::{ManageMutateOp, ManageRequest};

#[tokio::test]
async fn manage_request_decodes() {
    let req = ManageRequest::new(
        ManageMutateOp::CollectionAdd {
            collection_ids: vec!["c1".into()],
        },
        b"t".to_vec(),
    );
    let back: ManageRequest =
        defra_core::cbor::from_slice(&defra_core::cbor::to_vec(&req).unwrap()).unwrap();
    assert!(matches!(back.op, ManageMutateOp::CollectionAdd { .. }));
}
