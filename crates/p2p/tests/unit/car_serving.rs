use super::*;
use crate::sync::car::{decode_car, decode_car_oversized, encode_car_response, CAR_MAX_BYTES};

#[tokio::test]
async fn oversized_car_block_keeps_sibling_and_accurate_presence_counts() {
    for allowed in [false, true] {
        let peer = random_peer_id();
        let transport = NoopTransport::new();
        let store = Arc::new(RegolithStore::in_memory().unwrap());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let oversized_data = vec![0; CAR_MAX_BYTES + 1];
        let oversized = Cid::new_v1(0x71, Code::Sha2_256.digest(&oversized_data));
        let small_data = b"small";
        let small = Cid::new_v1(0x71, Code::Sha2_256.digest(small_data));
        let absent = Cid::new_v1(0x71, Code::Sha2_256.digest(b"absent"));
        blockstore.put(&oversized, &oversized_data).await.unwrap();
        blockstore.put(&small, small_data).await.unwrap();

        let replicators = Arc::new(ReplicatorRegistry::new());
        if allowed {
            replicators.add_replicator("collection1", peer.as_str());
        }
        let (coordinator, _events) = SyncCoordinator::with_access_control_and_serve_gate(
            transport.clone(),
            blockstore,
            SyncConfig::default(),
            AccessMode::Controlled,
            replicators,
            Arc::new(NoOpCollectionStorage),
            Arc::new(crate::replicator::EqOnlyFilterMatcher),
            Arc::new(StaticDataClassifier {
                collection_id: "collection1".to_owned(),
            }),
            Arc::new(LateBoundServeAcp::default()),
        )
        .await
        .unwrap();

        coordinator
            .handle_transport_event(selective_car_fetch_event(
                peer,
                oversized,
                vec![oversized, absent, small],
            ))
            .await
            .unwrap();

        let responses = transport.car_responses();
        assert_eq!(responses.len(), 1);
        let (_, blocks) = decode_car(&responses[0]).unwrap();
        if allowed {
            assert_eq!(blocks, vec![(small, small_data.to_vec())]);
            assert_eq!(
                decode_car_oversized(&responses[0]).unwrap(),
                vec![(oversized, CAR_MAX_BYTES + 1)]
            );
        } else {
            assert!(
                blocks.is_empty(),
                "skipping oversized data must not bypass access control"
            );
            assert!(decode_car_oversized(&responses[0]).unwrap().is_empty());
        }
        let diagnostics = coordinator.manager().diagnostics.snapshot();
        assert_eq!(diagnostics.car_requested_cids, 3);
        assert_eq!(diagnostics.car_present_cids, 2);
        assert_eq!(diagnostics.car_served_cids, u64::from(allowed));
        assert_eq!(diagnostics.car_filtered_cids, u64::from(!allowed));
    }
}

#[tokio::test]
async fn car_size_notice_stores_siblings_and_only_reports_missing_link() {
    use crate::sync::manager::FetchCompletion;
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink};

    let peer = random_peer_id();
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, _events) = SyncCoordinator::new(
        NoopTransport::new(),
        blockstore.clone(),
        SyncConfig::default(),
    )
    .await
    .unwrap();
    let large = Cid::new_v1(0x71, Code::Sha2_256.digest(b"large"));
    let small_data = serde_ipld_dagcbor::to_vec(&ipld_core::ipld!({ "value": "small" })).unwrap();
    let small = Cid::new_v1(0x71, Code::Sha2_256.digest(&small_data));
    let root_data = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "version".to_owned(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("large", large), DAGLink::new("small", small)],
    )
    .to_dag_cbor()
    .unwrap();
    let root = Cid::new_v1(0x71, Code::Sha2_256.digest(&root_data));
    let data = encode_car_response(
        &[root],
        &[(&root, &root_data), (&small, &small_data)],
        &[(large, CAR_MAX_BYTES + 1)],
    )
    .unwrap();
    let completion = coordinator
        .manager()
        .block_sync_completion_tracker()
        .register(QueryId(71));
    coordinator
        .handle_transport_event(TransportEvent::CarFetchResponse {
            query_id: Some(QueryId(71)),
            peer_id: peer.clone(),
            root_cid: root,
            car_data: data,
        })
        .await
        .unwrap();
    assert_eq!(completion.await.unwrap(), FetchCompletion::SizeLimit(large));
    assert!(blockstore.has(&root).await.unwrap());
    assert!(blockstore.has(&small).await.unwrap());

    // A claim about a block already present must not veto this fetch.
    let completion = coordinator
        .manager()
        .block_sync_completion_tracker()
        .register(QueryId(72));
    let data = encode_car_response(&[root], &[], &[(small, CAR_MAX_BYTES + 1)]).unwrap();
    coordinator
        .handle_transport_event(TransportEvent::CarFetchResponse {
            query_id: Some(QueryId(72)),
            peer_id: peer.clone(),
            root_cid: root,
            car_data: data,
        })
        .await
        .unwrap();
    assert_eq!(completion.await.unwrap(), FetchCompletion::Success);

    let mut completion = coordinator
        .manager()
        .rooted_car_completion_tracker()
        .register(root, peer);
    let data = encode_car_response(&[root], &[], &[(large, CAR_MAX_BYTES + 1)]).unwrap();
    coordinator
        .handle_transport_event(TransportEvent::CarFetchResponse {
            query_id: None,
            peer_id: random_peer_id(),
            root_cid: root,
            car_data: data,
        })
        .await
        .unwrap();
    assert!(matches!(
        completion.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
}
