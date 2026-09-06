use super::*;
use crate::sync::car::{decode_car, CAR_MAX_BYTES};

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
        } else {
            assert!(
                blocks.is_empty(),
                "skipping oversized data must not bypass access control"
            );
        }
        let diagnostics = coordinator.manager().diagnostics.snapshot();
        assert_eq!(diagnostics.car_requested_cids, 3);
        assert_eq!(diagnostics.car_present_cids, 2);
        assert_eq!(diagnostics.car_served_cids, u64::from(allowed));
        assert_eq!(diagnostics.car_filtered_cids, u64::from(!allowed));
    }
}
