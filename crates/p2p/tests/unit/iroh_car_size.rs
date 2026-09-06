use super::*;
use crate::sync::car::{decode_car_oversized, encode_car_response, CAR_MAX_BYTES};
use multihash_codetable::{Code, MultihashDigest};
use tokio::sync::oneshot;

async fn serve(endpoint: Endpoint, response: Vec<u8>, ready: Option<oneshot::Receiver<()>>) {
    let connection = endpoint.accept().await.unwrap().await.unwrap();
    let (mut send, mut recv) = connection.accept_bi().await.unwrap();
    recv.read_to_end(4096).await.unwrap();
    if let Some(ready) = ready {
        ready.await.unwrap();
    }
    send.write_all(&response).await.unwrap();
    send.finish().unwrap();
    let _ = connection.closed().await;
}

#[tokio::test]
async fn size_notice_does_not_cancel_alternate_provider_or_emit_generic_failure() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for alternate in [false, true] {
            let client = tests::localhost_endpoint(vec![]).await;
            let limited = tests::localhost_endpoint(vec![protocols::ALPN_CAR.to_vec()]).await;
            let healthy = tests::localhost_endpoint(vec![protocols::ALPN_CAR.to_vec()]).await;
            let root = cid::Cid::new_v1(0x71, Code::Sha2_256.digest(b"block"));
            let notice = encode_car_response(&[root], &[], &[(root, CAR_MAX_BYTES + 1)]).unwrap();
            let limited_task = tokio::spawn(serve(limited.clone(), notice, None));
            let (ready_tx, ready_rx) = oneshot::channel();
            let healthy_task = alternate.then(|| {
                tokio::spawn(serve(
                    healthy.clone(),
                    encode_car_response(&[root], &[(&root, b"block")], &[]).unwrap(),
                    Some(ready_rx),
                ))
            });
            let cache = new_connection_cache();
            let peer_map = Arc::new(parking_lot::Mutex::new(PeerMap::new()));
            let endpoints = if alternate {
                vec![&limited, &healthy]
            } else {
                vec![&limited]
            };
            let mut providers = Vec::new();
            for endpoint in endpoints {
                let provider = PeerId::new(endpoint.id().to_string());
                let addr = endpoint.addr().ip_addrs().next().copied().unwrap();
                let connection =
                    connect_with_cache(&client, &provider, protocols::ALPN_CAR, Some(addr), &cache)
                        .await
                        .unwrap();
                peer_map
                    .lock()
                    .increment_connections(endpoint.id(), Some(addr), connection);
                providers.push(provider);
            }
            let (tx, mut rx) = mpsc::channel(8);
            let task = tokio::spawn(handle_block_sync(
                BlockSyncResources::new(client.clone(), peer_map, cache, tx),
                QueryId(88),
                root,
                providers,
                vec![root],
            ));
            let Some(TransportEvent::CarFetchResponse {
                query_id, car_data, ..
            }) = rx.recv().await
            else {
                panic!("expected size-limit CAR response");
            };
            assert_eq!(query_id, Some(QueryId(88)));
            assert_eq!(
                decode_car_oversized(&car_data).unwrap(),
                vec![(root, CAR_MAX_BYTES + 1)]
            );
            if alternate {
                ready_tx.send(()).unwrap();
                let Some(TransportEvent::CarFetchResponse { car_data, .. }) = rx.recv().await
                else {
                    panic!("size-limited response must not cancel the healthy provider");
                };
                assert!(crate::sync::car::car_has_any_block(&car_data));
                assert!(decode_car_oversized(&car_data).unwrap().is_empty());
            }
            task.await.unwrap();
            assert!(
                rx.try_recv().is_err(),
                "no generic failure may overwrite the size notice"
            );
            client.close().await;
            limited.close().await;
            healthy.close().await;
            limited_task.abort();
            if let Some(task) = healthy_task {
                task.abort();
            }
        }
    })
    .await
    .expect("CAR providers should finish promptly");
}
