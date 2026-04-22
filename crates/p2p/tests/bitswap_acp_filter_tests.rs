//! End-to-end tests for per-peer Bitswap access control (#830).
//!
//! Spins up two real `P2PHost` instances over loopback TCP with the
//! `make_peer_block_access_filter` installed at the producer side, then
//! issues Bitswap WANTs from the consumer for both allowed and denied
//! block/peer pairs.

use std::time::Duration;

use defra_core::{Block as DefraBlock, CompositeDeltaPayload, CrdtDelta};
use p2p::testutil::MockBitswapStore;
use p2p::{bitswap::AccessMode, HostEvent, P2PHost, P2PHostConfig};
use tokio::time::timeout;

const COLLECTION_USERS: &str = "bafyusers";
const COLLECTION_POSTS: &str = "bafyposts";

fn make_data_block(collection_id: &str, doc_id: &[u8]) -> (cid::Cid, Vec<u8>) {
    let payload = CompositeDeltaPayload {
        doc_id: doc_id.to_vec(),
        priority: 1,
        schema_version_id: collection_id.to_string(),
        status: 1,
    };
    let delta = CrdtDelta::Composite(payload);
    let block = DefraBlock::new(delta, Vec::new(), Vec::new());
    let bytes = block.to_dag_cbor().unwrap();
    let cid = defra_core::block::generate_cid_from_bytes(&bytes).unwrap();
    (cid, bytes)
}

async fn wait_connected(handle: &p2p::P2PHostHandle, target: libp2p::PeerId) {
    let start = std::time::Instant::now();
    while !handle
        .connected_peers()
        .await
        .unwrap_or_default()
        .contains(&target)
    {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "timed out waiting to connect to {target}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn controlled_config() -> P2PHostConfig {
    P2PHostConfig {
        access_mode: AccessMode::Controlled,
        ..P2PHostConfig::default()
    }
}

async fn drain_until_complete(
    events: &mut tokio::sync::mpsc::Receiver<HostEvent>,
    query: p2p::QueryId,
    wait: Duration,
) -> (bool, bool) {
    // Returns (received_block, completed_success).
    let deadline = std::time::Instant::now() + wait;
    let mut got_block = false;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return (got_block, false);
        }
        match timeout(remaining, events.recv()).await {
            Ok(Some(HostEvent::BitswapBlockReceived { query_id, .. })) if query_id == query => {
                got_block = true;
            }
            Ok(Some(HostEvent::BitswapComplete {
                query_id, success, ..
            })) if query_id == query => {
                return (got_block, success);
            }
            Ok(Some(_)) => continue,
            Ok(None) | Err(_) => return (got_block, false),
        }
    }
}

#[tokio::test]
async fn controlled_mode_denies_unregistered_bitswap_peer() {
    let (cid, bytes) = make_data_block(COLLECTION_USERS, b"alice");

    let producer_store = MockBitswapStore::new().with_block(cid, bytes);
    let consumer_store = MockBitswapStore::new();

    let (producer, producer_handle, _producer_events, producer_replicators) =
        P2PHost::with_config(producer_store, controlled_config())
            .await
            .unwrap();
    let (consumer, consumer_handle, mut consumer_events, _consumer_replicators) =
        P2PHost::with_config(consumer_store, controlled_config())
            .await
            .unwrap();

    let producer_peer = producer.local_peer_id();
    let consumer_peer = consumer.local_peer_id();

    tokio::spawn(producer.run());
    tokio::spawn(consumer.run());

    producer_handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let producer_addr = producer_handle.listen_addresses().await.unwrap().remove(0);

    consumer_handle
        .dial(producer_peer, vec![producer_addr])
        .await
        .unwrap();
    wait_connected(&consumer_handle, producer_peer).await;
    wait_connected(&producer_handle, consumer_peer).await;

    // Explicitly leave the consumer out of the producer's replicator
    // registry — the filter must deny.
    assert!(!producer_replicators.is_replicator(COLLECTION_USERS, &consumer_peer.to_string()));

    let query = consumer_handle
        .bitswap_sync(cid, vec![producer_peer], vec![cid])
        .await
        .unwrap();

    let (got_block, completed_success) =
        drain_until_complete(&mut consumer_events, query, Duration::from_secs(2)).await;

    assert!(
        !got_block,
        "Bitswap must not deliver a block to an unregistered peer in Controlled mode"
    );
    assert!(
        !completed_success,
        "Bitswap sync must not report success without delivering the block"
    );
    let _ = consumer_handle.bitswap_cancel(query).await;

    consumer_handle.shutdown().await.ok();
    producer_handle.shutdown().await.ok();
}

#[tokio::test]
async fn controlled_mode_serves_registered_replicator() {
    let (cid, bytes) = make_data_block(COLLECTION_USERS, b"alice");

    let producer_store = MockBitswapStore::new().with_block(cid, bytes);
    let consumer_store = MockBitswapStore::new();

    let (producer, producer_handle, _producer_events, producer_replicators) =
        P2PHost::with_config(producer_store, controlled_config())
            .await
            .unwrap();
    let (consumer, consumer_handle, mut consumer_events, _consumer_replicators) =
        P2PHost::with_config(consumer_store, controlled_config())
            .await
            .unwrap();

    let producer_peer = producer.local_peer_id();
    let consumer_peer = consumer.local_peer_id();

    tokio::spawn(producer.run());
    tokio::spawn(consumer.run());

    producer_handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let producer_addr = producer_handle.listen_addresses().await.unwrap().remove(0);

    consumer_handle
        .dial(producer_peer, vec![producer_addr])
        .await
        .unwrap();
    wait_connected(&consumer_handle, producer_peer).await;
    wait_connected(&producer_handle, consumer_peer).await;

    // Register the consumer as an authorized replicator for the block's
    // collection on the producer side.
    producer_replicators.add_replicator(COLLECTION_USERS, &consumer_peer.to_string());

    let query = consumer_handle
        .bitswap_sync(cid, vec![producer_peer], vec![cid])
        .await
        .unwrap();

    let (got_block, completed_success) =
        drain_until_complete(&mut consumer_events, query, Duration::from_secs(5)).await;

    assert!(got_block, "registered replicator must receive the block");
    assert!(completed_success, "sync must complete successfully");

    consumer_handle.shutdown().await.ok();
    producer_handle.shutdown().await.ok();
}

#[tokio::test]
async fn controlled_mode_denies_replicator_for_other_collection() {
    let (cid, bytes) = make_data_block(COLLECTION_USERS, b"alice");

    let producer_store = MockBitswapStore::new().with_block(cid, bytes);
    let consumer_store = MockBitswapStore::new();

    let (producer, producer_handle, _producer_events, producer_replicators) =
        P2PHost::with_config(producer_store, controlled_config())
            .await
            .unwrap();
    let (consumer, consumer_handle, mut consumer_events, _consumer_replicators) =
        P2PHost::with_config(consumer_store, controlled_config())
            .await
            .unwrap();

    let producer_peer = producer.local_peer_id();
    let consumer_peer = consumer.local_peer_id();

    tokio::spawn(producer.run());
    tokio::spawn(consumer.run());

    producer_handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let producer_addr = producer_handle.listen_addresses().await.unwrap().remove(0);

    consumer_handle
        .dial(producer_peer, vec![producer_addr])
        .await
        .unwrap();
    wait_connected(&consumer_handle, producer_peer).await;
    wait_connected(&producer_handle, consumer_peer).await;

    // Authorize the consumer for a DIFFERENT collection than the one the
    // requested block belongs to.
    producer_replicators.add_replicator(COLLECTION_POSTS, &consumer_peer.to_string());

    let query = consumer_handle
        .bitswap_sync(cid, vec![producer_peer], vec![cid])
        .await
        .unwrap();

    let (got_block, completed_success) =
        drain_until_complete(&mut consumer_events, query, Duration::from_secs(2)).await;

    assert!(
        !got_block,
        "replicator for collection A must not fetch blocks from collection B"
    );
    assert!(!completed_success);
    let _ = consumer_handle.bitswap_cancel(query).await;

    consumer_handle.shutdown().await.ok();
    producer_handle.shutdown().await.ok();
}

/// Live Go interop sibling of `controlled_mode_denies_unregistered_bitswap_peer`.
///
/// Proves that Go DefraDB's `hasAccess` filter (`go-p2p/peer.go:200-211`)
/// and Rust's new `make_peer_block_access_filter` agree on the "deny
/// unauthorized peer" case. Skipped by default — run against a Go defradb
/// instance configured with ACP and at least one stored block:
///
/// ```sh
/// defradb start --development --store memory \
///   --p2paddr /ip4/127.0.0.1/tcp/9391 --url 127.0.0.1:9392 \
///   --no-keyring --no-signing --rootdir /tmp/defradb-go-data &
///
/// # Apply an ACP policy + schema, add a document (see #828 live test).
/// # Then export a CID from the Go node's blockstore to fetch:
/// export DEFRADB_GO_PEER=/ip4/127.0.0.1/tcp/9391/p2p/12D3KooW...
/// export DEFRADB_GO_BLOCK_CID=bafyrei...
/// cargo test -p p2p --test bitswap_acp_filter_tests \
///   -- --ignored --nocapture bitswap_unauthorized_against_go_defradb
/// ```
///
/// Expected: Go rejects the Bitswap WANT (no block received in 5s) because
/// our Rust peer is not registered as a replicator for the owning
/// collection — the same deny behavior this PR now implements in Rust.
#[tokio::test]
#[ignore = "requires a pre-running Go defradb with DEFRADB_GO_PEER / DEFRADB_GO_BLOCK_CID set"]
async fn bitswap_unauthorized_against_go_defradb() {
    let peer_addr = std::env::var("DEFRADB_GO_PEER")
        .expect("DEFRADB_GO_PEER must be set to the Go peer multiaddr");
    let block_cid_str = std::env::var("DEFRADB_GO_BLOCK_CID")
        .expect("DEFRADB_GO_BLOCK_CID must be set to a CID owned by an ACP'd collection");

    let store = MockBitswapStore::new();
    let (host, handle, mut events, _replicators) =
        P2PHost::with_config(store, P2PHostConfig::default())
            .await
            .unwrap();
    tokio::spawn(host.run());

    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();

    let multi: libp2p::Multiaddr = peer_addr.parse().expect("parse multiaddr");
    let mut go_peer: Option<libp2p::PeerId> = None;
    let mut stripped = libp2p::Multiaddr::empty();
    for proto in multi.iter() {
        match proto {
            libp2p::multiaddr::Protocol::P2p(pid) => go_peer = Some(pid),
            other => stripped.push(other),
        }
    }
    let go_peer = go_peer.expect("multiaddr missing /p2p/<peer>");

    handle.dial(go_peer, vec![stripped]).await.unwrap();
    wait_connected(&handle, go_peer).await;
    eprintln!("connected to Go peer {go_peer}");

    let cid: cid::Cid = block_cid_str.parse().expect("parse DEFRADB_GO_BLOCK_CID");
    let query = handle
        .bitswap_sync(cid, vec![go_peer], vec![cid])
        .await
        .unwrap();

    let (got_block, completed_success) =
        drain_until_complete(&mut events, query, Duration::from_secs(5)).await;

    eprintln!("got_block={got_block} completed_success={completed_success}");
    assert!(
        !got_block,
        "Go must not serve ACP-protected block to an unregistered Rust peer"
    );

    let _ = handle.bitswap_cancel(query).await;
    handle.shutdown().await.ok();
}

#[tokio::test]
async fn open_mode_serves_all_peers() {
    let (cid, bytes) = make_data_block(COLLECTION_USERS, b"alice");

    let producer_store = MockBitswapStore::new().with_block(cid, bytes);
    let consumer_store = MockBitswapStore::new();

    // Producer in Open mode should serve the block regardless of registry
    // state. Matches the pre-#830 default / AccessMode::Open contract.
    let (producer, producer_handle, _producer_events, _producer_replicators) =
        P2PHost::with_config(producer_store, P2PHostConfig::default())
            .await
            .unwrap();
    let (consumer, consumer_handle, mut consumer_events, _consumer_replicators) =
        P2PHost::with_config(consumer_store, P2PHostConfig::default())
            .await
            .unwrap();

    let producer_peer = producer.local_peer_id();
    let consumer_peer = consumer.local_peer_id();

    tokio::spawn(producer.run());
    tokio::spawn(consumer.run());

    producer_handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let producer_addr = producer_handle.listen_addresses().await.unwrap().remove(0);

    consumer_handle
        .dial(producer_peer, vec![producer_addr])
        .await
        .unwrap();
    wait_connected(&consumer_handle, producer_peer).await;
    wait_connected(&producer_handle, consumer_peer).await;

    let query = consumer_handle
        .bitswap_sync(cid, vec![producer_peer], vec![cid])
        .await
        .unwrap();

    let (got_block, completed_success) =
        drain_until_complete(&mut consumer_events, query, Duration::from_secs(5)).await;
    assert!(got_block, "Open mode must serve blocks to any peer");
    assert!(completed_success);

    consumer_handle.shutdown().await.ok();
    producer_handle.shutdown().await.ok();
}
