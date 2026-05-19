//! Live Go↔Rust pubsub_rpc parity test (#828).
//!
//! Spins up a Rust libp2p host + SyncCoordinator, dials a pre-running
//! Go DefraDB node, and issues a real `pubsub_sync_documents` call over
//! gossipsub. The Go node's `go-libp2p-pubsub-rpc` handler serves the
//! request and we verify the reply decodes into a Rust `DocSyncReply`.
//!
//! Skipped by default because it needs an external Go node. Enable with:
//!
//! ```sh
//! # 1. Start Go defradb with User collection + Alice doc:
//! defradb start --development --store memory \
//!   --p2paddr /ip4/127.0.0.1/tcp/9391 --url 127.0.0.1:9392 \
//!   --no-keyring --no-signing --rootdir /tmp/defradb-go-data &
//! curl -X POST http://127.0.0.1:9392/api/v0/collections \
//!   -H "Content-Type: text/plain" -d 'type User @branchable { name: String }'
//! curl -X POST http://127.0.0.1:9392/api/v0/graphql -H "Content-Type: application/json" \
//!   -d '{"query":"mutation{add_User(input:{name:\"Alice\"}){_docID}}"}'
//! defradb client p2p collection add User --url 127.0.0.1:9392
//!
//! # 2. Export the peer multiaddr + doc id + collection id + run:
//! export DEFRADB_GO_PEER="/ip4/127.0.0.1/tcp/9391/p2p/12D3KooW..."
//! export DEFRADB_GO_DOC_ID="bae-..."
//! export DEFRADB_GO_COLLECTION_ID="bafyreib..."
//! cargo test -p p2p --test live_go_pubsub_rpc -- --ignored --nocapture
//! ```
//!
//! The go-compat CI matrix exercises the same pathway via the Rust
//! integration-test harness (`tools/integration-test/tests/p2p/sync.rs`)
//! which launches both nodes under test; this file is the bare-metal
//! variant useful for ad-hoc diagnosis.

use std::sync::Arc;
use std::time::Duration;

use blockstore::DefraBlockstore;
use p2p::host::libp2p_transport::{convert_host_event, Libp2pTransport};
use p2p::testutil::MockBitswapStore;
use p2p::P2PHost;
use storage::backends::MemoryStore;

type TestBlockstore = DefraBlockstore<MemoryStore>;

#[tokio::test]
#[ignore = "requires a pre-running Go defradb with DEFRADB_GO_PEER / DEFRADB_GO_DOC_ID set"]
async fn doc_sync_against_go_defradb() {
    let peer_addr = std::env::var("DEFRADB_GO_PEER")
        .expect("DEFRADB_GO_PEER must be set to the Go peer multiaddr");
    let doc_id = std::env::var("DEFRADB_GO_DOC_ID")
        .expect("DEFRADB_GO_DOC_ID must be set to a docID the Go node has");

    let store = MockBitswapStore::new();
    let (host, handle, events, _replicators) = P2PHost::new(store).await.unwrap();
    tokio::spawn(host.run());
    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();

    let multi: libp2p::Multiaddr = peer_addr.parse().expect("parse multiaddr");
    let go_peer_id = extract_peer_id(&multi).expect("multiaddr contains /p2p/<peer>");

    handle
        .dial(go_peer_id, vec![strip_p2p_component(multi.clone())])
        .await
        .expect("dial Go peer");
    wait_connected(&handle, go_peer_id).await;
    eprintln!("connected to Go peer {go_peer_id}");

    let transport = Libp2pTransport::new(handle.clone());
    let blockstore = Arc::new(TestBlockstore::new(Arc::new(MemoryStore::new()), true));
    let (coord, _sync_events) =
        p2p::sync::SyncCoordinator::new(transport, blockstore, p2p::sync::SyncConfig::default())
            .await
            .expect("build coordinator");
    let coord = Arc::new(coord);

    let coord_pump = Arc::clone(&coord);
    tokio::spawn(async move {
        let mut events = events;
        while let Some(host_event) = events.recv().await {
            let _ = coord_pump
                .handle_transport_event(convert_host_event(host_event))
                .await;
        }
    });

    coord
        .start_pubsub_services()
        .await
        .expect("start pubsub services");
    eprintln!("pubsub services started");

    // Give gossipsub a moment to form a mesh on the doc-sync topic.
    tokio::time::sleep(Duration::from_secs(2)).await;

    eprintln!("issuing pubsub_sync_documents for doc {doc_id}");
    let results = coord
        .pubsub_sync_documents(vec![doc_id.clone()], Some(Duration::from_secs(8)))
        .await
        .expect("pubsub_sync_documents call");

    eprintln!("received {} replies:", results.len());
    for (peer, reply) in &results {
        eprintln!(
            "  from={peer} sender={} results={}",
            reply.sender,
            reply.results.len()
        );
        for item in &reply.results {
            eprintln!("    doc_id={} heads={}", item.doc_id, item.heads.len());
        }
    }

    assert!(
        !results.is_empty(),
        "expected at least one reply from Go peer"
    );
    let (_peer, reply) = &results[0];
    assert_eq!(
        reply.results.len(),
        1,
        "Go node should have heads for the known doc"
    );
    assert_eq!(reply.results[0].doc_id, doc_id);

    handle.shutdown().await.ok();
}

#[tokio::test]
#[ignore = "requires a pre-running Go defradb with DEFRADB_GO_PEER / DEFRADB_GO_COLLECTION_ID set"]
async fn branchable_sync_against_go_defradb() {
    let peer_addr = std::env::var("DEFRADB_GO_PEER")
        .expect("DEFRADB_GO_PEER must be set to the Go peer multiaddr");
    let collection_id = std::env::var("DEFRADB_GO_COLLECTION_ID")
        .expect("DEFRADB_GO_COLLECTION_ID must be set to a branchable collection id");

    let store = MockBitswapStore::new();
    let (host, handle, events, _replicators) = P2PHost::new(store).await.unwrap();
    tokio::spawn(host.run());
    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();

    let multi: libp2p::Multiaddr = peer_addr.parse().unwrap();
    let go_peer_id = extract_peer_id(&multi).unwrap();
    handle
        .dial(go_peer_id, vec![strip_p2p_component(multi.clone())])
        .await
        .unwrap();
    wait_connected(&handle, go_peer_id).await;
    eprintln!("connected to Go peer {go_peer_id}");

    let transport = Libp2pTransport::new(handle.clone());
    let blockstore = Arc::new(TestBlockstore::new(Arc::new(MemoryStore::new()), true));
    let (coord, _sync_events) =
        p2p::sync::SyncCoordinator::new(transport, blockstore, p2p::sync::SyncConfig::default())
            .await
            .unwrap();
    let coord = Arc::new(coord);

    let coord_pump = Arc::clone(&coord);
    tokio::spawn(async move {
        let mut events = events;
        while let Some(host_event) = events.recv().await {
            let _ = coord_pump
                .handle_transport_event(convert_host_event(host_event))
                .await;
        }
    });

    coord.start_pubsub_services().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    eprintln!("issuing pubsub_sync_branchable_collection for {collection_id}");
    let reply = coord
        .pubsub_sync_branchable_collection(
            collection_id.clone(),
            Some(Duration::from_secs(8)),
            Some(1),
        )
        .await
        .expect("call succeeded");

    match reply.first() {
        Some((peer, r)) => eprintln!(
            "reply from {peer}: collection={} heads={} sender={}",
            r.collection_id,
            r.heads.len(),
            r.sender,
        ),
        None => eprintln!("no reply"),
    }

    let (_peer, r) = reply.first().expect("expected a reply from Go peer");
    assert_eq!(r.collection_id, collection_id);
    assert!(
        !r.heads.is_empty(),
        "branchable collection should have at least one head"
    );

    handle.shutdown().await.ok();
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
            start.elapsed() < Duration::from_secs(10),
            "timed out waiting to connect to Go peer"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn extract_peer_id(addr: &libp2p::Multiaddr) -> Option<libp2p::PeerId> {
    for proto in addr.iter() {
        if let libp2p::multiaddr::Protocol::P2p(pid) = proto {
            return Some(pid);
        }
    }
    None
}

fn strip_p2p_component(addr: libp2p::Multiaddr) -> libp2p::Multiaddr {
    let mut out = libp2p::Multiaddr::empty();
    for proto in addr.iter() {
        if !matches!(proto, libp2p::multiaddr::Protocol::P2p(_)) {
            out.push(proto);
        }
    }
    out
}
