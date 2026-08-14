//! The KV adapter: key layout, value codec, and a graph round-tripped through
//! an actual store.
//!
//! The golden byte strings below were produced by the Go implementation itself,
//! not re-derived from its source. They come from a throwaway package inside a
//! worktree of `sourcenetwork/defradb` PR 5096 calling `keys.NewVector*Key`,
//! `hnsw.MarshalNode` and `hnsw.MarshalMeta` and printing hex. Reproduce with a
//! `go test` that does the same; the point is that a re-reading of the Go
//! source cannot drift these, because they are its output.

use db_index::error::Error;
use db_index::vector::codec::{decode_meta, decode_node, encode_meta, encode_node};
use db_index::vector::core::Metric;
use db_index::vector::engine::hnsw::Hnsw;
use db_index::vector::kv_store::KvNodeStore;
use db_index::vector::params::{Params, DEFAULT_M};
use db_index::vector::store::{Meta, Node, NodeId, VectorNodeStore};
use storage::backends::MemoryStore;
use storage::corekv::{Key, Reader, Store, Txn, Writer};
use storage::keys::datastore::VectorIndexKey;

const COLLECTION: u32 = 7;
const INDEX: u32 = 3;
const EPOCH: u32 = 2;
const SEED: u64 = 0x0000_1234_5678_9ABC;

/// A fresh writable transaction. `Box<dyn Txn>` is what `Store::new_txn`
/// hands back, and it satisfies `Reader + Writer` through the blanket impls,
/// so it is exactly the shape a real caller threads into the adapter.
async fn txn(store: &MemoryStore) -> Box<dyn Txn> {
    store.new_txn(false).await.unwrap()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn node_fixture() -> Node {
    Node {
        id: NodeId(42),
        vector: vec![1.5, -2.25, 0.0],
        layers: vec![vec![NodeId(7), NodeId(9)], Vec::new(), vec![NodeId(1)]],
        deleted: true,
    }
}

#[test]
fn keys_match_the_go_layout() {
    assert_eq!(
        hex(&VectorIndexKey::meta(COLLECTION, INDEX, EPOCH).bytes()),
        "2f8f2f8b2f8a2f6d"
    );
    assert_eq!(
        hex(&VectorIndexKey::node(COLLECTION, INDEX, EPOCH, 42).bytes()),
        "2f8f2f8b2f8a2f6e2fb2"
    );
    assert_eq!(
        hex(&VectorIndexKey::node_prefix(COLLECTION, INDEX, EPOCH).bytes()),
        "2f8f2f8b2f8a2f6e"
    );
    // Components wide enough to need multi-byte uvarints, so the encoding is
    // exercised past its single-byte fast path.
    assert_eq!(
        hex(&VectorIndexKey::node(300, 70_000, 1, 1 << 40).bytes()),
        "2ff7012c2ff80111702f892f6e2ffb010000000000"
    );
}

#[test]
fn values_match_the_go_codec() {
    assert_eq!(
        hex(&encode_node(&node_fixture())),
        "012a0000000000000001030000000000c03f000010c000000000030000000200000007000000000000000900000000000000000000000100000001000000\
00000000"
    );
    assert_eq!(
        hex(&encode_node(&Node {
            id: NodeId(1),
            vector: Vec::new(),
            layers: Vec::new(),
            deleted: false,
        })),
        "010100000000000000000000000000000000"
    );
    assert_eq!(
        hex(&encode_meta(&Meta {
            entry_point: NodeId(42),
            top_layer: 3,
        })),
        "012a0000000000000003000000"
    );
}

#[test]
fn values_round_trip() {
    let node = node_fixture();
    assert_eq!(decode_node(&encode_node(&node)).unwrap(), node);

    // An empty layer and an empty vector are both representable and must not
    // collapse into each other.
    for node in [
        Node {
            id: NodeId(1),
            vector: Vec::new(),
            layers: vec![Vec::new()],
            deleted: false,
        },
        Node {
            id: NodeId(u64::MAX),
            vector: vec![f32::MIN, f32::MAX, f32::EPSILON, -0.0],
            layers: Vec::new(),
            deleted: false,
        },
    ] {
        assert_eq!(decode_node(&encode_node(&node)).unwrap(), node);
    }

    let meta = Meta {
        entry_point: NodeId(u64::MAX),
        top_layer: 0,
    };
    assert_eq!(decode_meta(&encode_meta(&meta)).unwrap(), meta);
}

/// Negative zero must survive: it is a distinct bit pattern, and a codec that
/// round-trips it as `+0.0` is silently rewriting stored data.
#[test]
fn negative_zero_survives_the_codec() {
    let node = Node {
        id: NodeId(1),
        vector: vec![-0.0, 0.0],
        layers: Vec::new(),
        deleted: false,
    };
    let decoded = decode_node(&encode_node(&node)).unwrap();
    assert!(decoded.vector[0].is_sign_negative());
    assert!(decoded.vector[1].is_sign_positive());
}

/// A truncated or corrupt value is an error, never a panic and never a huge
/// allocation from a length field that outruns the buffer.
#[test]
fn corrupt_values_are_rejected() {
    let good = encode_node(&node_fixture());
    for cut in 0..good.len() {
        let err = decode_node(&good[..cut]);
        assert!(err.is_err(), "a node truncated to {cut} bytes decoded");
    }

    let mut wrong_version = good.clone();
    wrong_version[0] = 0xFF;
    assert!(decode_node(&wrong_version).is_err());

    // A length prefix claiming far more than the buffer holds.
    let mut huge_vector = good.clone();
    huge_vector[10..14].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_node(&huge_vector).is_err());

    let meta = encode_meta(&Meta {
        entry_point: NodeId(1),
        top_layer: 1,
    });
    assert!(decode_meta(&meta[..meta.len() - 1]).is_err());
    assert!(decode_meta(&[&meta[..], &[0u8]].concat()).is_err());
    let mut negative_layer = meta.clone();
    negative_layer[9..13].copy_from_slice(&(-1i32).to_le_bytes());
    assert!(decode_meta(&negative_layer).is_err());
}

#[tokio::test]
async fn a_graph_round_trips_through_the_store() {
    let store = MemoryStore::new();
    let vectors: Vec<Vec<f32>> = (0..200)
        .map(|i| {
            let a = i as f32 * 0.37;
            vec![a.sin(), a.cos(), (a * 0.5).sin(), (a * 0.25).cos()]
        })
        .collect();

    let mut write = txn(&store).await;
    {
        let mut index = Hnsw::new(
            KvNodeStore::new(&mut write, COLLECTION, INDEX, EPOCH),
            Metric::Cosine,
            Params::new(DEFAULT_M),
            SEED,
        );
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(NodeId(i as u64), vector).await.unwrap();
        }
    }
    write.commit().await.unwrap();

    // A second transaction, so the graph is read back from the store rather
    // than from the writer's own buffer.
    let mut txn = txn(&store).await;
    let reopened = Hnsw::new(
        KvNodeStore::new(&mut txn, COLLECTION, INDEX, EPOCH),
        Metric::Cosine,
        Params::new(DEFAULT_M),
        SEED,
    );
    for (i, vector) in vectors.iter().enumerate() {
        let hits = reopened.search_with_ef(vector, 1, 64).await.unwrap();
        assert_eq!(
            hits.first().map(|h| h.id),
            Some(NodeId(i as u64)),
            "vector {i} was not retrievable after a reopen"
        );
    }
}

/// A node written by the engine must come back byte-identical, not merely
/// equal after a lossy re-encode.
#[tokio::test]
async fn stored_nodes_are_byte_identical() {
    let store = MemoryStore::new();
    let node = node_fixture();

    let mut write = txn(&store).await;
    KvNodeStore::new(&mut write, COLLECTION, INDEX, EPOCH)
        .put_node(node.clone())
        .await
        .unwrap();
    write.commit().await.unwrap();

    let mut read = txn(&store).await;
    let raw = read
        .get(&VectorIndexKey::node(COLLECTION, INDEX, EPOCH, node.id.0).bytes())
        .await
        .unwrap()
        .expect("the node was written under the Go key layout");
    assert_eq!(raw, encode_node(&node));
    assert_eq!(
        KvNodeStore::new(&mut read, COLLECTION, INDEX, EPOCH)
            .get_node(node.id)
            .await
            .unwrap(),
        Some(node)
    );
}

/// The node prefix must select exactly one epoch of one index: not the meta
/// key, not a neighbouring epoch, not a neighbouring index or collection.
#[tokio::test]
async fn a_node_scan_sees_one_epoch_only() {
    let store = MemoryStore::new();
    let mut txn = txn(&store).await;

    let elsewhere = [
        (COLLECTION, INDEX, EPOCH + 1),
        (COLLECTION, INDEX + 1, EPOCH),
        (COLLECTION + 1, INDEX, EPOCH),
    ];
    for (collection, index, epoch) in elsewhere {
        let mut kv = KvNodeStore::new(&mut txn, collection, index, epoch);
        for id in 0..5u64 {
            kv.put_node(Node::new(NodeId(1000 + id), vec![1.0], 0))
                .await
                .unwrap();
        }
        kv.put_meta(Meta {
            entry_point: NodeId(1000),
            top_layer: 0,
        })
        .await
        .unwrap();
    }

    let mut kv = KvNodeStore::new(&mut txn, COLLECTION, INDEX, EPOCH);
    for id in 0..3u64 {
        kv.put_node(Node::new(NodeId(id + 1), vec![1.0], 0))
            .await
            .unwrap();
    }
    kv.put_meta(Meta {
        entry_point: NodeId(1),
        top_layer: 0,
    })
    .await
    .unwrap();

    let mut seen = Vec::new();
    kv.iterate_nodes(|node| {
        seen.push(node.id);
        Ok(())
    })
    .await
    .unwrap();
    seen.sort();
    assert_eq!(seen, vec![NodeId(1), NodeId(2), NodeId(3)]);
}

/// Keys must sort so that a prefix scan is contiguous: every node key of an
/// epoch sits together, above that epoch's meta key and below the next epoch.
#[test]
fn keys_sort_into_contiguous_epochs() {
    let mut keys: Vec<(String, Vec<u8>)> = Vec::new();
    for epoch in 0..3u32 {
        keys.push((
            format!("e{epoch}/m"),
            VectorIndexKey::meta(COLLECTION, INDEX, epoch).bytes(),
        ));
        for id in [1u64, 2, 300, u64::MAX] {
            keys.push((
                format!("e{epoch}/n/{id}"),
                VectorIndexKey::node(COLLECTION, INDEX, epoch, id).bytes(),
            ));
        }
    }
    keys.sort_by(|a, b| a.1.cmp(&b.1));
    let order: Vec<&str> = keys.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        order,
        vec![
            "e0/m",
            "e0/n/1",
            "e0/n/2",
            "e0/n/300",
            "e0/n/18446744073709551615",
            "e1/m",
            "e1/n/1",
            "e1/n/2",
            "e1/n/300",
            "e1/n/18446744073709551615",
            "e2/m",
            "e2/n/1",
            "e2/n/2",
            "e2/n/300",
            "e2/n/18446744073709551615",
        ],
        "epochs must not interleave, and node ids must sort numerically"
    );

    // The prefix is a true prefix of every node key of its epoch, and of
    // nothing else.
    let prefix = VectorIndexKey::node_prefix(COLLECTION, INDEX, EPOCH).bytes();
    assert!(VectorIndexKey::node(COLLECTION, INDEX, EPOCH, 1)
        .bytes()
        .starts_with(&prefix));
    assert!(!VectorIndexKey::meta(COLLECTION, INDEX, EPOCH)
        .bytes()
        .starts_with(&prefix));
    assert!(!VectorIndexKey::node(COLLECTION, INDEX, EPOCH + 1, 1)
        .bytes()
        .starts_with(&prefix));
}

#[tokio::test]
async fn clearing_an_epoch_leaves_its_neighbours_alone() {
    let store = MemoryStore::new();
    let mut txn = txn(&store).await;

    for epoch in [EPOCH, EPOCH + 1] {
        let mut kv = KvNodeStore::new(&mut txn, COLLECTION, INDEX, epoch);
        for id in 0..2000u64 {
            kv.put_node(Node::new(NodeId(id + 1), vec![1.0], 0))
                .await
                .unwrap();
        }
        kv.put_meta(Meta {
            entry_point: NodeId(1),
            top_layer: 0,
        })
        .await
        .unwrap();
    }

    KvNodeStore::new(&mut txn, COLLECTION, INDEX, EPOCH)
        .clear()
        .await
        .unwrap();

    let kv = KvNodeStore::new(&mut txn, COLLECTION, INDEX, EPOCH);
    assert_eq!(kv.get_meta().await.unwrap(), None);
    let mut count = 0;
    kv.iterate_nodes(|_| {
        count += 1;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(count, 0, "the cleared epoch still has nodes");

    let survivor = KvNodeStore::new(&mut txn, COLLECTION, INDEX, EPOCH + 1);
    assert!(survivor.get_meta().await.unwrap().is_some());
    let mut count = 0;
    survivor
        .iterate_nodes(|_| {
            count += 1;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(count, 2000, "clearing one epoch removed another's nodes");
}

/// A corrupt value reaching the store surfaces as an error, not a panic and not
/// a silently skipped node.
#[tokio::test]
async fn a_corrupt_stored_value_is_an_error() {
    let store = MemoryStore::new();
    let mut txn = txn(&store).await;
    txn.set(
        &VectorIndexKey::node(COLLECTION, INDEX, EPOCH, 1).bytes(),
        b"not a node",
    )
    .await
    .unwrap();

    let kv = KvNodeStore::new(&mut txn, COLLECTION, INDEX, EPOCH);
    assert!(matches!(kv.get_node(NodeId(1)).await, Err(Error::Other(_))));
    assert!(matches!(
        kv.iterate_nodes(|_| Ok(())).await,
        Err(Error::Other(_))
    ));
}
