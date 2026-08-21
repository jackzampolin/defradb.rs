//! The index's private blob space, over both store implementations.

use db::index::vector::kv_store::KvNodeStore;
use db::index::vector::store::{MemoryNodeStore, Node, NodeId, VectorNodeStore};
use storage::backends::MemoryStore;
use storage::corekv::{Store, Txn};

const COLLECTION: u32 = 41;
const INDEX: u32 = 9;
const CENTROID: u8 = b'c';
const CODEBOOK: u8 = b'b';
const LIST: u8 = b'l';

async fn txn(store: &MemoryStore) -> Box<dyn Txn> {
    store.new_txn(false).await.unwrap()
}

async fn collect<S: VectorNodeStore>(
    store: &S,
    kind: u8,
    prefix: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    store
        .iterate_aux(kind, prefix, |key, value| {
            out.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .await
        .unwrap();
    out
}

/// Both implementations answer the same, or the memory store stops being a
/// faithful stand-in for the persisted one.
macro_rules! for_both_stores {
    ($body:ident) => {{
        let backing = MemoryStore::new();
        let mut write = txn(&backing).await;
        let mut kv = KvNodeStore::new(&mut write, COLLECTION, INDEX, 0);
        $body(&mut kv).await;

        let mut memory = MemoryNodeStore::new();
        $body(&mut memory).await;
    }};
}

#[tokio::test]
async fn an_entry_round_trips() {
    async fn body<S: VectorNodeStore>(store: &mut S) {
        store.put_aux(CENTROID, b"7", b"payload").await.unwrap();
        assert_eq!(
            store.get_aux(CENTROID, b"7").await.unwrap(),
            Some(b"payload".to_vec())
        );
        assert_eq!(store.get_aux(CENTROID, b"8").await.unwrap(), None);
    }
    for_both_stores!(body);
}

#[tokio::test]
async fn a_later_write_replaces_an_earlier_one() {
    async fn body<S: VectorNodeStore>(store: &mut S) {
        store.put_aux(CODEBOOK, b"0", b"first").await.unwrap();
        store.put_aux(CODEBOOK, b"0", b"second").await.unwrap();
        assert_eq!(
            store.get_aux(CODEBOOK, b"0").await.unwrap(),
            Some(b"second".to_vec())
        );
    }
    for_both_stores!(body);
}

/// A scan of one kind must not see another's entries, or a list scan would
/// read codebooks as codes.
#[tokio::test]
async fn kinds_do_not_leak_into_each_other() {
    async fn body<S: VectorNodeStore>(store: &mut S) {
        store.put_aux(CENTROID, b"1", b"c1").await.unwrap();
        store.put_aux(CODEBOOK, b"1", b"b1").await.unwrap();
        store.put_aux(LIST, b"1", b"l1").await.unwrap();

        for (kind, expected) in [(CENTROID, "c1"), (CODEBOOK, "b1"), (LIST, "l1")] {
            let seen = collect(store, kind, b"").await;
            assert_eq!(seen.len(), 1, "kind {} saw {seen:?}", kind as char);
            assert_eq!(seen[0].1, expected.as_bytes());
        }
    }
    for_both_stores!(body);
}

/// The inverted list depends on this: a prefix scan of one list id must yield
/// exactly that list.
#[tokio::test]
async fn a_prefix_scan_yields_exactly_its_prefix() {
    async fn body<S: VectorNodeStore>(store: &mut S) {
        for list in 0u8..3 {
            for node in 0u8..4 {
                store
                    .put_aux(LIST, &[list, node], &[list * 10 + node])
                    .await
                    .unwrap();
            }
        }

        for list in 0u8..3 {
            let seen = collect(store, LIST, &[list]).await;
            assert_eq!(seen.len(), 4, "list {list} saw {seen:?}");
            for (key, value) in seen {
                assert_eq!(key[0], list, "a foreign list leaked in");
                assert_eq!(value[0] / 10, list);
            }
        }

        assert_eq!(collect(store, LIST, b"").await.len(), 12);
    }
    for_both_stores!(body);
}

/// The key a scan reports must be the one that was written, not the storage
/// key with its prefix still attached.
#[tokio::test]
async fn a_scan_reports_the_key_as_written() {
    async fn body<S: VectorNodeStore>(store: &mut S) {
        store.put_aux(CENTROID, b"abc", b"v").await.unwrap();
        let seen = collect(store, CENTROID, b"").await;
        assert_eq!(seen, vec![(b"abc".to_vec(), b"v".to_vec())]);

        let narrowed = collect(store, CENTROID, b"a").await;
        assert_eq!(narrowed, vec![(b"abc".to_vec(), b"v".to_vec())]);
    }
    for_both_stores!(body);
}

#[tokio::test]
async fn an_empty_kind_scans_empty() {
    async fn body<S: VectorNodeStore>(store: &mut S) {
        assert!(collect(store, CENTROID, b"").await.is_empty());
    }
    for_both_stores!(body);
}

/// Aux entries and the graph share a keyspace, so neither may disturb the
/// other.
#[tokio::test]
async fn the_graph_and_the_aux_space_are_independent() {
    async fn body<S: VectorNodeStore>(store: &mut S) {
        store
            .put_node(Node::new(NodeId(1), vec![1.0, 0.0], 0))
            .await
            .unwrap();
        store.put_aux(CENTROID, b"1", b"not a node").await.unwrap();

        let node = store.get_node(NodeId(1)).await.unwrap().unwrap();
        assert_eq!(node.vector, vec![1.0, 0.0]);

        let mut nodes = 0;
        store
            .iterate_nodes(|_| {
                nodes += 1;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(nodes, 1, "an aux entry was read as a node");

        assert_eq!(
            store.get_aux(CENTROID, b"1").await.unwrap(),
            Some(b"not a node".to_vec())
        );
    }
    for_both_stores!(body);
}

/// A rebuild writes a new epoch beside the live one, so the two must not see
/// each other's entries.
#[tokio::test]
async fn epochs_are_isolated() {
    let backing = MemoryStore::new();

    let mut write = txn(&backing).await;
    KvNodeStore::new(&mut write, COLLECTION, INDEX, 0)
        .put_aux(CENTROID, b"1", b"epoch zero")
        .await
        .unwrap();
    KvNodeStore::new(&mut write, COLLECTION, INDEX, 1)
        .put_aux(CENTROID, b"1", b"epoch one")
        .await
        .unwrap();
    write.commit().await.unwrap();

    let mut read = txn(&backing).await;
    for (epoch, expected) in [(0u32, "epoch zero"), (1, "epoch one")] {
        let store = KvNodeStore::new(&mut read, COLLECTION, INDEX, epoch);
        assert_eq!(
            store.get_aux(CENTROID, b"1").await.unwrap(),
            Some(expected.as_bytes().to_vec())
        );
        assert_eq!(collect(&store, CENTROID, b"").await.len(), 1);
    }
}

/// Two indexes on one collection share the keyspace too.
#[tokio::test]
async fn indexes_are_isolated() {
    let backing = MemoryStore::new();

    let mut write = txn(&backing).await;
    KvNodeStore::new(&mut write, COLLECTION, 1, 0)
        .put_aux(LIST, b"k", b"one")
        .await
        .unwrap();
    KvNodeStore::new(&mut write, COLLECTION, 2, 0)
        .put_aux(LIST, b"k", b"two")
        .await
        .unwrap();
    write.commit().await.unwrap();

    let mut read = txn(&backing).await;
    let first = KvNodeStore::new(&mut read, COLLECTION, 1, 0);
    assert_eq!(collect(&first, LIST, b"").await.len(), 1);
    assert_eq!(first.get_aux(LIST, b"k").await.unwrap().unwrap(), b"one");
}
