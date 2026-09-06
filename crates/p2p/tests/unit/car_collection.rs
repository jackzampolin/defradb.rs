use std::sync::Arc;

use blockstore::{Blockstore, DefraBlockstore};
use cid::Cid;
use defra_core::{Block, CrdtDelta, DAGLink, LwwDeltaPayload};
use multihash_codetable::{Code, MultihashDigest};
use storage::RegolithStore;

use super::{collect_dag_blocks_from_roots, collect_exact_blocks, CAR_MAX_BLOCKS, CAR_MAX_BYTES};

fn blockstore() -> DefraBlockstore<RegolithStore> {
    DefraBlockstore::new(Arc::new(RegolithStore::in_memory().unwrap()), true)
}

async fn put(store: &impl Blockstore, data: &[u8]) -> Cid {
    let cid = Cid::new_v1(0x71, Code::Sha2_256.digest(data));
    store.put(&cid, data).await.unwrap();
    cid
}

#[tokio::test]
async fn oversized_block_does_not_hide_later_requested_blocks() {
    let store = blockstore();
    let oversized = put(&store, &vec![0; CAR_MAX_BYTES + 1]).await;
    let small = put(&store, b"small").await;

    for recursive in [false, true] {
        let outcome = if recursive {
            collect_dag_blocks_from_roots(&store, &[oversized, small]).await
        } else {
            collect_exact_blocks(&store, &[oversized, small]).await
        }
        .unwrap();

        assert_eq!(outcome.blocks.len(), 1, "recursive={recursive}");
        assert_eq!(outcome.blocks[0].0, small);
        assert!(outcome.truncated_by_bytes);
        assert_eq!(
            outcome.oversized_blocks,
            vec![(oversized, CAR_MAX_BYTES + 1)]
        );
        assert_eq!(outcome.blockstore_hits, 2);
        assert_eq!(outcome.blockstore_misses, 0);
    }
}

#[tokio::test]
async fn aggregate_byte_limit_does_not_hide_smaller_blocks() {
    let store = blockstore();
    let large = put(&store, &vec![0; CAR_MAX_BYTES - 5]).await;
    let deferred = put(&store, b"deferred").await;
    let small = put(&store, b"small").await;

    for recursive in [false, true] {
        let outcome = if recursive {
            collect_dag_blocks_from_roots(&store, &[large, deferred, small]).await
        } else {
            collect_exact_blocks(&store, &[large, deferred, small]).await
        }
        .unwrap();
        assert_eq!(outcome.blocks.len(), 2);
        assert_eq!(outcome.blocks[1].0, small);
        assert_eq!(
            outcome
                .blocks
                .iter()
                .map(|(_, data)| data.len())
                .sum::<usize>(),
            CAR_MAX_BYTES
        );
        assert!(outcome.truncated_by_bytes);
        assert!(outcome.oversized_blocks.is_empty());

        let retry = collect_exact_blocks(&store, &[deferred]).await.unwrap();
        assert_eq!(retry.blocks[0].0, deferred);
        assert!(!retry.truncated());
    }
}

#[tokio::test]
async fn single_block_at_byte_limit_is_served() {
    let store = blockstore();
    for size in [CAR_MAX_BYTES - 1, CAR_MAX_BYTES] {
        let cid = put(&store, &vec![0; size]).await;
        for recursive in [false, true] {
            let outcome = if recursive {
                collect_dag_blocks_from_roots(&store, &[cid]).await
            } else {
                collect_exact_blocks(&store, &[cid]).await
            }
            .unwrap();
            assert_eq!(outcome.blocks.len(), 1);
            assert_eq!(outcome.blocks[0].1.len(), size);
            assert!(!outcome.truncated());
            assert!(outcome.oversized_blocks.is_empty());
        }
    }
}

#[tokio::test]
async fn missing_blocks_count_toward_work_limit_but_duplicates_do_not() {
    let store = blockstore();
    let last = put(&store, b"last").await;
    let mut roots = Vec::new();
    for i in 0..CAR_MAX_BLOCKS {
        let cid = Cid::new_v1(0x71, Code::Sha2_256.digest(&i.to_le_bytes()));
        roots.extend([cid, cid]);
    }
    roots.push(last);

    for recursive in [false, true] {
        let outcome = if recursive {
            collect_dag_blocks_from_roots(&store, &roots).await
        } else {
            collect_exact_blocks(&store, &roots).await
        }
        .unwrap();
        assert!(outcome.blocks.is_empty());
        assert_eq!(outcome.blockstore_misses, CAR_MAX_BLOCKS);
        assert_eq!(outcome.blockstore_hits, 0);
        assert!(outcome.truncated_by_blocks);
        assert!(!outcome.truncated_by_bytes);
    }

    roots.drain(..2);
    let outcome = collect_exact_blocks(&store, &roots).await.unwrap();
    assert_eq!(outcome.blocks[0].0, last);
    assert_eq!(outcome.blockstore_misses, CAR_MAX_BLOCKS - 1);
    assert!(!outcome.truncated());
}

#[tokio::test]
async fn recursive_collection_keeps_sibling_and_its_descendants() {
    let store = blockstore();
    let oversized = put(&store, &vec![0; CAR_MAX_BYTES + 1]).await;
    let leaf = put(&store, b"leaf").await;
    let child = linked_block(vec![DAGLink::new("leaf", leaf)]);
    let child = put(&store, &child).await;
    let root = linked_block(vec![
        DAGLink::new("oversized", oversized),
        DAGLink::new("child", child),
    ]);
    let root = put(&store, &root).await;

    let outcome = collect_dag_blocks_from_roots(&store, &[root])
        .await
        .unwrap();
    let cids: Vec<_> = outcome.blocks.iter().map(|(cid, _)| *cid).collect();
    assert_eq!(cids, vec![root, child, leaf]);
    assert!(outcome.truncated_by_bytes);
}

fn linked_block(links: Vec<DAGLink>) -> Vec<u8> {
    Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "value".to_owned(),
            priority: 1,
            schema_version_id: "version".to_owned(),
            data: Vec::new(),
        }),
        Vec::new(),
        links,
    )
    .to_dag_cbor()
    .unwrap()
}
