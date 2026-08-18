use std::collections::HashSet;
use std::str::FromStr;

use cid::Cid;
use storage::corekv::{IterOptions, Reader};
use storage::keys::headstore::{HeadstoreDocKey, HeadstorePriorityKey};

pub(crate) async fn load_push_dag_blocks<R: Reader + ?Sized, E: Reader + ?Sized>(
    block_reader: &R,
    enc_reader: &E,
    root_cid: Cid,
    root_data: Vec<u8>,
) -> Vec<(Cid, Vec<u8>)> {
    let mut ordered = Vec::new();
    let mut visited = HashSet::new();
    let mut stack = vec![(root_cid, root_data, false)];

    while let Some((cid, data, expanded)) = stack.pop() {
        if expanded {
            ordered.push((cid, data));
            continue;
        }

        if !visited.insert(cid) {
            continue;
        }

        let linked_cids = extract_block_links(&data);
        stack.push((cid, data, true));

        for linked_cid in linked_cids.into_iter().rev() {
            match block_reader.get(&linked_cid.to_bytes()).await {
                Ok(Some(linked_data)) => stack.push((linked_cid, linked_data, false)),
                Ok(None) => match enc_reader.get(&linked_cid.to_bytes()).await {
                    Ok(Some(linked_data)) => stack.push((linked_cid, linked_data, false)),
                    Ok(None) => {
                        tracing::warn!(
                            root_cid = %root_cid,
                            missing_cid = %linked_cid,
                            "Replay DAG block missing from local blockstore and encstore"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            root_cid = %root_cid,
                            missing_cid = %linked_cid,
                            error = %error,
                            "Failed to load replay DAG block from local encstore"
                        );
                    }
                },
                Err(error) => {
                    tracing::warn!(
                        root_cid = %root_cid,
                        missing_cid = %linked_cid,
                        error = %error,
                        "Failed to load replay DAG block from local blockstore"
                    );
                }
            }
        }
    }

    ordered
}

pub(crate) async fn load_replay_blocks<R: Reader + ?Sized, E: Reader + ?Sized>(
    block_reader: &R,
    enc_reader: &E,
    heads: Vec<(Cid, Vec<u8>)>,
    include_dependencies: bool,
) -> Vec<(Cid, Vec<u8>)> {
    if !include_dependencies {
        return heads;
    }

    let mut blocks = Vec::new();
    for (root_cid, root_data) in heads {
        blocks.extend(load_push_dag_blocks(block_reader, enc_reader, root_cid, root_data).await);
    }
    blocks
}

pub(crate) async fn load_latest_composite_heads<R: Reader + ?Sized, B: Reader + ?Sized>(
    head_reader: &R,
    block_reader: &B,
    doc_short_id: u64,
) -> Vec<(Cid, Vec<u8>)> {
    let mut heads = Vec::new();
    let mut max_priority: Option<u64> = None;

    if let Ok(mut iter) = head_reader
        .iterator(
            IterOptions::new().with_prefix(HeadstorePriorityKey::document_prefix(doc_short_id)),
        )
        .await
    {
        let cid_offset = HeadstorePriorityKey::cid_offset(doc_short_id);
        while let Ok(Some(pair)) = iter.next().await {
            let cid_bytes = match pair.key.get(cid_offset..) {
                Some(bytes) => bytes,
                None => continue,
            };
            let Ok(cid) = Cid::try_from(cid_bytes) else {
                continue;
            };
            let Ok(Some(block_bytes)) = block_reader.get(&cid.to_bytes()).await else {
                continue;
            };
            let Ok(block) = defra_core::Block::from_dag_cbor(&block_bytes) else {
                continue;
            };
            if !matches!(block.delta, defra_core::CrdtDelta::Composite(_)) {
                continue;
            }

            let priority = block.delta.priority();
            match max_priority {
                Some(current) if priority < current => {}
                Some(current) if priority == current => heads.push((cid, block_bytes)),
                _ => {
                    max_priority = Some(priority);
                    heads.clear();
                    heads.push((cid, block_bytes));
                }
            }
        }
        let _ = iter.close().await;
    }

    if !heads.is_empty() {
        return heads;
    }

    let field_prefix = HeadstoreDocKey::field_prefix(doc_short_id, "C");
    let field_prefix_len = field_prefix.len();
    if let Ok(mut iter) = head_reader
        .iterator(IterOptions::new().with_prefix(field_prefix))
        .await
    {
        while let Ok(Some(pair)) = iter.next().await {
            let cid_str = String::from_utf8_lossy(&pair.key[field_prefix_len..]);
            let Ok(cid) = Cid::from_str(&cid_str) else {
                continue;
            };
            let Ok(Some(block_bytes)) = block_reader.get(&cid.to_bytes()).await else {
                continue;
            };
            let Ok(block) = defra_core::Block::from_dag_cbor(&block_bytes) else {
                continue;
            };
            if matches!(block.delta, defra_core::CrdtDelta::Composite(_)) {
                heads.push((cid, block_bytes));
            }
        }
        let _ = iter.close().await;
    }

    heads
}

fn extract_block_links(block_data: &[u8]) -> Vec<Cid> {
    let Some(block) = defra_core::Block::from_dag_cbor(block_data).ok() else {
        return Vec::new();
    };
    let mut links = defra_core::collect_block_links(&block).unwrap_or_default();
    // Drop the encryption-metadata link. The encryption block holds the
    // plaintext DEK and is gated by the KMS access policy (NacDacPolicy):
    // it must travel ONLY over the KMS `encryption` topic (ECIES-wrapped,
    // permission-checked), never bundled into a replication pushlog. Walking
    // it here would copy the DEK to a peer that has no DAC permission,
    // bypassing the dual-gate (issue #976). Mirrors the Bitswap link walker
    // in crates/p2p/src/sync/manager/links.rs.
    if let Some(enc_cid) = block.encryption {
        links.retain(|cid| *cid != enc_cid);
    }
    links
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::DB;
    use defra_core::{
        block::generate_cid_from_bytes, Block, CompositeDeltaPayload, CrdtDelta, DAGLink,
        LwwDeltaPayload,
    };
    use std::sync::Arc;
    use storage::backends::MemoryStore;
    use storage::corekv::Key;
    use storage::keys::headstore::{HeadstoreDocKey, HeadstorePriorityKey};

    #[tokio::test]
    async fn latest_composite_heads_return_only_highest_priority_roots() {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store).unwrap());
        let doc_short_id = 7_u64;
        let first = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "schema-v1".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![],
            None,
            None,
        );
        let first_bytes = first.to_dag_cbor().unwrap();
        let first_cid = generate_cid_from_bytes(&first_bytes).unwrap();

        let field = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: "name".to_string(),
                priority: 2,
                schema_version_id: "schema-v1".to_string(),
                data: vec![1],
            }),
            vec![],
            vec![],
        );
        let field_bytes = field.to_dag_cbor().unwrap();
        let field_cid = generate_cid_from_bytes(&field_bytes).unwrap();
        let second = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "schema-v1".to_string(),
                priority: 2,
                status: 1,
            }),
            vec![],
            vec![DAGLink::new("name", field_cid)],
            None,
            None,
        );
        let second_bytes = second.to_dag_cbor().unwrap();
        let second_cid = generate_cid_from_bytes(&second_bytes).unwrap();

        let txn = db.new_txn(false).await.unwrap();
        txn.blockstore()
            .unwrap()
            .set(&field_cid.to_bytes(), &field_bytes)
            .await
            .unwrap();
        txn.blockstore()
            .unwrap()
            .set(&first_cid.to_bytes(), &first_bytes)
            .await
            .unwrap();
        txn.blockstore()
            .unwrap()
            .set(&second_cid.to_bytes(), &second_bytes)
            .await
            .unwrap();
        txn.headstore()
            .unwrap()
            .set(
                &HeadstoreDocKey::new(doc_short_id, "C", first_cid).bytes(),
                &[],
            )
            .await
            .unwrap();
        txn.headstore()
            .unwrap()
            .set(
                &HeadstoreDocKey::new(doc_short_id, "C", second_cid).bytes(),
                &[],
            )
            .await
            .unwrap();
        txn.headstore()
            .unwrap()
            .set(
                &HeadstorePriorityKey::new(doc_short_id, 1, first_cid).bytes(),
                &[],
            )
            .await
            .unwrap();
        txn.headstore()
            .unwrap()
            .set(
                &HeadstorePriorityKey::new(doc_short_id, 2, second_cid).bytes(),
                &[],
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = db.new_txn(true).await.unwrap();
        let heads = load_latest_composite_heads(
            &txn.headstore().unwrap(),
            &txn.blockstore().unwrap(),
            doc_short_id,
        )
        .await;

        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0].0, second_cid);
        assert_eq!(heads[0].1, second_bytes);
    }
}
