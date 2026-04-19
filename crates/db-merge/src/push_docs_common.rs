use std::collections::HashSet;
use std::str::FromStr;

use acp::DocumentACP;
use cid::Cid;
use storage::corekv::{IterOptions, Reader};
use storage::keys::headstore::{HeadstoreDocKey, HeadstorePriorityKey};

use db::Collection;

/// Maximum concurrent per-document push tasks during initial replay.
///
/// Lower than the coordinator's live push limit (32) because initial replay
/// is background work that shouldn't starve real-time sync traffic.
pub(crate) const MAX_CONCURRENT_REPLAY_TASKS: usize = 8;

pub(crate) async fn resolve_push_creator(
    document_acp: Option<&dyn DocumentACP>,
    collection: &Collection,
    doc_id: &str,
    fallback_creator: &str,
) -> String {
    let Some(policy) = &collection.schema().policy else {
        return fallback_creator.to_string();
    };

    let mut resource_names = vec![policy.resource_name.clone()];
    for candidate in [
        collection.name().to_string(),
        collection.name().to_lowercase(),
        format!("{}s", collection.name().to_lowercase()),
    ] {
        if !resource_names.iter().any(|existing| existing == &candidate) {
            resource_names.push(candidate);
        }
    }

    if let Some(acp) = document_acp {
        for resource_name in &resource_names {
            match acp.get_doc_owner(&policy.id, resource_name, doc_id).await {
                Ok(Some(owner)) => {
                    if resource_name != &policy.resource_name {
                        tracing::info!(
                            collection = %collection.name(),
                            collection_id = %collection.collection_id(),
                            resource_name = %resource_name,
                            doc_id = %doc_id,
                            "Resolved ACP owner for replicator push using fallback resource name"
                        );
                    }
                    return owner.to_string();
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        collection = %collection.name(),
                        collection_id = %collection.collection_id(),
                        resource_name = %resource_name,
                        doc_id = %doc_id,
                        error = %error,
                        "Failed to resolve ACP owner for replicator push"
                    );
                }
            }
        }
    }

    tracing::warn!(
        collection = %collection.name(),
        collection_id = %collection.collection_id(),
        resource_names = ?resource_names,
        doc_id = %doc_id,
        fallback_creator = %fallback_creator,
        "Falling back to peer identity for replicator replay creator"
    );
    fallback_creator.to_string()
}

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

pub(crate) async fn load_latest_composite_head_cids<R: Reader + ?Sized, B: Reader + ?Sized>(
    head_reader: &R,
    block_reader: &B,
    doc_id: &str,
) -> Vec<Cid> {
    let mut cids = Vec::new();
    let mut max_priority: Option<u64> = None;

    if let Ok(mut iter) = head_reader
        .iterator(IterOptions::new().with_prefix(HeadstorePriorityKey::document_prefix(doc_id)))
        .await
    {
        let cid_offset = HeadstorePriorityKey::cid_offset(doc_id);
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
                Some(current) if priority == current => cids.push(cid),
                _ => {
                    max_priority = Some(priority);
                    cids.clear();
                    cids.push(cid);
                }
            }
        }
        let _ = iter.close().await;
    }

    if !cids.is_empty() {
        return cids;
    }

    if let Ok(mut iter) = head_reader
        .iterator(IterOptions::new().with_prefix(HeadstoreDocKey::field_prefix(doc_id, "C")))
        .await
    {
        while let Ok(Some(pair)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&pair.key);
            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() < 5 {
                continue;
            }
            let Ok(cid) = Cid::from_str(parts[4]) else {
                continue;
            };
            cids.push(cid);
        }
        let _ = iter.close().await;
    }

    cids
}

fn extract_block_links(block_data: &[u8]) -> Vec<Cid> {
    defra_core::Block::from_dag_cbor(block_data)
        .ok()
        .and_then(|block| defra_core::collect_block_links(&block).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::DB;
    use defra_core::{block::generate_cid_from_bytes, Block, CompositeDeltaPayload, CrdtDelta};
    use std::sync::Arc;
    use storage::backends::MemoryStore;
    use storage::corekv::Key;
    use storage::keys::headstore::{HeadstoreDocKey, HeadstorePriorityKey};

    #[tokio::test]
    async fn latest_composite_head_selection_prefers_highest_priority_index() {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store).unwrap());
        let doc_id = "bae-test-latest-head";
        let first = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: doc_id.as_bytes().to_vec(),
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

        let second = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: doc_id.as_bytes().to_vec(),
                schema_version_id: "schema-v1".to_string(),
                priority: 2,
                status: 1,
            }),
            vec![],
            vec![],
            None,
            None,
        );
        let second_bytes = second.to_dag_cbor().unwrap();
        let second_cid = generate_cid_from_bytes(&second_bytes).unwrap();

        let txn = db.new_txn(false).await.unwrap();
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
            .set(&HeadstoreDocKey::new(doc_id, "C", first_cid).bytes(), &[])
            .await
            .unwrap();
        txn.headstore()
            .unwrap()
            .set(&HeadstoreDocKey::new(doc_id, "C", second_cid).bytes(), &[])
            .await
            .unwrap();
        txn.headstore()
            .unwrap()
            .set(&HeadstorePriorityKey::new(doc_id, 1, first_cid).bytes(), &[])
            .await
            .unwrap();
        txn.headstore()
            .unwrap()
            .set(&HeadstorePriorityKey::new(doc_id, 2, second_cid).bytes(), &[])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = db.new_txn(true).await.unwrap();
        let heads = load_latest_composite_head_cids(
            &txn.headstore().unwrap(),
            &txn.blockstore().unwrap(),
            doc_id,
        )
        .await;

        assert_eq!(heads, vec![second_cid]);
        assert_ne!(heads, vec![first_cid]);
    }
}
