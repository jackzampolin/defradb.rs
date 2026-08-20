use std::collections::HashSet;
use std::str::FromStr;

use cid::Cid;
use storage::corekv::{IterOptions, Reader};
#[cfg(not(target_arch = "wasm32"))]
use storage::keys::headstore::HeadstoreColKey;
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

pub(crate) async fn load_latest_composite_head_cids<R: Reader + ?Sized, B: Reader + ?Sized>(
    head_reader: &R,
    block_reader: &B,
    doc_short_id: u64,
) -> Vec<Cid> {
    let mut cids = Vec::new();

    // The composite head keyspace is the authoritative current frontier. A
    // document may have concurrent sibling heads with different priorities;
    // collapsing this set to the maximum priority loses an obligation when a
    // marker retry is the sender's only durable source of truth. This matches
    // Go's getHeadsForDocShortID path.
    let head_prefix = HeadstoreDocKey::field_prefix(doc_short_id, "C");
    let head_prefix_len = head_prefix.len();
    if let Ok(mut iter) = head_reader
        .iterator(IterOptions::new().with_prefix(head_prefix))
        .await
    {
        while let Ok(Some(pair)) = iter.next().await {
            let cid_str = String::from_utf8_lossy(&pair.key[head_prefix_len..]);
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
                cids.push(cid);
            }
        }
        let _ = iter.close().await;
    }

    if !cids.is_empty() {
        return cids;
    }

    // Recovery fallback for stores whose authoritative head entries are
    // absent but whose commit-priority index is intact. The index includes
    // history, so only its highest composite priority is current in this mode.
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

    cids
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn load_collection_head_cids<R: Reader + ?Sized>(
    head_reader: &R,
    collection_short_id: u32,
) -> Result<Vec<Cid>, String> {
    let prefix = HeadstoreColKey::collection_prefix(collection_short_id);
    let prefix_len = prefix.len();
    let mut iter = head_reader
        .iterator(IterOptions::new().with_prefix(prefix))
        .await
        .map_err(|error| format!("failed to iterate collection heads: {error}"))?;
    let mut heads = Vec::new();
    while let Some(pair) = iter
        .next()
        .await
        .map_err(|error| format!("failed to read collection head: {error}"))?
    {
        let cid_text = String::from_utf8_lossy(&pair.key[prefix_len..]);
        if let Ok(cid) = Cid::from_str(&cid_text) {
            heads.push(cid);
        }
    }
    Ok(heads)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn resolve_collection_id_for_doc<S: storage::corekv::Store>(
    db: &db::DB<S>,
    doc_id: &str,
) -> Result<Option<String>, String> {
    let txn = db
        .new_txn(true)
        .await
        .map_err(|error| format!("document scope lookup txn: {error}"))?;
    let systemstore = txn.systemstore().map_err(|error| error.to_string())?;
    let Some(doc_ref) = db::doc_id_map::get_doc_ref(&systemstore, doc_id)
        .await
        .map_err(|error| format!("document scope lookup: {error}"))?
    else {
        return Ok(None);
    };
    for name in db.list_collections().map_err(|error| error.to_string())? {
        let Some(collection) = db
            .get_collection(&name)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        // Collections are loaded with their persisted root IDs. Avoid one
        // systemstore round-trip per collection for every dirty document.
        if collection.resolved_root_id() == doc_ref.collection_short_id {
            return Ok(Some(collection.collection_id().to_string()));
        }
    }
    Ok(None)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn complete_document_retry_if_current<S: storage::corekv::Store>(
    db: &db::DB<S>,
    peer_id: &str,
    doc_id: &str,
    collection_id: &str,
    doc_short_id: u64,
    attempted_heads: &[Cid],
) -> Result<(), String> {
    let peerstore = storage::stores::Peerstore::new(db.store().clone());
    let Some(_guard) = peerstore
        .acquire_replicator_retry_guard(peer_id)
        .await
        .map_err(|error| format!("retry completion guard: {error}"))?
    else {
        return Ok(());
    };
    let txn = db
        .new_txn(true)
        .await
        .map_err(|error| format!("head verification transaction: {error}"))?;
    let heads = txn.headstore().map_err(|error| error.to_string())?;
    let blocks = txn.blockstore().map_err(|error| error.to_string())?;
    let mut current_heads = load_latest_composite_head_cids(&heads, &blocks, doc_short_id).await;
    current_heads.sort_unstable();
    if current_heads != attempted_heads {
        return Err("document heads changed during retry; retaining dirty marker".to_string());
    }
    peerstore
        .complete_retry_scope(peer_id, doc_id, collection_id, false)
        .await
        .map_err(|error| format!("failed to clear current document retry marker: {error}"))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn complete_document_retry_if_absent<S: storage::corekv::Store>(
    db: &db::DB<S>,
    peer_id: &str,
    doc_id: &str,
    collection_id: &str,
) -> Result<(), String> {
    let peerstore = storage::stores::Peerstore::new(db.store().clone());
    let Some(_guard) = peerstore
        .acquire_replicator_retry_guard(peer_id)
        .await
        .map_err(|error| format!("retry completion guard: {error}"))?
    else {
        return Ok(());
    };
    let txn = db
        .new_txn(true)
        .await
        .map_err(|error| format!("document absence verification transaction: {error}"))?;
    let systemstore = txn.systemstore().map_err(|error| error.to_string())?;
    if db::doc_id_map::get_doc_ref(&systemstore, doc_id)
        .await
        .map_err(|error| format!("document absence verification: {error}"))?
        .is_some()
    {
        return Err("document appeared during retry; retaining dirty marker".to_string());
    }
    peerstore
        .complete_retry_scope(peer_id, doc_id, collection_id, false)
        .await
        .map_err(|error| format!("failed to clear absent document retry marker: {error}"))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn complete_collection_retry_if_current<S: storage::corekv::Store>(
    db: &db::DB<S>,
    peer_id: &str,
    collection_id: &str,
    attempted_heads: &[Cid],
) -> Result<(), String> {
    let peerstore = storage::stores::Peerstore::new(db.store().clone());
    let Some(_guard) = peerstore
        .acquire_replicator_retry_guard(peer_id)
        .await
        .map_err(|error| format!("retry completion guard: {error}"))?
    else {
        return Ok(());
    };
    let txn = db
        .new_txn(true)
        .await
        .map_err(|error| format!("collection head verification transaction: {error}"))?;
    let systemstore = txn.systemstore().map_err(|error| error.to_string())?;
    let headstore = txn.headstore().map_err(|error| error.to_string())?;
    let short_id =
        db::collection::require_persisted_collection_short_id(&systemstore, collection_id)
            .await
            .map_err(|error| format!("collection retry verification short id: {error}"))?;
    let mut current_heads = load_collection_head_cids(&headstore, short_id).await?;
    current_heads.sort_unstable();
    if current_heads != attempted_heads {
        return Err("collection heads changed during retry; retaining dirty marker".to_string());
    }
    peerstore
        .complete_retry_scope(peer_id, "", collection_id, true)
        .await
        .map_err(|error| format!("failed to clear current collection retry marker: {error}"))
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
    use defra_core::{block::generate_cid_from_bytes, Block, CompositeDeltaPayload, CrdtDelta};
    use std::sync::Arc;
    use storage::backends::MemoryStore;
    use storage::corekv::Key;
    use storage::keys::headstore::{HeadstoreDocKey, HeadstorePriorityKey};

    #[tokio::test]
    async fn current_composite_frontier_retains_lower_priority_sibling() {
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

        let second = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
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
        let mut heads = load_latest_composite_head_cids(
            &txn.headstore().unwrap(),
            &txn.blockstore().unwrap(),
            doc_short_id,
        )
        .await;

        heads.sort_unstable();
        let mut expected = vec![first_cid, second_cid];
        expected.sort_unstable();
        assert_eq!(heads, expected);
    }

    #[tokio::test]
    async fn stale_retry_heads_cannot_clear_a_newer_document_marker() {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store.clone()).unwrap());
        let peerstore = storage::stores::Peerstore::new(store);
        let peer_id = "peer";
        let doc_id = "doc";
        let collection_id = "collection";
        let doc_short_id = 7_u64;
        peerstore
            .create_replicator(peer_id, b"replicator")
            .await
            .unwrap();
        peerstore
            .observe_push_head(peer_id, doc_id, collection_id)
            .await
            .unwrap();

        let old = Block::new_with_options(
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
        let old_bytes = old.to_dag_cbor().unwrap();
        let old_cid = generate_cid_from_bytes(&old_bytes).unwrap();
        let current = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "schema-v1".to_string(),
                priority: 2,
                status: 1,
            }),
            vec![],
            vec![],
            None,
            None,
        );
        let current_bytes = current.to_dag_cbor().unwrap();
        let current_cid = generate_cid_from_bytes(&current_bytes).unwrap();
        let txn = db.new_txn(false).await.unwrap();
        for (cid, bytes, priority) in [
            (old_cid, old_bytes.as_slice(), 1),
            (current_cid, current_bytes.as_slice(), 2),
        ] {
            txn.blockstore()
                .unwrap()
                .set(&cid.to_bytes(), bytes)
                .await
                .unwrap();
            txn.headstore()
                .unwrap()
                .set(
                    &HeadstorePriorityKey::new(doc_short_id, priority, cid).bytes(),
                    &[],
                )
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();

        let error = complete_document_retry_if_current(
            &db,
            peer_id,
            doc_id,
            collection_id,
            doc_short_id,
            &[old_cid],
        )
        .await
        .unwrap_err();
        assert!(error.contains("heads changed"));
        assert_eq!(
            peerstore.get_retry_documents(peer_id).await.unwrap().len(),
            1
        );

        complete_document_retry_if_current(
            &db,
            peer_id,
            doc_id,
            collection_id,
            doc_short_id,
            &[current_cid],
        )
        .await
        .unwrap();
        assert!(peerstore
            .get_retry_documents(peer_id)
            .await
            .unwrap()
            .is_empty());
    }
}
