use std::collections::HashSet;

use acp::DocumentACP;
use cid::Cid;
use storage::corekv::Reader;

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

pub(crate) async fn load_push_dag_blocks<R: Reader + ?Sized>(
    reader: &R,
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
            match reader.get(&linked_cid.to_bytes()).await {
                Ok(Some(linked_data)) => stack.push((linked_cid, linked_data, false)),
                Ok(None) => {
                    tracing::warn!(
                        root_cid = %root_cid,
                        missing_cid = %linked_cid,
                        "Replay DAG block missing from local blockstore"
                    );
                }
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

fn extract_block_links(block_data: &[u8]) -> Vec<Cid> {
    defra_core::Block::from_dag_cbor(block_data)
        .ok()
        .and_then(|block| defra_core::collect_block_links(&block).ok())
        .unwrap_or_default()
}
