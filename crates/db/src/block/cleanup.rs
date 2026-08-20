use std::collections::{HashMap, HashSet};

use crate::{Error, Result};
use cid::Cid;
use datastore::NamespaceView;
use defra_core::Block;

async fn remove_owner(systemstore: &NamespaceView, cid: &Cid, doc_id: &str) -> Result<bool> {
    crate::docid::map::delete_block_doc_id_mapping(systemstore, &cid.to_string(), doc_id).await?;
    Ok(
        crate::docid::map::get_doc_ids_for_block(systemstore, &cid.to_string())
            .await?
            .is_empty(),
    )
}

async fn load_block(blockstore: &NamespaceView, cid: &Cid) -> Result<Option<Block>> {
    let Some(bytes) = blockstore
        .get(&cid.to_bytes())
        .await
        .map_err(Error::Storage)?
    else {
        return Ok(None);
    };
    Ok(Block::from_dag_cbor(&bytes).ok())
}

async fn remove_leaf_owner(
    blockstore: &NamespaceView,
    systemstore: &NamespaceView,
    cid: &Cid,
    doc_id: &str,
    can_delete: bool,
) -> Result<()> {
    let block = load_block(blockstore, cid).await?;
    let unowned = remove_owner(systemstore, cid, doc_id).await?;
    if let Some(block) = block {
        if let Some(encryption_cid) = block.encryption {
            let encryption_unowned = remove_owner(systemstore, &encryption_cid, doc_id).await?;
            if can_delete && unowned && encryption_unowned {
                blockstore
                    .delete(&encryption_cid.to_bytes())
                    .await
                    .map_err(Error::Storage)?;
            }
        }
        if can_delete && unowned {
            if let Some(signature_cid) = block.signature {
                blockstore
                    .delete(&signature_cid.to_bytes())
                    .await
                    .map_err(Error::Storage)?;
            }
        }
    }
    if can_delete && unowned {
        blockstore
            .delete(&cid.to_bytes())
            .await
            .map_err(Error::Storage)?;
    }
    Ok(())
}

async fn remove_encryption_owner(
    blockstore: &NamespaceView,
    systemstore: &NamespaceView,
    encryption_cid: &Cid,
    doc_id: &str,
    can_delete: bool,
) -> Result<()> {
    let unowned = remove_owner(systemstore, encryption_cid, doc_id).await?;
    if can_delete && unowned {
        blockstore
            .delete(&encryption_cid.to_bytes())
            .await
            .map_err(Error::Storage)?;
    }
    Ok(())
}

/// Remove one document's ownership of a commit and its direct field blocks.
/// History parents are deliberately retained; the caller chooses which
/// priorities to prune.
pub(crate) async fn delete_owned_commit(
    blockstore: &NamespaceView,
    systemstore: &NamespaceView,
    cid: &Cid,
    doc_id: &str,
) -> Result<()> {
    let block = load_block(blockstore, cid).await?;
    let unowned = remove_owner(systemstore, cid, doc_id).await?;

    if let Some(block) = block {
        if let Some(links) = block.links {
            for link in links {
                remove_leaf_owner(blockstore, systemstore, &link.link, doc_id, unowned).await?;
            }
        }
        if let Some(encryption_cid) = block.encryption {
            remove_encryption_owner(blockstore, systemstore, &encryption_cid, doc_id, unowned)
                .await?;
        }
        if unowned {
            if let Some(signature_cid) = block.signature {
                blockstore
                    .delete(&signature_cid.to_bytes())
                    .await
                    .map_err(Error::Storage)?;
            }
        }
    }
    if unowned {
        blockstore
            .delete(&cid.to_bytes())
            .await
            .map_err(Error::Storage)?;
    }
    Ok(())
}

/// Remove one document's ownership from every block reachable from `roots`.
/// Shared blocks and everything they still reference remain intact.
pub(crate) async fn delete_owned_dag(
    blockstore: &NamespaceView,
    systemstore: &NamespaceView,
    roots: &[Cid],
    doc_id: &str,
) -> Result<()> {
    let mut stack = roots.to_vec();
    let mut edges: HashMap<Cid, Vec<Cid>> = HashMap::new();
    let mut signatures: HashMap<Cid, Cid> = HashMap::new();
    let mut visited = HashSet::new();

    while let Some(cid) = stack.pop() {
        if !visited.insert(cid) {
            continue;
        }
        let Some(block) = load_block(blockstore, &cid).await? else {
            continue;
        };
        let mut children = block.all_links();
        if let Some(encryption_cid) = block.encryption {
            children.push(encryption_cid);
        }
        if let Some(signature_cid) = block.signature {
            signatures.insert(cid, signature_cid);
        }
        stack.extend(children.iter().copied());
        edges.insert(cid, children);
    }

    let mut retained = HashSet::new();
    for cid in &visited {
        if !remove_owner(systemstore, cid, doc_id).await? {
            retained.insert(*cid);
        }
    }

    stack.extend(retained.iter().copied());
    while let Some(cid) = stack.pop() {
        if let Some(children) = edges.get(&cid) {
            for child in children {
                if retained.insert(*child) {
                    stack.push(*child);
                }
            }
        }
    }

    for cid in visited.difference(&retained) {
        if let Some(signature_cid) = signatures.get(cid) {
            blockstore
                .delete(&signature_cid.to_bytes())
                .await
                .map_err(Error::Storage)?;
        }
        blockstore
            .delete(&cid.to_bytes())
            .await
            .map_err(Error::Storage)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use datastore::{NamespaceView, SharedTxn};
    use defra_core::{CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};
    use storage::backends::MemoryStore;
    use storage::corekv::Store;
    use storage::namespace::Namespace;

    use super::*;

    async fn stores() -> (NamespaceView, NamespaceView) {
        let store = MemoryStore::new();
        let txn = SharedTxn::new(store.new_txn(false).await.unwrap());
        (
            NamespaceView::new(txn.clone(), Namespace::Blockstore),
            NamespaceView::new(txn, Namespace::Systemstore),
        )
    }

    #[tokio::test]
    async fn deleting_one_owner_keeps_a_shared_block() {
        let (blockstore, systemstore) = stores().await;
        let block = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: "name".to_string(),
                schema_version_id: "v1".to_string(),
                priority: 1,
                data: vec![1],
            }),
            vec![],
            vec![],
        );
        let cid = block.generate_cid().unwrap();
        blockstore
            .set(&cid.to_bytes(), &block.to_dag_cbor().unwrap())
            .await
            .unwrap();
        for owner in ["doc-a", "doc-b"] {
            crate::docid::map::set_block_doc_id_mapping(&systemstore, &cid.to_string(), owner)
                .await
                .unwrap();
        }

        delete_owned_commit(&blockstore, &systemstore, &cid, "doc-a")
            .await
            .unwrap();

        assert!(blockstore.get(&cid.to_bytes()).await.unwrap().is_some());
        assert_eq!(
            crate::docid::map::get_doc_ids_for_block(&systemstore, &cid.to_string())
                .await
                .unwrap(),
            vec!["doc-b".to_string()]
        );
    }

    #[tokio::test]
    async fn deleting_a_dag_keeps_the_shared_subtree() {
        let (blockstore, systemstore) = stores().await;
        let field = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: "name".to_string(),
                schema_version_id: "v1".to_string(),
                priority: 1,
                data: vec![1],
            }),
            vec![],
            vec![],
        );
        let field_cid = field.generate_cid().unwrap();
        let root = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "v1".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![DAGLink::new("name", field_cid)],
        );
        let root_cid = root.generate_cid().unwrap();
        for (cid, block) in [(field_cid, field), (root_cid, root)] {
            blockstore
                .set(&cid.to_bytes(), &block.to_dag_cbor().unwrap())
                .await
                .unwrap();
        }
        crate::docid::map::set_block_doc_id_mapping(&systemstore, &root_cid.to_string(), "doc-a")
            .await
            .unwrap();
        for owner in ["doc-a", "doc-b"] {
            crate::docid::map::set_block_doc_id_mapping(
                &systemstore,
                &field_cid.to_string(),
                owner,
            )
            .await
            .unwrap();
        }

        delete_owned_dag(&blockstore, &systemstore, &[root_cid], "doc-a")
            .await
            .unwrap();

        assert!(blockstore
            .get(&root_cid.to_bytes())
            .await
            .unwrap()
            .is_none());
        assert!(blockstore
            .get(&field_cid.to_bytes())
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn deleting_one_commit_owner_clears_child_ownership_but_keeps_shared_bytes() {
        let (blockstore, systemstore) = stores().await;
        let field = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: "name".to_string(),
                schema_version_id: "v1".to_string(),
                priority: 1,
                data: vec![1],
            }),
            vec![],
            vec![],
        );
        let field_cid = field.generate_cid().unwrap();
        let root = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "v1".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![DAGLink::new("name", field_cid)],
        );
        let root_cid = root.generate_cid().unwrap();
        for (cid, block) in [(field_cid, field), (root_cid, root)] {
            blockstore
                .set(&cid.to_bytes(), &block.to_dag_cbor().unwrap())
                .await
                .unwrap();
            for owner in ["doc-a", "doc-b"] {
                crate::docid::map::set_block_doc_id_mapping(&systemstore, &cid.to_string(), owner)
                    .await
                    .unwrap();
            }
        }

        delete_owned_commit(&blockstore, &systemstore, &root_cid, "doc-a")
            .await
            .unwrap();

        for cid in [root_cid, field_cid] {
            assert!(blockstore.get(&cid.to_bytes()).await.unwrap().is_some());
            assert_eq!(
                crate::docid::map::get_doc_ids_for_block(&systemstore, &cid.to_string())
                    .await
                    .unwrap(),
                vec!["doc-b".to_string()]
            );
        }
    }
}
