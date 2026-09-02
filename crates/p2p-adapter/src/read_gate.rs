//! DB-backed serve-gate adapters shared by libp2p Bitswap and CAR.

use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use defra_core::{is_lens_block, Block as DefraBlock, Signature};
use p2p::bitswap::{BlockAcpMeta, BlockClass, BlockClassifier, BlockReadGate};
use storage::corekv::Store;

pub struct DbBlockClassifier<S: Store + 'static> {
    db: Arc<db::DB<S>>,
}

impl<S: Store + 'static> DbBlockClassifier<S> {
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        Self { db }
    }

    pub fn new_arc(db: Arc<db::DB<S>>) -> Arc<dyn BlockClassifier> {
        Arc::new(Self::new(db))
    }

    async fn doc_ids_for_block(&self, cid: &Cid, block: &DefraBlock) -> Option<Vec<String>> {
        let txn = self.db.new_txn(true).await.ok()?;
        let systemstore = match txn.systemstore() {
            Ok(systemstore) => systemstore,
            Err(_) => {
                let _ = txn.discard();
                return None;
            }
        };
        let doc_ids = db::docid::map::resolve_block_doc_ids(&systemstore, cid, block)
            .await
            .ok()
            .flatten();
        let _ = txn.discard();
        doc_ids
    }
}

#[async_trait]
impl<S: Store + 'static> BlockClassifier for DbBlockClassifier<S> {
    async fn classify(&self, cid: &Cid, data: &[u8]) -> BlockClass {
        match defra_core::block::generate_cid_from_bytes(data) {
            Ok(actual) if &actual == cid => {}
            _ => return BlockClass::Deny,
        }

        if Signature::from_dag_cbor(data).is_ok() {
            return BlockClass::Allow;
        }

        match DefraBlock::from_dag_cbor(data) {
            Ok(block) => {
                if block.delta.is_definition() {
                    return BlockClass::Allow;
                }

                let Some(schema_version_id) = block.delta.schema_version_id() else {
                    return BlockClass::Deny;
                };
                let collection = match self
                    .db
                    .get_collection_by_version_id_full(schema_version_id)
                    .await
                {
                    Ok(Some(collection)) => collection,
                    Ok(None) | Err(_) => return BlockClass::Deny,
                };
                let collection = collection.schema();
                let policy = collection
                    .policy
                    .as_ref()
                    .map(|p| (p.id.clone(), p.resource_name.clone()));
                let Some(doc_ids) = self.doc_ids_for_block(cid, &block).await else {
                    return BlockClass::Deny;
                };

                BlockClass::Data(BlockAcpMeta {
                    collection_id: collection.collection_id.clone(),
                    is_branchable: collection.is_branchable,
                    policy,
                    doc_ids,
                })
            }
            Err(_) if is_lens_block(data) => BlockClass::Allow,
            Err(_) => BlockClass::Deny,
        }
    }
}

pub struct DbBlockReadGate {
    acp: Arc<dyn acp::DocumentACP>,
}

impl DbBlockReadGate {
    pub fn new(acp: Arc<dyn acp::DocumentACP>) -> Self {
        Self { acp }
    }

    pub fn new_arc(acp: Arc<dyn acp::DocumentACP>) -> Arc<dyn BlockReadGate> {
        Arc::new(Self::new(acp))
    }
}

#[async_trait]
impl BlockReadGate for DbBlockReadGate {
    async fn may_read(&self, identity: &acp::Identity, meta: &BlockAcpMeta) -> bool {
        let Some((policy_id, resource_name)) = meta.policy.as_ref() else {
            return true;
        };

        let checker = acp::read_access::DirectChecker {
            acp: self.acp.as_ref(),
            identity,
        };

        if meta.doc_ids.is_empty() {
            return acp::read_access::check_doc_read_access(
                &checker,
                policy_id,
                resource_name,
                &meta.collection_id,
                meta.is_branchable,
                "",
            )
            .await
            .unwrap_or(false);
        }

        for doc_id in &meta.doc_ids {
            if acp::read_access::check_doc_read_access(
                &checker,
                policy_id,
                resource_name,
                &meta.collection_id,
                meta.is_branchable,
                doc_id,
            )
            .await
            .unwrap_or(false)
            {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use acp::{DocumentACP, Identity, LocalDocumentACP, MemoryAcpStore};
    use p2p::bitswap::{BlockAcpMeta, BlockClass, BlockClassifier, BlockReadGate};
    use schema::{CollectionVersion, FieldDescription, FieldKind, PolicyDescription};
    use storage::RegolithStore;

    use super::{DbBlockClassifier, DbBlockReadGate};

    fn test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "User",
            "version-1",
            "collection-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        )
        .with_policy(PolicyDescription::new("policy1", "users"))
        .as_branchable()
    }

    fn data_block(_doc_id: &str) -> (cid::Cid, Vec<u8>) {
        let block = defra_core::Block::new(
            defra_core::CrdtDelta::Lww(defra_core::LwwDeltaPayload {
                field_name: "name".to_string(),
                priority: 1,
                schema_version_id: "version-1".to_string(),
                data: b"Alice".to_vec(),
            }),
            vec![],
            vec![],
        );
        let bytes = block.to_dag_cbor().unwrap();
        let cid = defra_core::block::generate_cid_from_bytes(&bytes).unwrap();
        (cid, bytes)
    }

    #[tokio::test]
    async fn classifier_uses_serving_cid_owner_metadata() {
        let db = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
        db.create_collection(test_collection()).await.unwrap();
        let (cid, bytes) = data_block("doc-from-delta");

        let txn = db.new_txn(false).await.unwrap();
        {
            let systemstore = txn.systemstore().unwrap();
            db::docid::map::set_block_doc_id_mapping(
                &systemstore,
                &cid.to_string(),
                "doc-from-index",
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();

        let classifier = DbBlockClassifier::new(db);
        let class = classifier.classify(&cid, &bytes).await;

        match class {
            BlockClass::Data(meta) => {
                assert_eq!(meta.collection_id, "collection-1");
                assert!(meta.is_branchable);
                assert_eq!(
                    meta.policy,
                    Some(("policy1".to_string(), "users".to_string()))
                );
                assert_eq!(meta.doc_ids, vec!["doc-from-index"]);
            }
            other => panic!("expected data block, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn classifier_denies_field_block_without_owner_metadata() {
        let db = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
        db.create_collection(test_collection()).await.unwrap();
        let (cid, bytes) = data_block("doc-from-delta");

        let classifier = DbBlockClassifier::new(db);

        assert_eq!(classifier.classify(&cid, &bytes).await, BlockClass::Deny);
    }

    #[tokio::test]
    async fn node_identity_without_a_grant_cannot_read_a_protected_block() {
        let owner =
            identity::Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap();
        let node =
            identity::Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap();
        let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();
        let gate = DbBlockReadGate::new(acp);
        let meta = BlockAcpMeta {
            collection_id: "collection1".to_string(),
            is_branchable: false,
            policy: Some(("policy1".to_string(), "users".to_string())),
            doc_ids: vec!["doc1".to_string()],
        };

        assert!(
            !gate.may_read(&Identity::Authenticated(node), &meta).await,
            "the process owner must satisfy document ACP when serving blocks"
        );
    }
}
