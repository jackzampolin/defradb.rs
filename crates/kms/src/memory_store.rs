//! In-memory `KeyStore` backend. Tests + ephemeral embedded.

use async_lock::RwLock;
use async_trait::async_trait;
use rand::RngCore;
use std::collections::HashMap;
use std::sync::Arc;

use defra_core::block::generate_cid_from_bytes;
use defra_core::Encryption;

use crate::error::{Error, Result};
use crate::store::{KeyStore, StoredKey};
use crate::types::{EncryptionCid, KeyScope};

/// In-memory `KeyStore`. All entries live in RAM behind an async `RwLock`.
/// Ephemeral — process restart loses all keys.
#[derive(Default)]
pub struct MemoryKeyStore {
    inner: Arc<RwLock<HashMap<EncryptionCid, StoredKey>>>,
}

impl MemoryKeyStore {
    /// Construct an empty `MemoryKeyStore`.
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl KeyStore for MemoryKeyStore {
    async fn put(&self, cid: EncryptionCid, stored: StoredKey) -> Result<()> {
        self.inner.write().await.insert(cid, stored);
        Ok(())
    }

    async fn get(&self, cid: &EncryptionCid) -> Result<Option<StoredKey>> {
        Ok(self.inner.read().await.get(cid).cloned())
    }

    async fn generate(&self, scope: &KeyScope) -> Result<(EncryptionCid, StoredKey)> {
        let mut key = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut key);

        let (doc_id_bytes, field_name) = match scope {
            KeyScope::Document { doc_id, field } => (doc_id.as_bytes().to_vec(), field.clone()),
            KeyScope::Collection { collection_id } => {
                // Mirrors Go's internal/kms/pubsub.go: collection-scoped DEKs use an
                // empty doc_id and carry the collection_id in the field_name slot.
                (Vec::new(), Some(collection_id.clone()))
            }
        };
        let block = Encryption {
            doc_id: doc_id_bytes,
            field_name,
            key: key.to_vec(),
        };
        let block_bytes = block
            .to_dag_cbor()
            .map_err(|e| Error::Storage(format!("encode encryption block: {e}")))?;
        let cid = generate_cid_from_bytes(&block_bytes)
            .map_err(|e| Error::Storage(format!("cid from block: {e}")))?;

        let stored = StoredKey { key, block_bytes };
        self.inner.write().await.insert(cid, stored.clone());
        Ok((cid, stored))
    }

    async fn delete(&self, cid: &EncryptionCid) -> Result<()> {
        self.inner.write().await.remove(cid);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<EncryptionCid>> {
        Ok(self.inner.read().await.keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyScope;

    #[tokio::test]
    async fn generate_returns_cid_key_and_block_bytes() {
        let store = MemoryKeyStore::new();
        let (cid, stored) = store
            .generate(&KeyScope::Document {
                doc_id: "d1".into(),
                field: None,
            })
            .await
            .unwrap();
        assert!(!stored.block_bytes.is_empty());
        // Decoding the stored block yields the same key bytes.
        let block = defra_core::Encryption::from_dag_cbor(&stored.block_bytes).unwrap();
        assert_eq!(block.key, stored.key.to_vec());
        // get returns the same record.
        let got = store.get(&cid).await.unwrap().unwrap();
        assert_eq!(got.key, stored.key);
        assert_eq!(got.block_bytes, stored.block_bytes);
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let store = MemoryKeyStore::new();
        let block = defra_core::Encryption {
            doc_id: b"d".to_vec(),
            field_name: None,
            key: vec![0x42; 32],
        };
        let block_bytes = block.to_dag_cbor().unwrap();
        let cid: EncryptionCid = defra_core::block::generate_cid_from_bytes(&block_bytes).unwrap();
        store
            .put(
                cid,
                StoredKey {
                    key: [0x42; 32],
                    block_bytes: block_bytes.clone(),
                },
            )
            .await
            .unwrap();
        let got = store.get(&cid).await.unwrap().unwrap();
        assert_eq!(got.key, [0x42; 32]);
        assert_eq!(got.block_bytes, block_bytes);
    }

    #[tokio::test]
    async fn delete_removes_entry() {
        let store = MemoryKeyStore::new();
        let (cid, _) = store
            .generate(&KeyScope::Document {
                doc_id: "x".into(),
                field: None,
            })
            .await
            .unwrap();
        store.delete(&cid).await.unwrap();
        assert!(store.get(&cid).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_returns_all_cids() {
        let store = MemoryKeyStore::new();
        let (c1, _) = store
            .generate(&KeyScope::Document {
                doc_id: "a".into(),
                field: None,
            })
            .await
            .unwrap();
        let (c2, _) = store
            .generate(&KeyScope::Collection {
                collection_id: "b".into(),
            })
            .await
            .unwrap();
        let mut list = store.list().await.unwrap();
        list.sort();
        let mut expected = vec![c1, c2];
        expected.sort();
        assert_eq!(list, expected);
    }
}
