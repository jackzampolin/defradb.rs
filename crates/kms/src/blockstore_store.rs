//! Blockstore-backed `KeyStore`.
//!
//! Mirrors Go's `internal/kms/enc_store.go`: the KMS reads/writes `Encryption`
//! metadata blocks from the node's durable IPLD store rather than a RAM-only
//! map. This lets the KMS serve a DEK for ANY encrypted write — including
//! blocks written by the legacy (non-KMS) auto-commit path — so cross-peer key
//! fetch (rust→rust, rust→go) works.
//!
//! To keep `kms` free of a `db` dependency (db depends on kms), the durable
//! store is abstracted behind the [`EncBlockStore`] trait, which the node
//! implements over its encstore→blockstore.

use async_trait::async_trait;
use defra_core::thread_bounds::MaybeSendSync;
use rand::RngCore;

use defra_core::block::generate_cid_from_bytes;
use defra_core::Encryption;

use crate::error::{Error, Result};
use crate::store::{KeyStore, StoredKey};
use crate::types::{EncryptionCid, KeyScope};

/// Durable source of `Encryption`-block bytes, keyed by CID.
///
/// The node provides an impl that reads its encstore→blockstore (mirroring the
/// merge decrypt path) and writes to its blockstore. Keeps `kms` free of a
/// `db` dependency.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait EncBlockStore: MaybeSendSync {
    /// Fetch the raw CBOR bytes of the `Encryption` block for this CID.
    /// `None` if not held in the durable store.
    async fn get_block(&self, cid: &EncryptionCid) -> Result<Option<Vec<u8>>>;

    /// Persist the raw CBOR bytes of an `Encryption` block under its CID.
    async fn put_block(&self, cid: EncryptionCid, bytes: Vec<u8>) -> Result<()>;
}

/// `KeyStore` backed by the node's durable `Encryption`-block store.
///
/// `generate`/`put` write the block to the durable store; `get` reads it back
/// and decodes the DEK. `delete` is a no-op and `list` is unsupported because
/// the blockstore is not enumerable by encryption-CID.
pub struct BlockstoreKeyStore {
    inner: std::sync::Arc<dyn EncBlockStore>,
}

impl BlockstoreKeyStore {
    /// Wrap a durable [`EncBlockStore`] as a `KeyStore`.
    pub fn new(inner: std::sync::Arc<dyn EncBlockStore>) -> Self {
        Self { inner }
    }
}

fn decode_stored(block_bytes: Vec<u8>) -> Result<StoredKey> {
    let block = Encryption::from_dag_cbor(&block_bytes)
        .map_err(|e| Error::Storage(format!("decode encryption block: {e}")))?;
    let key: [u8; 32] = block.key.as_slice().try_into().map_err(|_| {
        Error::Storage(format!(
            "encryption block key is {} bytes, expected 32",
            block.key.len()
        ))
    })?;
    Ok(StoredKey { key, block_bytes })
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl KeyStore for BlockstoreKeyStore {
    async fn put(&self, cid: EncryptionCid, stored: StoredKey) -> Result<()> {
        self.inner.put_block(cid, stored.block_bytes.clone()).await
    }

    async fn get(&self, cid: &EncryptionCid) -> Result<Option<StoredKey>> {
        match self.inner.get_block(cid).await? {
            Some(bytes) => Ok(Some(decode_stored(bytes)?)),
            None => Ok(None),
        }
    }

    async fn generate(&self, scope: &KeyScope) -> Result<(EncryptionCid, StoredKey)> {
        let mut key = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut key);

        let (doc_id_bytes, field_name) = match scope {
            KeyScope::Document { doc_id, field } => (doc_id.as_bytes().to_vec(), field.clone()),
            KeyScope::Collection { collection_id } => {
                // Mirrors Go's internal/kms/pubsub.go: collection-scoped DEKs use
                // an empty doc_id and carry the collection_id in the field_name slot.
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

        self.inner.put_block(cid, block_bytes.clone()).await?;
        Ok((cid, StoredKey { key, block_bytes }))
    }

    async fn delete(&self, _cid: &EncryptionCid) -> Result<()> {
        // Encryption blocks are content-addressed and immutable; the durable
        // blockstore is not pruned per-DEK. No-op.
        Ok(())
    }

    async fn list(&self) -> Result<Vec<EncryptionCid>> {
        Err(Error::Unsupported(
            "BlockstoreKeyStore does not support listing by encryption CID",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_lock::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Default)]
    struct FakeEncBlockStore {
        inner: RwLock<HashMap<EncryptionCid, Vec<u8>>>,
    }

    #[async_trait]
    impl EncBlockStore for FakeEncBlockStore {
        async fn get_block(&self, cid: &EncryptionCid) -> Result<Option<Vec<u8>>> {
            Ok(self.inner.read().await.get(cid).cloned())
        }
        async fn put_block(&self, cid: EncryptionCid, bytes: Vec<u8>) -> Result<()> {
            self.inner.write().await.insert(cid, bytes);
            Ok(())
        }
    }

    #[tokio::test]
    async fn generate_then_get_roundtrips_through_durable_store() {
        let fake = Arc::new(FakeEncBlockStore::default());
        let store = BlockstoreKeyStore::new(fake.clone());
        let (cid, stored) = store
            .generate(&KeyScope::Document {
                doc_id: "d1".into(),
                field: None,
            })
            .await
            .unwrap();

        // The block landed in the durable store.
        assert!(fake.get_block(&cid).await.unwrap().is_some());

        // get reads it back and recovers the same key + bytes.
        let got = store.get(&cid).await.unwrap().unwrap();
        assert_eq!(got.key, stored.key);
        assert_eq!(got.block_bytes, stored.block_bytes);
    }

    #[tokio::test]
    async fn get_serves_a_block_written_outside_the_kms() {
        // Simulates the legacy write path: a block placed in the durable store
        // by something other than KMS::generate. The KMS must still serve it.
        let fake = Arc::new(FakeEncBlockStore::default());
        let block = Encryption {
            doc_id: b"doc".to_vec(),
            field_name: Some("title".into()),
            key: vec![0x07; 32],
        };
        let block_bytes = block.to_dag_cbor().unwrap();
        let cid = generate_cid_from_bytes(&block_bytes).unwrap();
        fake.put_block(cid, block_bytes.clone()).await.unwrap();

        let store = BlockstoreKeyStore::new(fake);
        let got = store.get(&cid).await.unwrap().unwrap();
        assert_eq!(got.key, [0x07; 32]);
        assert_eq!(got.block_bytes, block_bytes);
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let fake = Arc::new(FakeEncBlockStore::default());
        let store = BlockstoreKeyStore::new(fake);
        let block = Encryption {
            doc_id: b"x".to_vec(),
            field_name: None,
            key: vec![1u8; 32],
        };
        let cid = generate_cid_from_bytes(&block.to_dag_cbor().unwrap()).unwrap();
        assert!(store.get(&cid).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_persists_and_list_unsupported() {
        let fake = Arc::new(FakeEncBlockStore::default());
        let store = BlockstoreKeyStore::new(fake);
        let block = Encryption {
            doc_id: b"d".to_vec(),
            field_name: None,
            key: vec![0x42; 32],
        };
        let block_bytes = block.to_dag_cbor().unwrap();
        let cid = generate_cid_from_bytes(&block_bytes).unwrap();
        store
            .put(
                cid,
                StoredKey {
                    key: [0x42; 32],
                    block_bytes,
                },
            )
            .await
            .unwrap();
        assert_eq!(store.get(&cid).await.unwrap().unwrap().key, [0x42; 32]);
        assert!(matches!(store.list().await, Err(Error::Unsupported(_))));
    }
}
