use std::sync::Arc;

use cid::Cid;
use defra_core::block::Encryption;
use defra_core::merge::BlockMetadata;
use storage::corekv::Store;

use super::{DbMergeHandler, MergeError};

#[cfg(not(target_arch = "wasm32"))]
fn spawn_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(target_arch = "wasm32")]
fn spawn_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    /// Fire-and-forget cross-peer DEK request for an encrypted field block
    /// whose owner is not yet known locally (its merge is deferred to the
    /// composite). Go parity: Go requests the DEK unconditionally at DAG-sync
    /// time (internal/db/p2p/sync_dag.go `kms.GetKeys`), BEFORE any merge
    /// admission — which is also what makes an unauthorized node's request
    /// observable as a serve-side denial on the key holder
    /// (proofs/tests/behavioral/kms.rs). The prefetch runs detached so the
    /// merge path never blocks on it; on success the reply handler caches the
    /// key in the local store.
    pub fn spawn_dek_prefetch(&self, enc_cid: Cid, metadata: &BlockMetadata<'_>) {
        let Some(kms) = self.kms() else {
            return;
        };
        {
            let mut seen = self.prefetched_dek_cids.lock().unwrap();
            if !seen.insert(enc_cid) {
                return;
            }
        }
        let ctx = Self::kms_request_context(Some(metadata));
        let prefetched_dek_cids = Arc::clone(&self.prefetched_dek_cids);
        spawn_task(async move {
            let result = match kms.get_keys(&ctx, std::slice::from_ref(&enc_cid)).await {
                Ok(results) => results.wait_all().await.map(|_| ()),
                Err(error) => Err(error),
            };
            prefetched_dek_cids.lock().unwrap().remove(&enc_cid);
            if let Err(error) = result {
                tracing::debug!(enc_cid = %enc_cid, error = %error, "DEK prefetch failed");
            }
        });
    }

    /// Decrypt block delta data using the encryption metadata block.
    ///
    /// If `encryption_cid` is Some, loads the Encryption block from encstore,
    /// falling back to the P2P blockstore when the metadata arrived via replay,
    /// extracts the AES key, and decrypts the data. Returns data unchanged if
    /// no encryption CID is present.
    fn kms_request_context(metadata: Option<&BlockMetadata<'_>>) -> kms::RequestContext {
        let Some(metadata) = metadata else {
            return kms::RequestContext::anonymous();
        };
        let Some(collection_id) = metadata.collection_id else {
            return kms::RequestContext::anonymous();
        };
        let Some(authorization) = metadata.explicit_replay_authorization_for(collection_id) else {
            return kms::RequestContext::anonymous();
        };
        match identity::Did::new(&authorization.authorizer_did) {
            Ok(did) => match &authorization.capability {
                Some(capability) => {
                    kms::RequestContext::with_explicit_replay(did, capability.clone())
                }
                None => kms::RequestContext::with_user(did),
            },
            Err(_) => kms::RequestContext::anonymous(),
        }
    }

    pub async fn decrypt_block_data(
        &self,
        data: &[u8],
        encryption_cid: Option<&Cid>,
        metadata: Option<&BlockMetadata<'_>>,
    ) -> std::result::Result<Vec<u8>, MergeError> {
        let enc_cid = match encryption_cid {
            Some(cid) => cid,
            None => return Ok(data.to_vec()),
        };

        // KMS path: fetch the DEK through the KMS (NAC/DAC-gated). The KMS
        // resolves the key locally or via cross-peer fetch and returns the
        // plaintext key; we then AES-GCM decrypt the block data.
        if let Some(kms) = self.kms() {
            let ctx = Self::kms_request_context(metadata);
            let results = kms.get_keys(&ctx, std::slice::from_ref(enc_cid)).await?;
            let mut receiver = results.into_receiver();
            let mut denied = None;
            let mut unavailable = None;
            while let Some(result) = receiver.recv().await {
                match result {
                    Ok((cid, key)) if cid == *enc_cid => {
                        return crypto::encryption::aes::decrypt_aes(None, data, &key, &[])
                            .map_err(|e| {
                                kms::Error::Crypto(format!("KMS-keyed decryption failed: {e}"))
                                    .into()
                            });
                    }
                    Ok(_) => {}
                    Err(error @ kms::Error::AccessDenied { .. }) => denied = Some(error),
                    Err(error) => unavailable = Some(error),
                }
            }
            return Err(unavailable
                .or(denied)
                .unwrap_or(kms::Error::KeyUnavailable)
                .into());
        }

        // Legacy path (unchanged): read the raw key directly from the
        // Encryption block in encstore/blockstore.
        let enc_txn = self.db.new_txn(true).await.map_err(MergeError::Database)?;
        let encstore = enc_txn.encstore().map_err(MergeError::Database)?;
        let enc_cid_bytes = enc_cid.to_bytes();
        let enc_data = if let Some(enc_data) = encstore
            .get(&enc_cid_bytes)
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?
        {
            enc_data
        } else if let Some(enc_data) = self
            .blockstore
            .get(enc_cid)
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?
        {
            enc_data
        } else {
            return Err(MergeError::Storage(format!(
                "Encryption block {} not found",
                enc_cid
            )));
        };

        let enc_block = Encryption::from_dag_cbor(&enc_data).map_err(|e| {
            MergeError::BlockDecode(format!("Failed to decode encryption block: {}", e))
        })?;

        // Decrypt using AES-256-GCM (nonce is prepended to ciphertext)
        match crypto::encryption::aes::decrypt_aes(None, data, &enc_block.key, &[]) {
            Ok(decrypted) => {
                tracing::debug!(
                    encryption_cid = %enc_cid,
                    plaintext_len = decrypted.len(),
                    "Decrypted block data"
                );
                Ok(decrypted)
            }
            Err(e) => {
                tracing::warn!(
                    encryption_cid = %enc_cid,
                    error = %e,
                    "Failed to decrypt block data"
                );
                Err(MergeError::MergeFailed(format!("Decryption failed: {}", e)))
            }
        }
    }
}
