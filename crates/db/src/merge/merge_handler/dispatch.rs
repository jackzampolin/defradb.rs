use async_trait::async_trait;
use cid::Cid;
use defra_core::block::{Block, CrdtDelta};
use defra_core::merge::{
    BlockMetadata, ExplicitReplayAuthorization, MergeBlock, MergeErrorDisposition, MergeHandler,
    MergeOutcome, RecoveredBlockMetadata,
};
use storage::corekv::Store;

use super::{DbMergeHandler, MergeError};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: blockstore::Blockstore + 'static> MergeHandler
    for DbMergeHandler<S, B>
{
    type Error = MergeError;

    fn error_disposition(&self, error: &Self::Error) -> MergeErrorDisposition {
        error.disposition()
    }

    async fn validate_authorization(
        &self,
        authorization: Option<&ExplicitReplayAuthorization>,
        block: &MergeBlock,
    ) -> Result<(), Self::Error> {
        self.validate_explicit_replay_authorization(authorization, block)
            .await
    }

    async fn recover_block_metadata(
        &self,
        cid: &Cid,
        block_data: &[u8],
    ) -> Result<Option<RecoveredBlockMetadata>, Self::Error> {
        self.recover_metadata_from_block(cid, block_data).await
    }

    async fn handle_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        // Go parity (internal/db/merge.go): merges race concurrent merges and
        // local writes on shared systemstore keys — the /seq/doc short-ID
        // sequence and co-owned block-ownership entries — so an optimistic
        // TxnConflict is expected business. Go retries `executeMerge` up to
        // MaxTxnRetries; the p2p layer treats a Failed merge as terminal (the
        // pusher was already acked), so dropping a conflicted merge silently
        // loses the document (observed as encrypted filtered-replication poll
        // timeouts on the Linux CI runner).
        const MAX_TXN_RETRIES: usize = 5;

        let mut result = self
            .merge_block_attempt(cid, block_data, metadata.clone())
            .await;
        let mut retry_count = 0;
        for attempt in 1..MAX_TXN_RETRIES {
            match &result {
                Err(e) if e.is_txn_conflict() => {
                    telemetry::record_retry_attempt(telemetry::RetryLayer::Merge);
                    retry_count += 1;
                    tracing::debug!(cid = %cid, attempt, "Merge txn conflict, retrying");
                    result = self
                        .merge_block_attempt(cid, block_data, metadata.clone())
                        .await;
                }
                _ => break,
            }
        }
        if retry_count > 0 && !result.as_ref().is_err_and(|error| error.is_txn_conflict()) {
            telemetry::record_retry_success(telemetry::RetryLayer::Merge);
        }
        if let Err(e) = &result {
            if e.is_txn_conflict() {
                telemetry::record_retry_exhaustion(telemetry::RetryLayer::Merge);
                tracing::warn!(
                    cid = %cid,
                    max_retries = MAX_TXN_RETRIES,
                    "Merge txn conflict retries exhausted — document merge failed"
                );
            }
        }
        match result {
            Err(error) if error.disposition() == MergeErrorDisposition::Terminal => {
                Ok(MergeOutcome::rejected(error.to_string()))
            }
            other => other,
        }
    }

    async fn handle_block_batch(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, Self::Error>> {
        if blocks.len() <= 1 {
            return self.merge_blocks_individually(blocks).await;
        }

        self.try_batch_merge_with_split(blocks).await
    }
}

impl<S: Store + 'static, B: blockstore::Blockstore + 'static> DbMergeHandler<S, B> {
    /// One merge attempt for a single block. Conflict retry lives in the
    /// `MergeHandler::handle_block` wrapper above (Go's `executeMerge` split).
    pub(crate) async fn merge_block_attempt(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, MergeError> {
        tracing::debug!(
            cid = %cid,
            block_size = block_data.len(),
            is_recovery = metadata.is_recovery,
            "Handling block for merge"
        );

        // Decode the block from DAG-CBOR
        let block =
            Block::from_dag_cbor(block_data).map_err(|e| MergeError::BlockDecode(e.to_string()))?;

        tracing::debug!(
            cid = %cid,
            delta_type = ?std::mem::discriminant(&block.delta),
            heads_count = block.heads.as_ref().map(|h| h.len()).unwrap_or(0),
            links_count = block.links.as_ref().map(|l| l.len()).unwrap_or(0),
            "Block decoded successfully"
        );

        // Verify block signature for P2P blocks (skip during recovery).
        // On success, populate verified_creator with the cryptographically
        // verified signer identity. Invalid signatures reject the block.
        let mut metadata = metadata;
        if !metadata.is_recovery {
            let verified = self.verify_block_signature(cid, &block, block_data).await?;
            metadata.verified_creator = verified;
        }

        // A standalone encrypted field block can only be merged once the
        // block-CID -> DocID ownership index names a single owner (recorded
        // by the composite merge that links it); otherwise it is merged via
        // its composite instead (see `process_lww_delta`/`process_counter_delta`).
        // Check that BEFORE attempting decryption: decrypting requires a KMS
        // fetch that may cross the network, and paying for that round trip
        // only to discard the result when ownership turns out to be unknown
        // wastes a fetch the composite merge will redundantly repeat moments
        // later (and, under load, can blow the caller's retry/poll budget).
        if block.encryption.is_some()
            && matches!(block.delta, CrdtDelta::Lww(_) | CrdtDelta::Counter(_))
            && self.resolve_field_block_doc_id(cid).await?.is_none()
        {
            // Still REQUEST the DEK (detached), Go-parity with sync-time
            // GetKeys: it warms the local key store for the composite merge
            // and keeps the serve-side authorization decision — including the
            // observable denial for unauthorized nodes — prompt.
            if let Some(enc_cid) = block.encryption {
                self.spawn_dek_prefetch(enc_cid, &metadata);
            }
            return Ok(MergeOutcome::terminal_skip(
                "field block has no unambiguous owner; merged via its composite",
            ));
        }

        // Decrypt delta data if the block has encryption.
        // If decryption fails (encryption key block unavailable), skip the
        // standalone field merge -- the composite merge will re-attempt
        // decryption when it processes the linked field blocks.
        let decrypted_block;
        let effective_block = if block.encryption.is_some() {
            match &block.delta {
                CrdtDelta::Lww(payload) => {
                    match self
                        .decrypt_block_data(
                            &payload.data,
                            block.encryption.as_ref(),
                            Some(&metadata),
                        )
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Lww(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(error @ MergeError::Kms(kms::Error::AccessDenied { .. })) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone LWW block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                        Err(error @ MergeError::Kms(_)) => return Err(error),
                        Err(error) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone LWW block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                    }
                }
                CrdtDelta::Counter(payload) => {
                    match self
                        .decrypt_block_data(
                            &payload.data,
                            block.encryption.as_ref(),
                            Some(&metadata),
                        )
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Counter(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(error @ MergeError::Kms(kms::Error::AccessDenied { .. })) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone Counter block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                        Err(error @ MergeError::Kms(_)) => return Err(error),
                        Err(error) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone Counter block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                    }
                }
                _ => &block,
            }
        } else {
            &block
        };

        // Process based on delta type
        match &effective_block.delta {
            CrdtDelta::Lww(payload) => self.process_lww_delta(cid, payload, &metadata).await,
            CrdtDelta::Counter(payload) => {
                self.process_counter_delta(cid, payload, &metadata).await
            }
            CrdtDelta::Composite(payload) => {
                self.process_composite_delta(cid, &block, payload, &metadata, false, 0)
                    .await
            }
            CrdtDelta::Collection(payload) => {
                self.process_collection_delta(cid, &block, payload, &metadata, 0)
                    .await
            }
            CrdtDelta::FieldDefinition(_) => {
                // Field definitions are processed as part of CollectionDefinition
                tracing::debug!(cid = %cid, "FieldDefinition delta - skipping (processed with collection)");
                Ok(MergeOutcome::terminal_skip(
                    "field definition processed with collection",
                ))
            }
            CrdtDelta::CollectionDefinition(payload) => {
                self.process_collection_definition_delta(cid, &block, payload, &metadata)
                    .await
            }
            CrdtDelta::CollectionSet(_) => {
                tracing::debug!(cid = %cid, "CollectionSet delta - skipping");
                Ok(MergeOutcome::terminal_skip("collection set delta"))
            }
            // Only the variant discriminant is reported — `CrdtDelta` carries
            // field-value bytes from user documents and must not be formatted
            // into error strings that may end up in logs.
            other => Err(MergeError::UnsupportedDelta(format!(
                "unhandled CrdtDelta variant in merge dispatch: {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }
}
