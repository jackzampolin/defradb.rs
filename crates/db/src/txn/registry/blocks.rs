//! Block signature verification inside a transaction.

use super::*;

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Verify a block signature using the existing transaction's blockstore view.
    pub async fn verify_block_signature_in_txn(
        &self,
        txn_id: &str,
        document_acp: &dyn acp::DocumentACP,
        cid_str: &str,
        public_key_hex: &str,
        key_type: crypto::KeyType,
        caller_identity: &acp::Identity,
    ) -> Result<()> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let shared_txn = ctx.fetcher_shared_txn();
        let txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_ref().ok_or(Error::TxnNotActive)?;

        crate::block_verify::verify_block_signature_in_txn(
            &self.db,
            document_acp,
            txn,
            cid_str,
            public_key_hex,
            key_type,
            caller_identity,
        )
        .await
        .map_err(Error::Other)
    }

    /// Verify a block's embedded signature inside an active transaction and
    /// return the signer DID.
    pub async fn verified_block_signer_did_in_txn(
        &self,
        txn_id: &str,
        document_acp: &dyn acp::DocumentACP,
        cid_str: &str,
        caller_identity: &acp::Identity,
    ) -> Result<String> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let (blockstore, systemstore) = {
            let shared_txn = ctx.fetcher_shared_txn();
            let txn_guard = shared_txn.lock().await;
            let txn = txn_guard.as_ref().ok_or(Error::TxnNotActive)?;
            (txn.blockstore()?, txn.systemstore()?)
        };

        crate::block_verify::verified_block_signer_did_with_blockstore(
            &self.db,
            document_acp,
            blockstore,
            systemstore,
            cid_str,
            caller_identity,
        )
        .await
        .map_err(Error::Other)
    }
}
