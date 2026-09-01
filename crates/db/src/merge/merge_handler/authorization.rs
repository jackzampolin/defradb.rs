use defra_core::block::Block;
use defra_core::merge::{ExplicitReplayAuthorization, MergeBlock};
use storage::corekv::Store;

use super::{DbMergeHandler, MergeError};

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    pub(crate) async fn validate_explicit_replay_authorization(
        &self,
        authorization: Option<&ExplicitReplayAuthorization>,
        block: &MergeBlock,
    ) -> Result<(), MergeError> {
        let Some(authorization) = authorization else {
            return Ok(());
        };

        if authorization.collection_id != block.collection_id {
            return Err(MergeError::InvalidReplayAuthorization(format!(
                "explicit replay authorization collection '{}' does not match block collection '{}'",
                authorization.collection_id, block.collection_id
            )));
        }

        let decoded_block = Block::from_dag_cbor(&block.block_data)
            .map_err(|error| MergeError::BlockDecode(error.to_string()))?;
        let verified_creator = self
            .verify_block_signature(&block.cid, &decoded_block, &block.block_data)
            .await?;
        let effective_creator = verified_creator
            .as_deref()
            .unwrap_or(block.creator.as_str());

        if effective_creator != authorization.authorizer_did {
            return Err(MergeError::InvalidReplayAuthorization(format!(
                "explicit replay authorization authorizer '{}' does not match block creator '{}'",
                authorization.authorizer_did, effective_creator
            )));
        }

        Ok(())
    }
}
