//! Proof node for Merkle proofs.

use cid::Cid;
use defra_core::block::Block;
use defra_core::{Error, Result, DAG_CBOR_CODEC, SHA2_256_CODE};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A node in the Merkle proof path
///
/// Contains the block data and its computed CID for verification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProofNode {
    /// The CID of this block
    pub cid: Cid,

    /// The raw DAG-CBOR encoded block data
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

impl ProofNode {
    /// Create a new proof node from a block
    pub fn from_block(block: &Block) -> Result<Self> {
        let data = block.to_dag_cbor()?;
        let cid = block.generate_cid()?;
        Ok(Self { cid, data })
    }

    /// Verify this node's CID matches its data
    pub fn verify_cid(&self) -> Result<bool> {
        // Explicitly check for supported hash algorithm and codec
        if self.cid.hash().code() != *SHA2_256_CODE {
            return Err(Error::BlockError(format!(
                "Unsupported hash algorithm: 0x{:x} (only SHA2-256 0x12 is supported)",
                self.cid.hash().code()
            )));
        }
        if self.cid.codec() != *DAG_CBOR_CODEC {
            return Err(Error::BlockError(format!(
                "Unsupported codec: 0x{:x} (only DAG-CBOR 0x71 is supported)",
                self.cid.codec()
            )));
        }

        let computed_cid = compute_cid(&self.data)?;
        Ok(computed_cid == self.cid)
    }

    /// Decode the block data
    pub fn decode_block(&self) -> Result<Block> {
        Block::from_dag_cbor(&self.data)
    }
}

/// Compute CID from raw DAG-CBOR bytes
pub(crate) fn compute_cid(bytes: &[u8]) -> Result<Cid> {
    use multihash::MultihashGeneric;

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();

    let mh = MultihashGeneric::<64>::wrap(*SHA2_256_CODE, &digest)
        .map_err(|e| Error::BlockError(format!("Failed to create multihash: {}", e)))?;

    Ok(Cid::new_v1(*DAG_CBOR_CODEC, mh))
}
