//! Merkle proof structure and verification.

use cid::Cid;
use defra_core::{Error, Result};
use serde::{Deserialize, Serialize};

use super::proof_node::ProofNode;

/// Merkle proof demonstrating a path from leaf to root
///
/// The proof contains an ordered sequence of blocks from the leaf (index 0)
/// to the root (last index). Each block in the path must have its CID
/// appear in the `heads` of the next block in the sequence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MerkleProof {
    /// The leaf CID this proof starts from
    pub leaf_cid: Cid,

    /// The root CID this proof ends at
    pub root_cid: Cid,

    /// The path from leaf to root (inclusive)
    ///
    /// Index 0 is the leaf block, last index is the root block.
    pub path: Vec<ProofNode>,
}

impl MerkleProof {
    /// Create a new Merkle proof
    pub fn new(leaf_cid: Cid, root_cid: Cid, path: Vec<ProofNode>) -> Self {
        Self {
            leaf_cid,
            root_cid,
            path,
        }
    }

    /// Verify this proof is valid
    ///
    /// Checks:
    /// 1. Path is non-empty (Err)
    /// 2. First node CID matches leaf_cid (Err — wrong input)
    /// 3. Last node CID matches root_cid (Err — wrong input)
    /// 4. Each node's CID matches its content hash (Ok(false) — cryptographic failure)
    /// 5. Each node's heads contains the next node's CID (Ok(false) — broken chain)
    ///
    /// The chain structure is: leaf (newest) -> ... -> root (oldest).
    /// Each node points to its parent(s) via the `heads` field.
    ///
    /// # Returns
    ///
    /// - `Ok(true)` — proof verifies
    /// - `Ok(false)` — proof is structurally valid but cryptographically incorrect
    ///   (content hash mismatch, broken chain link)
    /// - `Err(_)` — caller supplied structurally invalid input (empty path, anchor mismatch)
    pub fn verify(&self) -> Result<bool> {
        // Anchor/structural failures are caller errors — return Err so they can't be
        // silently ignored. Match Go's convention where "nothing to verify" is an error.
        if self.path.is_empty() {
            return Err(Error::BlockError("proof path is empty".to_string()));
        }

        if self.path[0].cid != self.leaf_cid {
            return Err(Error::BlockError(
                "proof leaf CID does not match first path node".to_string(),
            ));
        }

        if self.path[self.path.len() - 1].cid != self.root_cid {
            return Err(Error::BlockError(
                "proof root CID does not match last path node".to_string(),
            ));
        }

        // Verify each node's CID matches its content.
        // A hash mismatch is a legitimate cryptographic "no" — return Ok(false).
        for node in &self.path {
            if !node.verify_cid()? {
                return Ok(false);
            }
        }

        // Verify chain integrity: each node's heads should contain the next node's CID.
        // A broken chain link is a legitimate cryptographic "no" — return Ok(false).
        for i in 0..self.path.len() - 1 {
            let current_block = self.path[i].decode_block()?;
            let next_cid = &self.path[i + 1].cid;

            let has_link = current_block
                .heads
                .as_ref()
                .map(|heads| heads.contains(next_cid))
                .unwrap_or(false);

            if !has_link {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Serialize the proof to DAG-CBOR
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>> {
        serde_ipld_dagcbor::to_vec(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Deserialize the proof from DAG-CBOR
    pub fn from_dag_cbor(bytes: &[u8]) -> Result<Self> {
        serde_ipld_dagcbor::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Get the proof path length
    pub fn len(&self) -> usize {
        self.path.len()
    }

    /// Check if the proof path is empty
    pub fn is_empty(&self) -> bool {
        self.path.is_empty()
    }
}

/// Verify a standalone proof without blockstore access
///
/// This is useful for verifying proofs received over the network.
pub fn verify_proof(proof: &MerkleProof) -> Result<bool> {
    proof.verify()
}
