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
    /// Checks (all return `Err` on failure to match Go's convention and the
    /// rest of the Rust crypto crate — see #733):
    ///
    /// 1. Path is non-empty
    /// 2. First node CID matches `leaf_cid`
    /// 3. Last node CID matches `root_cid`
    /// 4. Each node's CID matches its content hash (cryptographic failure)
    /// 5. Each node's `heads` contains the next node's CID (broken chain)
    ///
    /// The chain structure is: leaf (newest) -> ... -> root (oldest).
    /// Each node points to its parent(s) via the `heads` field.
    ///
    /// # Returns
    ///
    /// - `Ok(true)` — proof verifies
    /// - `Err(_)` — verification failed at any of the checks above. The
    ///   error variant is `Error::BlockError` for structural input errors
    ///   (empty path, anchor mismatch) and `Error::Crypto` for cryptographic
    ///   failures (hash mismatch, broken chain link).
    ///
    /// Note: this function never returns `Ok(false)`. Both invariants are
    /// security-sensitive enough that a caller who treats `Ok` as success
    /// (e.g. `if result.is_ok()` or `let _ = result?; trust_proof()`)
    /// must not be able to bypass verification by writing the obvious code.
    /// See #733.
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
        // A hash mismatch is a cryptographic verification failure — return
        // Err so callers using `?` propagate it instead of treating Ok(false)
        // as success. See #733.
        for (idx, node) in self.path.iter().enumerate() {
            if !node.verify_cid()? {
                return Err(Error::Crypto(format!(
                    "proof node CID does not match content hash at path index {}",
                    idx
                )));
            }
        }

        // Verify chain integrity: each node's heads should contain the next
        // node's CID. A broken chain link is a cryptographic verification
        // failure — return Err for the same reason as the hash check above.
        for i in 0..self.path.len() - 1 {
            let current_block = self.path[i].decode_block()?;
            let next_cid = &self.path[i + 1].cid;

            let has_link = current_block
                .heads
                .as_ref()
                .map(|heads| heads.contains(next_cid))
                .unwrap_or(false);

            if !has_link {
                return Err(Error::Crypto(format!(
                    "proof chain is broken: node at path index {} does not list \
                     node at index {} (cid: {}) in its heads",
                    i,
                    i + 1,
                    next_cid
                )));
            }
        }

        Ok(true)
    }

    /// Serialize the proof to DAG-CBOR
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>> {
        Ok(serde_ipld_dagcbor::to_vec(self)?)
    }

    /// Deserialize the proof from DAG-CBOR
    pub fn from_dag_cbor(bytes: &[u8]) -> Result<Self> {
        Ok(serde_ipld_dagcbor::from_slice(bytes)?)
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
