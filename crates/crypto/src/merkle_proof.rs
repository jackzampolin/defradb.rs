//! Merkle proof generation and verification
//!
//! This module provides functionality for generating and verifying Merkle proofs
//! in the DefraDB Merkle-CRDT architecture. Proofs demonstrate that a specific
//! block is part of a Merkle chain leading to a known root.
//!
//! # Architecture
//!
//! DefraDB stores data as IPLD blocks where each block references its parent(s)
//! via the `heads` field. A Merkle proof is a path from a leaf block to a root,
//! containing all intermediate blocks needed to verify the chain.
//!
//! # Wire Compatibility
//!
//! All types use DAG-CBOR serialization compatible with Go DefraDB for cross-
//! implementation verification.

use cid::Cid;
use defra_core::block::{Block, Signature, SignatureHeader, SignatureType};
use defra_core::{Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};

use crate::keys::{PrivateKey, PublicKey};
use crate::types::KeyType;

/// Maximum allowed proof path length to prevent DoS via large proofs
const MAX_PROOF_PATH_LENGTH: usize = 1000;

/// Maximum nodes to visit during BFS traversal to prevent DoS via large DAGs
const MAX_TRAVERSAL_NODES: usize = 10_000;

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
        const SHA2_256_CODE: u64 = 0x12;
        const DAG_CBOR_CODEC: u64 = 0x71;

        // Explicitly check for supported hash algorithm and codec
        if self.cid.hash().code() != SHA2_256_CODE {
            return Err(Error::BlockError(format!(
                "Unsupported hash algorithm: 0x{:x} (only SHA2-256 0x12 is supported)",
                self.cid.hash().code()
            )));
        }
        if self.cid.codec() != DAG_CBOR_CODEC {
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
    /// 1. Path length is within limits
    /// 2. Path is non-empty
    /// 3. First node CID matches leaf_cid
    /// 4. Last node CID matches root_cid
    /// 5. Each node's CID matches its content hash
    /// 6. Each node's heads contains the next node's CID (child -> parent chain)
    ///
    /// The chain structure is: leaf (newest) -> ... -> root (oldest)
    /// Each node points to its parent(s) via the `heads` field.
    pub fn verify(&self) -> Result<bool> {
        // Check path length to prevent DoS
        if self.path.len() > MAX_PROOF_PATH_LENGTH {
            return Err(Error::BlockError(format!(
                "Proof path exceeds maximum length: {} > {}",
                self.path.len(),
                MAX_PROOF_PATH_LENGTH
            )));
        }

        if self.path.is_empty() {
            return Ok(false);
        }

        // Verify leaf CID matches
        if self.path[0].cid != self.leaf_cid {
            return Ok(false);
        }

        // Verify root CID matches
        if self.path[self.path.len() - 1].cid != self.root_cid {
            return Ok(false);
        }

        // Verify each node's CID matches its content
        for node in &self.path {
            if !node.verify_cid()? {
                return Ok(false);
            }
        }

        // Verify chain integrity: each node's heads should contain the next node's CID
        // This is because in a child -> parent chain, child.heads points to parent
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

/// A signed Merkle proof with cryptographic attestation
///
/// The signature covers the entire proof content (leaf_cid, root_cid, and path).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignedMerkleProof {
    /// The underlying Merkle proof
    pub proof: MerkleProof,

    /// Signature over the proof
    pub signature: Signature,
}

impl SignedMerkleProof {
    /// Create a signed proof from a proof and private key
    ///
    /// The identity field is stored as hex-encoded public key bytes for
    /// wire compatibility with Go DefraDB.
    pub fn sign(proof: MerkleProof, private_key: &dyn PrivateKey) -> Result<Self> {
        let proof_bytes = proof.to_dag_cbor()?;
        let sig_value = private_key.sign(&proof_bytes)?;

        let sig_type = match private_key.key_type() {
            KeyType::Ed25519 => SignatureType::EdDSA,
            KeyType::Secp256k1 => SignatureType::ES256K,
            _ => {
                return Err(Error::Crypto(
                    "Unsupported key type for signing".to_string(),
                ))
            }
        };

        let public_key = private_key.public_key();
        // Go stores identity as hex-encoded public key string bytes
        let identity = public_key.to_hex_string().into_bytes();
        let header = SignatureHeader::new(sig_type, identity);

        Ok(Self {
            proof,
            signature: Signature::new(header, sig_value),
        })
    }

    /// Verify the signature and proof validity
    ///
    /// Returns true only if both the signature is valid AND the proof is valid.
    /// The public key type must match the signature algorithm.
    pub fn verify(&self, public_key: &dyn PublicKey) -> Result<bool> {
        // Validate key type matches signature algorithm
        let expected_key_type = match self.signature.header.sig_type {
            SignatureType::EdDSA => KeyType::Ed25519,
            SignatureType::ES256K => KeyType::Secp256k1,
        };
        if public_key.key_type() != expected_key_type {
            return Err(Error::Crypto(format!(
                "Key type mismatch: signature requires {:?}, got {:?}",
                expected_key_type,
                public_key.key_type()
            )));
        }

        // Verify the identity in the signature matches the provided key
        // Identity is stored as hex-encoded public key string bytes
        let expected_identity = public_key.to_hex_string().into_bytes();
        if self.signature.header.identity != expected_identity {
            return Ok(false);
        }

        // Verify the signature
        let proof_bytes = self.proof.to_dag_cbor()?;
        if !public_key.verify(&proof_bytes, &self.signature.value)? {
            return Ok(false);
        }

        // Verify the proof itself
        self.proof.verify()
    }

    /// Verify using the embedded public key from the signature
    ///
    /// Extracts the public key from the signature header and verifies.
    pub fn verify_with_embedded_key(&self) -> Result<bool> {
        let public_key = extract_public_key_from_signature(&self.signature)?;

        // Verify the signature
        let proof_bytes = self.proof.to_dag_cbor()?;
        if !public_key.verify(&proof_bytes, &self.signature.value)? {
            return Ok(false);
        }

        // Verify the proof itself
        self.proof.verify()
    }

    /// Serialize to DAG-CBOR
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>> {
        serde_ipld_dagcbor::to_vec(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Deserialize from DAG-CBOR
    pub fn from_dag_cbor(bytes: &[u8]) -> Result<Self> {
        serde_ipld_dagcbor::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
    }
}

/// Blockstore trait for proof extraction
///
/// This trait abstracts block retrieval to allow proof extraction from
/// any block storage implementation.
#[async_trait::async_trait]
pub trait ProofBlockstore: Send + Sync {
    /// Get a block by CID
    async fn get_block(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
}

/// Extract a Merkle proof from a blockstore
///
/// Traverses from the leaf block to the root by following the `heads` references.
/// Uses BFS to find the shortest path when multiple paths exist.
///
/// # Arguments
///
/// * `blockstore` - The block storage to retrieve blocks from
/// * `leaf_cid` - The CID of the leaf block to start from
/// * `root_cid` - The CID of the root block to reach
///
/// # Returns
///
/// * `Ok(Some(proof))` - A valid proof if a path exists
/// * `Ok(None)` - No path exists from leaf to root
/// * `Err(e)` - An error occurred during traversal
pub async fn extract_proof<B: ProofBlockstore>(
    blockstore: &B,
    leaf_cid: Cid,
    root_cid: Cid,
) -> Result<Option<MerkleProof>> {
    // Handle trivial case: leaf == root
    if leaf_cid == root_cid {
        let block_data = blockstore
            .get_block(&leaf_cid)
            .await?
            .ok_or_else(|| Error::BlockError(format!("Block not found: {}", leaf_cid)))?;

        let node = ProofNode {
            cid: leaf_cid,
            data: block_data,
        };
        return Ok(Some(MerkleProof::new(leaf_cid, root_cid, vec![node])));
    }

    // BFS to find path from leaf to root
    // We traverse "forward" through heads references
    let mut visited: HashSet<Cid> = HashSet::new();
    let mut parent_map: HashMap<Cid, Cid> = HashMap::new(); // child -> parent
    let mut block_cache: HashMap<Cid, Vec<u8>> = HashMap::new();
    let mut queue: VecDeque<Cid> = VecDeque::new();

    // Start from leaf
    let leaf_data = blockstore
        .get_block(&leaf_cid)
        .await?
        .ok_or_else(|| Error::BlockError(format!("Leaf block not found: {}", leaf_cid)))?;

    block_cache.insert(leaf_cid, leaf_data);
    visited.insert(leaf_cid);
    queue.push_back(leaf_cid);

    // BFS traversal following heads references
    while let Some(current_cid) = queue.pop_front() {
        // Check traversal limit to prevent DoS via large DAGs
        if visited.len() > MAX_TRAVERSAL_NODES {
            return Err(Error::BlockError(format!(
                "BFS traversal exceeded maximum nodes ({})",
                MAX_TRAVERSAL_NODES
            )));
        }

        // Get the current block data
        let block_data = if let Some(data) = block_cache.get(&current_cid) {
            data.clone()
        } else {
            let data = blockstore
                .get_block(&current_cid)
                .await?
                .ok_or_else(|| Error::BlockError(format!("Block not found: {}", current_cid)))?;
            block_cache.insert(current_cid, data.clone());
            data
        };

        let block = Block::from_dag_cbor(&block_data)?;

        // In DefraDB's Merkle structure, newer blocks point to older blocks via heads.
        // Following heads from leaf toward root naturally traverses to ancestors.

        if let Some(heads) = &block.heads {
            for parent_cid in heads {
                if !visited.contains(parent_cid) {
                    // Fetch parent block - this must exist if referenced
                    let parent_data = blockstore.get_block(parent_cid).await?.ok_or_else(|| {
                        Error::BlockError(format!(
                            "Missing parent block {} referenced by {}",
                            parent_cid, current_cid
                        ))
                    })?;

                    block_cache.insert(*parent_cid, parent_data);
                    visited.insert(*parent_cid);
                    parent_map.insert(*parent_cid, current_cid);
                    queue.push_back(*parent_cid);

                    // Check if we reached the root
                    if *parent_cid == root_cid {
                        // Reconstruct path from root back to leaf
                        let path = reconstruct_path(&parent_map, &block_cache, leaf_cid, root_cid)?;
                        return Ok(Some(MerkleProof::new(leaf_cid, root_cid, path)));
                    }
                }
            }
        }
    }

    // No path found
    Ok(None)
}

/// Reconstruct the path from leaf to root using the parent map
fn reconstruct_path(
    parent_map: &HashMap<Cid, Cid>,
    block_cache: &HashMap<Cid, Vec<u8>>,
    leaf_cid: Cid,
    root_cid: Cid,
) -> Result<Vec<ProofNode>> {
    // Build path from root back to leaf following parent_map
    let mut path = Vec::new();
    let mut current = root_cid;

    // Start at root
    let root_data = block_cache
        .get(&root_cid)
        .ok_or_else(|| Error::BlockError("Root block not in cache".to_string()))?;
    path.push(ProofNode {
        cid: root_cid,
        data: root_data.clone(),
    });

    // Follow the chain back to leaf
    while current != leaf_cid {
        let child = parent_map
            .get(&current)
            .ok_or_else(|| Error::BlockError("Path reconstruction failed".to_string()))?;

        let child_data = block_cache
            .get(child)
            .ok_or_else(|| Error::BlockError("Block not in cache".to_string()))?;

        path.push(ProofNode {
            cid: *child,
            data: child_data.clone(),
        });

        current = *child;
    }

    // Reverse to get leaf -> root order
    path.reverse();
    Ok(path)
}

/// Compute CID from raw DAG-CBOR bytes
fn compute_cid(bytes: &[u8]) -> Result<Cid> {
    use multihash::MultihashGeneric;

    const DAG_CBOR_CODEC: u64 = 0x71;
    const SHA2_256_CODE: u64 = 0x12;

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();

    let mh = MultihashGeneric::<64>::wrap(SHA2_256_CODE, &digest)
        .map_err(|e| Error::BlockError(format!("Failed to create multihash: {}", e)))?;

    Ok(Cid::new_v1(DAG_CBOR_CODEC, mh))
}

/// Extract public key from a signature header
///
/// The identity field is stored as hex-encoded public key string bytes
/// for wire compatibility with Go DefraDB.
fn extract_public_key_from_signature(sig: &Signature) -> Result<Box<dyn PublicKey>> {
    let key_type = match sig.header.sig_type {
        SignatureType::EdDSA => KeyType::Ed25519,
        SignatureType::ES256K => KeyType::Secp256k1,
    };

    // Identity is stored as hex-encoded string bytes
    let hex_string = String::from_utf8(sig.header.identity.clone()).map_err(|e| {
        Error::Crypto(format!(
            "Invalid identity encoding in signature header: {}",
            e
        ))
    })?;

    crate::keys::generation::public_key_from_string(key_type, &hex_string)
}

/// Verify a standalone proof without blockstore access
///
/// This is useful for verifying proofs received over the network.
pub fn verify_proof(proof: &MerkleProof) -> Result<bool> {
    proof.verify()
}

/// Verify a signed proof without blockstore access
pub fn verify_signed_proof(proof: &SignedMerkleProof) -> Result<bool> {
    proof.verify_with_embedded_key()
}
