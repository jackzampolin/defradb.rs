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

mod extraction;
mod proof;
mod proof_node;
mod signed_proof;

// Re-export all public types
pub use extraction::{extract_proof, ProofBlockstore};
pub use proof::{verify_proof, MerkleProof};
pub use proof_node::ProofNode;
pub use signed_proof::{verify_signed_proof, SignedMerkleProof};
