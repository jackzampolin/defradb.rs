//! Integration tests for merkle proof generation and verification

use cid::Cid;
use crypto::keys::generation::{generate_ed25519, generate_secp256k1};
use crypto::keys::PrivateKey;
use crypto::merkle_proof::{
    extract_proof, verify_proof, verify_signed_proof, MerkleProof, ProofBlockstore, ProofNode,
    SignedMerkleProof,
};
use defra_core::block::{Block, CrdtDelta, LwwDeltaPayload};
use defra_core::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory blockstore for testing
struct MemoryProofBlockstore {
    blocks: Arc<RwLock<HashMap<Cid, Vec<u8>>>>,
}

impl MemoryProofBlockstore {
    fn new() -> Self {
        Self {
            blocks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn put(&self, cid: Cid, data: Vec<u8>) {
        self.blocks.write().await.insert(cid, data);
    }
}

#[async_trait::async_trait]
impl ProofBlockstore for MemoryProofBlockstore {
    async fn get_block(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        Ok(self.blocks.read().await.get(cid).cloned())
    }
}

fn create_test_delta(doc_id: &str, field: &str) -> CrdtDelta {
    CrdtDelta::Lww(LwwDeltaPayload {
        doc_id: doc_id.as_bytes().to_vec(),
        field_name: field.to_string(),
        priority: 1,
        schema_version_id: "v1".to_string(),
        data: b"test".to_vec(),
    })
}

#[tokio::test]
async fn test_proof_node_verify_cid() {
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let node = ProofNode::from_block(&block).unwrap();

    assert!(node.verify_cid().unwrap());
}

#[tokio::test]
async fn test_proof_node_corrupt_data_fails_verification() {
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let mut node = ProofNode::from_block(&block).unwrap();

    // Corrupt the data
    if !node.data.is_empty() {
        node.data[0] ^= 0xFF;
    }

    assert!(!node.verify_cid().unwrap());
}

#[tokio::test]
async fn test_single_block_proof() {
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();

    let proof = MerkleProof::new(cid, cid, vec![node]);
    assert!(proof.verify().unwrap());
}

#[tokio::test]
async fn test_two_block_chain_proof() {
    // Create genesis block (root)
    let root_block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let root_cid = root_block.generate_cid().unwrap();
    let root_node = ProofNode::from_block(&root_block).unwrap();

    // Create child block pointing to root
    let child_block = Block::new(create_test_delta("doc1", "name"), vec![root_cid], vec![]);
    let child_cid = child_block.generate_cid().unwrap();
    let child_node = ProofNode::from_block(&child_block).unwrap();

    // Proof from child to root
    let proof = MerkleProof::new(child_cid, root_cid, vec![child_node, root_node]);
    assert!(proof.verify().unwrap());
}

#[tokio::test]
async fn test_proof_wrong_root_fails() {
    // Wrong root CID is a structural anchor mismatch — returns Err.
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();

    let other_block = Block::new(create_test_delta("doc2", "age"), vec![], vec![]);
    let wrong_root = other_block.generate_cid().unwrap();

    let proof = MerkleProof::new(cid, wrong_root, vec![node]);
    let result = proof.verify();
    assert!(result.is_err(), "wrong root CID should return Err");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("root CID does not match"));
}

#[tokio::test]
async fn test_proof_missing_link_fails() {
    // Missing chain link is a legitimate cryptographic "no" — returns Ok(false).
    // Create two unrelated blocks; their CIDs become the anchors so the structural
    // checks pass, but the heads chain between them is broken.
    let block1 = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid1 = block1.generate_cid().unwrap();
    let node1 = ProofNode::from_block(&block1).unwrap();

    let block2 = Block::new(create_test_delta("doc2", "name"), vec![], vec![]);
    let cid2 = block2.generate_cid().unwrap();
    let node2 = ProofNode::from_block(&block2).unwrap();

    let proof = MerkleProof::new(cid1, cid2, vec![node1, node2]);
    assert!(
        !proof.verify().unwrap(),
        "broken chain link should return Ok(false)"
    );
}

#[tokio::test]
async fn test_empty_proof_fails() {
    // Empty path is a structural input error — returns Err.
    let proof = MerkleProof::new(Cid::default(), Cid::default(), vec![]);
    let result = proof.verify();
    assert!(result.is_err(), "empty proof should return Err");
    assert!(result.unwrap_err().to_string().contains("empty"));
}

#[tokio::test]
async fn test_extract_proof_same_cid() {
    let blockstore = MemoryProofBlockstore::new();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let data = block.to_dag_cbor().unwrap();

    blockstore.put(cid, data).await;

    let proof = extract_proof(&blockstore, cid, cid).await.unwrap().unwrap();
    assert_eq!(proof.len(), 1);
    assert!(proof.verify().unwrap());
}

#[tokio::test]
async fn test_extract_proof_chain() {
    let blockstore = MemoryProofBlockstore::new();

    // Create a chain: root <- block1 <- block2 (leaf)
    let root = Block::new(create_test_delta("doc1", "v1"), vec![], vec![]);
    let root_cid = root.generate_cid().unwrap();
    blockstore.put(root_cid, root.to_dag_cbor().unwrap()).await;

    let block1 = Block::new(create_test_delta("doc1", "v2"), vec![root_cid], vec![]);
    let cid1 = block1.generate_cid().unwrap();
    blockstore.put(cid1, block1.to_dag_cbor().unwrap()).await;

    let block2 = Block::new(create_test_delta("doc1", "v3"), vec![cid1], vec![]);
    let cid2 = block2.generate_cid().unwrap();
    blockstore.put(cid2, block2.to_dag_cbor().unwrap()).await;

    // Extract proof from leaf (block2) to root
    let proof = extract_proof(&blockstore, cid2, root_cid)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(proof.len(), 3);
    assert_eq!(proof.leaf_cid, cid2);
    assert_eq!(proof.root_cid, root_cid);
    assert!(proof.verify().unwrap());
}

#[tokio::test]
async fn test_extract_proof_no_path() {
    let blockstore = MemoryProofBlockstore::new();

    // Create two unrelated blocks
    let block1 = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid1 = block1.generate_cid().unwrap();
    blockstore.put(cid1, block1.to_dag_cbor().unwrap()).await;

    let block2 = Block::new(create_test_delta("doc2", "name"), vec![], vec![]);
    let cid2 = block2.generate_cid().unwrap();
    blockstore.put(cid2, block2.to_dag_cbor().unwrap()).await;

    // Try to extract proof between unrelated blocks
    let result = extract_proof(&blockstore, cid1, cid2).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_proof_dag_cbor_roundtrip() {
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();

    let proof = MerkleProof::new(cid, cid, vec![node]);

    let bytes = proof.to_dag_cbor().unwrap();
    let restored = MerkleProof::from_dag_cbor(&bytes).unwrap();

    assert_eq!(proof, restored);
}

#[tokio::test]
async fn test_signed_proof_ed25519() {
    let private_key = generate_ed25519().unwrap();
    let public_key = private_key.public_key();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    // Verify with explicit public key
    assert!(signed.verify(public_key.as_ref()).unwrap());

    // Verify with embedded key
    assert!(signed.verify_with_embedded_key().unwrap());
}

#[tokio::test]
async fn test_signed_proof_secp256k1() {
    let private_key = generate_secp256k1().unwrap();
    let public_key = private_key.public_key();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    // Verify with explicit public key
    assert!(signed.verify(public_key.as_ref()).unwrap());

    // Verify with embedded key
    assert!(signed.verify_with_embedded_key().unwrap());
}

#[tokio::test]
async fn test_signed_proof_wrong_key_fails() {
    // Identity mismatch — the caller provided a key that isn't the signer.
    // This is a configuration error; return Err to match Go's convention.
    let private_key1 = generate_ed25519().unwrap();
    let private_key2 = generate_ed25519().unwrap();
    let wrong_public_key = private_key2.public_key();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let signed = SignedMerkleProof::sign(proof, &private_key1 as &dyn PrivateKey).unwrap();

    let result = signed.verify(wrong_public_key.as_ref());
    assert!(
        result.is_err(),
        "wrong key should return Err, not Ok(false)"
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("identity does not match"));
}

#[tokio::test]
async fn test_signed_proof_tampered_proof_fails() {
    // Signature verification failure — the signed bytes changed, so the signature
    // no longer matches. Cryptographic security event; return Err.
    let private_key = generate_ed25519().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let mut signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    let other_block = Block::new(create_test_delta("doc2", "other"), vec![], vec![]);
    let other_cid = other_block.generate_cid().unwrap();
    signed.proof.root_cid = other_cid;

    let result = signed.verify_with_embedded_key();
    assert!(
        result.is_err(),
        "tampered proof should return Err, not Ok(false)"
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("signature verification failed"));
}

#[tokio::test]
async fn test_signed_proof_dag_cbor_roundtrip() {
    let private_key = generate_ed25519().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    let bytes = signed.to_dag_cbor().unwrap();
    let restored = SignedMerkleProof::from_dag_cbor(&bytes).unwrap();

    assert_eq!(signed, restored);
    assert!(restored.verify_with_embedded_key().unwrap());
}

#[tokio::test]
async fn test_three_block_chain_proof() {
    // More comprehensive chain test
    let block0 = Block::new(create_test_delta("doc1", "genesis"), vec![], vec![]);
    let cid0 = block0.generate_cid().unwrap();
    let node0 = ProofNode::from_block(&block0).unwrap();

    let block1 = Block::new(create_test_delta("doc1", "update1"), vec![cid0], vec![]);
    let cid1 = block1.generate_cid().unwrap();
    let node1 = ProofNode::from_block(&block1).unwrap();

    let block2 = Block::new(create_test_delta("doc1", "update2"), vec![cid1], vec![]);
    let cid2 = block2.generate_cid().unwrap();
    let node2 = ProofNode::from_block(&block2).unwrap();

    // Proof from block2 (leaf) to block0 (root)
    let proof = MerkleProof::new(cid2, cid0, vec![node2, node1, node0]);
    assert!(proof.verify().unwrap());

    // Partial proof: block2 to block1
    let partial_proof = MerkleProof::new(
        cid2,
        cid1,
        vec![
            ProofNode::from_block(&block2).unwrap(),
            ProofNode::from_block(&block1).unwrap(),
        ],
    );
    assert!(partial_proof.verify().unwrap());
}

#[tokio::test]
async fn test_verify_standalone_functions() {
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    // Test standalone verify_proof
    assert!(verify_proof(&proof).unwrap());
}

#[tokio::test]
async fn test_verify_signed_standalone() {
    let private_key = generate_ed25519().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    // Test standalone verify_signed_proof
    assert!(verify_signed_proof(&signed).unwrap());
}

#[tokio::test]
async fn test_key_type_mismatch_returns_error() {
    // Sign with Ed25519
    let ed25519_key = generate_ed25519().unwrap();
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);
    let signed = SignedMerkleProof::sign(proof, &ed25519_key as &dyn PrivateKey).unwrap();

    // Try to verify with secp256k1 key - should return error, not false
    let secp_key = generate_secp256k1().unwrap();
    let secp_public = secp_key.public_key();
    let result = signed.verify(secp_public.as_ref());

    assert!(result.is_err(), "Key type mismatch should return error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Key type mismatch"),
        "Error should mention key type mismatch: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_extract_proof_branching_dag() {
    // Test branching DAG where a merge block points to multiple parents
    let blockstore = MemoryProofBlockstore::new();

    // Create root block
    let root = Block::new(create_test_delta("doc1", "v1"), vec![], vec![]);
    let root_cid = root.generate_cid().unwrap();
    blockstore.put(root_cid, root.to_dag_cbor().unwrap()).await;

    // Create two branches from root
    let b1 = Block::new(create_test_delta("doc1", "branch1"), vec![root_cid], vec![]);
    let b1_cid = b1.generate_cid().unwrap();
    blockstore.put(b1_cid, b1.to_dag_cbor().unwrap()).await;

    let b2 = Block::new(create_test_delta("doc1", "branch2"), vec![root_cid], vec![]);
    let b2_cid = b2.generate_cid().unwrap();
    blockstore.put(b2_cid, b2.to_dag_cbor().unwrap()).await;

    // Create merge block pointing to both branches
    let merge = Block::new(
        create_test_delta("doc1", "merge"),
        vec![b1_cid, b2_cid],
        vec![],
    );
    let merge_cid = merge.generate_cid().unwrap();
    blockstore
        .put(merge_cid, merge.to_dag_cbor().unwrap())
        .await;

    // Extract proof from merge to root - should find one of the paths
    let proof = extract_proof(&blockstore, merge_cid, root_cid)
        .await
        .unwrap()
        .unwrap();

    // BFS finds shortest path, so length should be 3 (merge -> b1 or b2 -> root)
    assert_eq!(proof.len(), 3);
    assert_eq!(proof.leaf_cid, merge_cid);
    assert_eq!(proof.root_cid, root_cid);
    assert!(proof.verify().unwrap());

    // The middle block should be either b1 or b2
    let middle_cid = proof.path[1].cid;
    assert!(
        middle_cid == b1_cid || middle_cid == b2_cid,
        "Middle block should be one of the branches"
    );
}

#[tokio::test]
async fn test_extract_proof_missing_parent_returns_error() {
    // Test that referencing a non-existent parent block returns an error
    let blockstore = MemoryProofBlockstore::new();

    // Create a block that references a non-existent parent
    let fake_parent_cid = {
        let fake = Block::new(create_test_delta("doc1", "fake"), vec![], vec![]);
        fake.generate_cid().unwrap()
    };

    let leaf = Block::new(
        create_test_delta("doc1", "leaf"),
        vec![fake_parent_cid],
        vec![],
    );
    let leaf_cid = leaf.generate_cid().unwrap();
    blockstore.put(leaf_cid, leaf.to_dag_cbor().unwrap()).await;

    // Create a root that exists but is unrelated
    let root = Block::new(create_test_delta("doc1", "root"), vec![], vec![]);
    let root_cid = root.generate_cid().unwrap();
    blockstore.put(root_cid, root.to_dag_cbor().unwrap()).await;

    // Attempting to extract proof should return error about missing parent
    let result = extract_proof(&blockstore, leaf_cid, root_cid).await;
    assert!(result.is_err(), "Should return error for missing parent");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Missing parent block"),
        "Error should mention missing parent: {}",
        err_msg
    );
}

#[test]
fn test_verify_cid_unsupported_hash_algorithm() {
    use multihash::MultihashGeneric;

    // Create a valid block
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let data = block.to_dag_cbor().unwrap();

    // Create a CID with unsupported hash algorithm (Blake2b-256 = 0xb220)
    const BLAKE2B_256_CODE: u64 = 0xb220;
    const DAG_CBOR_CODEC: u64 = 0x71;

    // Create a fake hash (all zeros) with Blake2b code
    let fake_digest = [0u8; 32];
    let mh = MultihashGeneric::<64>::wrap(BLAKE2B_256_CODE, &fake_digest).unwrap();
    let unsupported_cid = Cid::new_v1(DAG_CBOR_CODEC, mh);

    let node = ProofNode {
        cid: unsupported_cid,
        data,
    };

    let result = node.verify_cid();
    assert!(result.is_err(), "Should return error for unsupported hash");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Unsupported hash algorithm"),
        "Error should mention unsupported hash: {}",
        err_msg
    );
}

#[test]
fn test_verify_cid_unsupported_codec() {
    use multihash::MultihashGeneric;

    // Create a valid block
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let data = block.to_dag_cbor().unwrap();

    // Create a CID with unsupported codec (raw = 0x55)
    const SHA2_256_CODE: u64 = 0x12;
    const RAW_CODEC: u64 = 0x55;

    let fake_digest = [0u8; 32];
    let mh = MultihashGeneric::<64>::wrap(SHA2_256_CODE, &fake_digest).unwrap();
    let unsupported_cid = Cid::new_v1(RAW_CODEC, mh);

    let node = ProofNode {
        cid: unsupported_cid,
        data,
    };

    let result = node.verify_cid();
    assert!(result.is_err(), "Should return error for unsupported codec");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Unsupported codec"),
        "Error should mention unsupported codec: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_signed_proof_corrupted_signature_fails() {
    // Corrupted signature bytes — cryptographic verification failure, return Err.
    let private_key = generate_ed25519().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let mut signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    if !signed.signature.value.is_empty() {
        signed.signature.value[0] ^= 0xFF;
    }

    let result = signed.verify_with_embedded_key();
    assert!(
        result.is_err(),
        "corrupted signature should return Err, not Ok(false)"
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("signature verification failed"));
}

#[tokio::test]
async fn test_verify_with_embedded_key_invalid_utf8_identity_fails() {
    let private_key = generate_ed25519().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let mut signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    // Replace identity with invalid UTF-8 bytes
    signed.signature.header.identity = vec![0xFF, 0xFE, 0x00, 0x01];

    let result = signed.verify_with_embedded_key();
    assert!(
        result.is_err(),
        "Invalid UTF-8 identity should return error"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Invalid identity encoding"),
        "Error should mention invalid identity encoding: {}",
        err_msg
    );
}

#[test]
fn test_proof_node_decode_block_invalid_cbor_fails() {
    let node = ProofNode {
        cid: Cid::default(),
        data: vec![0xFF, 0xFF, 0xFF], // Invalid CBOR
    };

    let result = node.decode_block();
    assert!(result.is_err(), "Invalid CBOR should return error");
}

#[test]
fn test_merkle_proof_from_dag_cbor_invalid_bytes_fails() {
    let result = MerkleProof::from_dag_cbor(&[0xFF, 0xFF, 0xFF]);
    assert!(result.is_err(), "Invalid DAG-CBOR should return error");
}

#[test]
fn test_proof_wrong_leaf_cid_fails() {
    // Wrong leaf CID is a structural anchor mismatch — returns Err.
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();

    let other_block = Block::new(create_test_delta("doc2", "age"), vec![], vec![]);
    let wrong_leaf = other_block.generate_cid().unwrap();

    let proof = MerkleProof::new(wrong_leaf, cid, vec![node]);
    let result = proof.verify();
    assert!(result.is_err(), "wrong leaf CID should return Err");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("leaf CID does not match"));
}

#[tokio::test]
async fn test_extract_proof_leaf_not_found_returns_error() {
    let blockstore = MemoryProofBlockstore::new();

    // Create a CID without storing the block
    let fake_leaf = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let fake_leaf_cid = fake_leaf.generate_cid().unwrap();

    // Create and store a different block as "root"
    let root = Block::new(create_test_delta("doc2", "root"), vec![], vec![]);
    let root_cid = root.generate_cid().unwrap();
    blockstore.put(root_cid, root.to_dag_cbor().unwrap()).await;

    // Attempt to extract proof with non-existent leaf
    let result = extract_proof(&blockstore, fake_leaf_cid, root_cid).await;
    assert!(
        result.is_err(),
        "Should return error for missing leaf block"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Leaf block not found"),
        "Error should mention missing leaf: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_verify_with_embedded_key_invalid_hex_identity_fails() {
    let private_key = generate_ed25519().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let mut signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    // Replace identity with valid UTF-8 but invalid hex
    signed.signature.header.identity = b"not-valid-hex-string".to_vec();

    let result = signed.verify_with_embedded_key();
    assert!(result.is_err(), "Invalid hex identity should return error");
}

#[test]
fn test_merkle_proof_from_dag_cbor_wrong_schema_fails() {
    use serde::Serialize;

    #[derive(Serialize)]
    struct WrongSchema {
        foo: String,
        bar: i32,
    }

    let wrong_data = WrongSchema {
        foo: "test".into(),
        bar: 42,
    };
    let valid_cbor = serde_ipld_dagcbor::to_vec(&wrong_data).unwrap();

    let result = MerkleProof::from_dag_cbor(&valid_cbor);
    assert!(
        result.is_err(),
        "Wrong schema should return deserialization error"
    );
}

// =========================================================================
// Go parity tests for verification semantics
//
// These tests lock in the Rust behavior to match Go's conventions:
// - Cryptographic signature failures return Err (match Go's ErrSignatureVerification)
// - Caller-supplied anchor mismatches return Err (configuration error)
// - Hash/chain mismatches during verification return Ok(false) (legitimate "no")
// - No hardcoded path length limit (Go has none)
// =========================================================================

/// Build a chain of `count` blocks where each block's heads points to the previous block.
/// Returns (blocks, cids) in leaf-to-root order (leaf at index 0, root at last index).
fn build_linear_chain(count: usize) -> (Vec<Block>, Vec<Cid>) {
    assert!(count > 0, "chain must have at least one block");
    let mut blocks = Vec::with_capacity(count);
    let mut cids = Vec::with_capacity(count);

    // Root (index count-1) is the oldest block — no heads.
    // Each successive block (going toward the leaf at index 0) points to the previous CID
    // via its heads field. We build from root to leaf.
    let mut chain = Vec::with_capacity(count);
    let root = Block::new(create_test_delta("doc-chain", "f0"), vec![], vec![]);
    let mut prev_cid = root.generate_cid().unwrap();
    chain.push((root, prev_cid));

    for i in 1..count {
        let field = format!("f{}", i);
        let block = Block::new(
            create_test_delta("doc-chain", &field),
            vec![prev_cid],
            vec![],
        );
        let cid = block.generate_cid().unwrap();
        chain.push((block, cid));
        prev_cid = cid;
    }

    // chain is now [root, ..., leaf]. Reverse to get [leaf, ..., root] (proof path order).
    chain.reverse();
    for (b, c) in chain {
        blocks.push(b);
        cids.push(c);
    }
    (blocks, cids)
}

#[tokio::test]
async fn test_large_chain_proof_verifies_without_limit() {
    // Regression for #732: MAX_PROOF_PATH_LENGTH used to reject proofs > 1000 nodes.
    // Go has no such limit. A legitimate 1500-node chain must verify successfully.
    let count = 1500;
    let (blocks, cids) = build_linear_chain(count);

    let path: Vec<ProofNode> = blocks
        .iter()
        .map(|b| ProofNode::from_block(b).unwrap())
        .collect();

    let leaf_cid = cids[0];
    let root_cid = cids[count - 1];
    let proof = MerkleProof::new(leaf_cid, root_cid, path);

    assert!(
        proof.verify().unwrap(),
        "1500-node chain proof should verify without hitting any hardcoded limit"
    );
}

#[tokio::test]
async fn test_verify_err_vs_ok_false_semantics() {
    // Parity test documenting the Err vs Ok(false) contract:
    //
    // - Empty path, wrong leaf anchor, wrong root anchor: Err (caller mistake)
    // - Content hash mismatch, broken chain link: Ok(false) (cryptographic "no")

    // 1. Empty path → Err
    let empty = MerkleProof::new(Cid::default(), Cid::default(), vec![]);
    assert!(empty.verify().is_err());

    // 2. Wrong leaf anchor → Err
    let block_a = Block::new(create_test_delta("doc", "a"), vec![], vec![]);
    let cid_a = block_a.generate_cid().unwrap();
    let node_a = ProofNode::from_block(&block_a).unwrap();
    let block_b = Block::new(create_test_delta("doc", "b"), vec![], vec![]);
    let cid_b = block_b.generate_cid().unwrap();
    let wrong_leaf = MerkleProof::new(cid_b, cid_a, vec![node_a.clone()]);
    assert!(wrong_leaf.verify().is_err());

    // 3. Wrong root anchor → Err
    let wrong_root = MerkleProof::new(cid_a, cid_b, vec![node_a.clone()]);
    assert!(wrong_root.verify().is_err());

    // 4. Broken chain link → Ok(false) (cids match the path, but block_a.heads doesn't contain cid_b)
    let node_b = ProofNode::from_block(&block_b).unwrap();
    let broken = MerkleProof::new(cid_a, cid_b, vec![node_a, node_b]);
    let result = broken.verify();
    assert!(result.is_ok(), "broken chain link should be Ok, not Err");
    assert!(!result.unwrap(), "broken chain link should return false");
}

#[tokio::test]
async fn test_signed_proof_signature_failure_returns_err() {
    // Parity test: cryptographic signature verification failure must return Err,
    // matching Go's verifySignature returning ErrSignatureVerification.
    let private_key = generate_ed25519().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let mut signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    // Flip all bytes in the signature
    for byte in signed.signature.value.iter_mut() {
        *byte = !*byte;
    }

    // Direct verify() path
    let pub_key = private_key.public_key();
    let result_verify = signed.verify(pub_key.as_ref());
    assert!(
        result_verify.is_err(),
        "verify() must return Err on signature failure, got {:?}",
        result_verify
    );

    // verify_with_embedded_key() path
    let result_embedded = signed.verify_with_embedded_key();
    assert!(
        result_embedded.is_err(),
        "verify_with_embedded_key() must return Err on signature failure, got {:?}",
        result_embedded
    );

    // Standalone verify_signed_proof helper path
    let result_helper = verify_signed_proof(&signed);
    assert!(
        result_helper.is_err(),
        "verify_signed_proof() must return Err on signature failure, got {:?}",
        result_helper
    );
}

#[tokio::test]
async fn test_signed_proof_identity_mismatch_returns_err() {
    // Parity test: identity mismatch is a caller error (wrong key provided),
    // must return Err. This distinguishes "I don't have the right key" from
    // "the signature is cryptographically invalid".
    let signer_key = generate_ed25519().unwrap();
    let other_key = generate_secp256k1().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let signed = SignedMerkleProof::sign(proof, &signer_key as &dyn PrivateKey).unwrap();

    // Using a key of a different curve type — hits the key_type check first.
    let other_pub = other_key.public_key();
    let result_type = signed.verify(other_pub.as_ref());
    assert!(result_type.is_err(), "key type mismatch should return Err");

    // Using a different ed25519 key — hits the identity check.
    let wrong_ed = generate_ed25519().unwrap();
    let wrong_pub = wrong_ed.public_key();
    let result_identity = signed.verify(wrong_pub.as_ref());
    assert!(
        result_identity.is_err(),
        "identity mismatch should return Err, got {:?}",
        result_identity
    );
    assert!(result_identity
        .unwrap_err()
        .to_string()
        .contains("identity does not match"));
}

#[tokio::test]
async fn test_valid_signed_proof_still_returns_ok_true() {
    // Sanity check that the happy path still returns Ok(true) — we haven't
    // accidentally made valid proofs fail after tightening the error semantics.
    let private_key = generate_ed25519().unwrap();

    let block0 = Block::new(create_test_delta("doc", "g"), vec![], vec![]);
    let cid0 = block0.generate_cid().unwrap();
    let node0 = ProofNode::from_block(&block0).unwrap();

    let block1 = Block::new(create_test_delta("doc", "u1"), vec![cid0], vec![]);
    let cid1 = block1.generate_cid().unwrap();
    let node1 = ProofNode::from_block(&block1).unwrap();

    let proof = MerkleProof::new(cid1, cid0, vec![node1, node0]);
    let signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    let pub_key = private_key.public_key();
    assert!(signed.verify(pub_key.as_ref()).unwrap());
    assert!(signed.verify_with_embedded_key().unwrap());
}
