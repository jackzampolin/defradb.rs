//! Integration tests for merkle proof generation and verification

use cid::Cid;
use crypto::merkle_proof::{
    extract_proof, verify_proof, verify_signed_proof, MerkleProof, ProofBlockstore, ProofNode,
    SignedMerkleProof,
};
use crypto::keys::generation::{generate_ed25519, generate_secp256k1};
use crypto::keys::PrivateKey;
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
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();

    // Create a different block for wrong root CID
    let other_block = Block::new(create_test_delta("doc2", "age"), vec![], vec![]);
    let wrong_root = other_block.generate_cid().unwrap();

    let proof = MerkleProof::new(cid, wrong_root, vec![node]);
    assert!(!proof.verify().unwrap());
}

#[tokio::test]
async fn test_proof_missing_link_fails() {
    // Create two unrelated blocks
    let block1 = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid1 = block1.generate_cid().unwrap();
    let node1 = ProofNode::from_block(&block1).unwrap();

    let block2 = Block::new(create_test_delta("doc2", "name"), vec![], vec![]);
    let cid2 = block2.generate_cid().unwrap();
    let node2 = ProofNode::from_block(&block2).unwrap();

    // Try to create proof between unrelated blocks
    let proof = MerkleProof::new(cid1, cid2, vec![node1, node2]);
    assert!(!proof.verify().unwrap());
}

#[tokio::test]
async fn test_empty_proof_fails() {
    let proof = MerkleProof::new(Cid::default(), Cid::default(), vec![]);
    assert!(!proof.verify().unwrap());
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
    let private_key1 = generate_ed25519().unwrap();
    let private_key2 = generate_ed25519().unwrap();
    let wrong_public_key = private_key2.public_key();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let signed = SignedMerkleProof::sign(proof, &private_key1 as &dyn PrivateKey).unwrap();

    // Verification with wrong key should fail
    assert!(!signed.verify(wrong_public_key.as_ref()).unwrap());
}

#[tokio::test]
async fn test_signed_proof_tampered_proof_fails() {
    let private_key = generate_ed25519().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let mut signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    // Tamper with the proof
    let other_block = Block::new(create_test_delta("doc2", "other"), vec![], vec![]);
    let other_cid = other_block.generate_cid().unwrap();
    signed.proof.root_cid = other_cid;

    // Verification should fail due to signature mismatch
    assert!(!signed.verify_with_embedded_key().unwrap());
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
    let private_key = generate_ed25519().unwrap();

    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();
    let proof = MerkleProof::new(cid, cid, vec![node]);

    let mut signed = SignedMerkleProof::sign(proof, &private_key as &dyn PrivateKey).unwrap();

    // Corrupt the signature value by flipping bits
    if !signed.signature.value.is_empty() {
        signed.signature.value[0] ^= 0xFF;
    }

    // Verification should fail (return false, not error)
    assert!(
        !signed.verify_with_embedded_key().unwrap(),
        "Corrupted signature should fail verification"
    );
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
    let block = Block::new(create_test_delta("doc1", "name"), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    let node = ProofNode::from_block(&block).unwrap();

    // Create a different block for wrong leaf CID
    let other_block = Block::new(create_test_delta("doc2", "age"), vec![], vec![]);
    let wrong_leaf = other_block.generate_cid().unwrap();

    // Wrong leaf CID doesn't match path[0].cid
    let proof = MerkleProof::new(wrong_leaf, cid, vec![node]);
    assert!(
        !proof.verify().unwrap(),
        "Proof with wrong leaf CID should fail"
    );
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
