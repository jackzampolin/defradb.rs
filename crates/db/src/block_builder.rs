//! IPLD block builder for document mutations.
//!
//! Creates DAG-CBOR encoded blocks for P2P synchronization.

use cid::Cid;
use document::Document;
use multihash::MultihashGeneric;
use sha2::{Digest, Sha256};

/// DAG-CBOR codec identifier (multicodec 0x71)
const DAG_CBOR_CODEC: u64 = 0x71;

/// SHA2-256 multihash code
const SHA2_256_CODE: u64 = 0x12;

/// Result of building a block from a document mutation.
#[derive(Debug, Clone)]
pub struct BlockResult {
    /// The CID of the created block
    pub cid: Cid,
    /// The raw block bytes (DAG-CBOR encoded)
    pub block: Vec<u8>,
    /// The document ID
    pub doc_id: String,
}

/// Build an IPLD block from a document.
///
/// The block contains the document data encoded as CBOR.
/// The CID is generated using DAG-CBOR codec and SHA2-256 hash.
///
/// Note: This creates a simple document block, not a full CRDT block
/// with delta payloads. For full CRDT blocks, use defra-core's Block type.
pub fn build_block_from_document(doc: &Document) -> Result<BlockResult, String> {
    let doc_id = doc
        .id()
        .ok_or_else(|| "Document must have an ID".to_string())?
        .to_string();

    // Encode document as CBOR
    let block = doc
        .to_cbor()
        .map_err(|e| format!("Failed to encode document: {}", e))?;

    // Create CID from block
    let cid = generate_cid_from_bytes(&block)?;

    Ok(BlockResult { cid, block, doc_id })
}

/// Generate CID from raw bytes using DAG-CBOR codec and SHA2-256.
fn generate_cid_from_bytes(bytes: &[u8]) -> Result<Cid, String> {
    // Hash with SHA2-256
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();

    // Create multihash
    let mh = MultihashGeneric::<64>::wrap(SHA2_256_CODE, &digest)
        .map_err(|e| format!("Failed to create multihash: {}", e))?;

    // Create CIDv1 with DAG-CBOR codec
    Ok(Cid::new_v1(DAG_CBOR_CODEC, mh))
}

#[cfg(test)]
mod tests {
    use super::*;
    use document::NormalValue;

    #[test]
    fn test_build_block_from_document() {
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));

        let result = build_block_from_document(&doc).unwrap();

        assert!(!result.doc_id.is_empty());
        assert!(!result.block.is_empty());
        // CID should be valid
        assert_eq!(result.cid.codec(), DAG_CBOR_CODEC);
    }

    #[test]
    fn test_build_block_requires_doc_id() {
        let doc = Document::new();
        let result = build_block_from_document(&doc);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must have an ID"));
    }

    #[test]
    fn test_deterministic_cid() {
        // Same document content should produce same CID
        let mut doc1 = Document::new();
        doc1.generate_and_set_doc_id().unwrap();
        doc1.set("name", NormalValue::String("Bob".to_string()));

        let mut doc2 = Document::new();
        doc2.set_id(doc1.id().unwrap().clone());
        doc2.set("name", NormalValue::String("Bob".to_string()));

        let result1 = build_block_from_document(&doc1).unwrap();
        let result2 = build_block_from_document(&doc2).unwrap();

        assert_eq!(result1.cid, result2.cid);
    }
}
