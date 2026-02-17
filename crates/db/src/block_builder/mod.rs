//! IPLD block builder for document mutations.
//!
//! Creates proper Block structures with CRDT delta payloads for P2P synchronization.
//! Matches Go DefraDB's block format for wire compatibility.
//!
//! This module provides two main functions:
//! - `build_blocks_from_document`: For P2P broadcast (uses external blockstore)
//! - `write_document_blocks`: For FFI/local storage (uses transaction stores)

mod build;
mod collection;
mod compute;
mod read;
#[cfg(test)]
mod tests;
mod write;

#[allow(deprecated)]
pub use build::build_block_from_document;
pub use build::build_blocks_from_document;
pub use collection::write_collection_block;
pub use compute::{compute_document_blocks, insert_computed_blocks, ComputedBlocks};
pub use read::read_latest_composite_block;
pub use write::{write_delete_block, write_document_blocks};

use std::collections::HashMap;

use cid::Cid;
use crypto::PrivateKey;
use datastore::NamespaceView;
use defra_core::block::{
    generate_cid_from_bytes, Block, CollectionDeltaPayload, CompositeDeltaPayload,
    CounterDeltaPayload, CrdtDelta, DAGLink, Encryption, LwwDeltaPayload, Signature,
    SignatureHeader, SignatureType,
};
use defra_core::encryption::EncryptionConfig;
use defra_core::signing::SigningConfig;
use document::{Document, NormalValue};
use storage::corekv::Key;
use storage::keys::headstore::{HeadstoreColKey, HeadstoreDocKey};

pub(super) fn encrypt_delta(plaintext: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, String> {
    let (ciphertext, _nonce) = crypto::encryption::aes::encrypt_aes(plaintext, key, &[], true)
        .map_err(|e| format!("encryption failed: {}", e))?;
    Ok(ciphertext)
}

/// Compute a signature block without writing to storage.
///
/// Pure function: returns `(sig_cid, sig_cbor_bytes)` for the caller to store.
/// Returns `None` for field blocks with priority > 1 (not signed per Go behavior).
pub(super) fn compute_signature(
    block: &Block,
    signer: &SigningConfig,
) -> Result<Option<(Cid, Vec<u8>)>, String> {
    // Only sign first field blocks (priority <= 1) and composite blocks.
    // Higher-priority field blocks are not signed — their integrity is
    // guaranteed by the signature on the parent composite block.
    let is_field = matches!(&block.delta, CrdtDelta::Lww(_) | CrdtDelta::Counter(_));
    if is_field && block.delta.priority() > 1 {
        return Ok(None);
    }

    // Serialize the block (without signature) to get the bytes to sign
    let block_bytes = block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode block for signing: {}", e))?;

    // Determine signature type and sign
    let (sig_type, sig_bytes) = match signer.key_type.as_str() {
        "ed25519" => {
            let private_key = crypto::Ed25519PrivateKey::from_bytes(&signer.private_key_bytes)
                .map_err(|e| format!("Failed to load Ed25519 private key: {}", e))?;
            let sig = private_key
                .sign(&block_bytes)
                .map_err(|e| format!("Failed to sign block: {}", e))?;
            (SignatureType::EdDSA, sig)
        }
        "secp256k1" => {
            let private_key = crypto::Secp256k1PrivateKey::from_bytes(&signer.private_key_bytes)
                .map_err(|e| format!("Failed to load secp256k1 private key: {}", e))?;
            let sig = private_key
                .sign(&block_bytes)
                .map_err(|e| format!("Failed to sign block: {}", e))?;
            (SignatureType::ES256K, sig)
        }
        other => return Err(format!("Unsupported key type for signing: {}", other)),
    };

    // Create signature block.
    // Go uses `[]byte(fullIdent.PublicKey().String())` for identity,
    // which is the hex-encoded public key string as bytes.
    let signature = Signature::new(
        SignatureHeader::new(sig_type, signer.public_key_hex.as_bytes().to_vec()),
        sig_bytes,
    );

    let sig_cbor = signature
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode signature block: {}", e))?;
    let sig_cid = generate_cid_from_bytes(&sig_cbor)
        .map_err(|e| format!("Failed to generate signature CID: {}", e))?;

    Ok(Some((sig_cid, sig_cbor)))
}

/// Sign a block and store the signature as a separate IPLD block.
///
/// Delegates to `compute_signature()` for the pure computation, then writes
/// the signature block to blockstore. The caller must then set
/// `block.signature = Some(sig_cid)` and re-serialize.
pub(super) async fn sign_block(
    block: &Block,
    signer: &SigningConfig,
    blockstore: &NamespaceView,
) -> Result<Option<Cid>, String> {
    let Some((sig_cid, sig_cbor)) = compute_signature(block, signer)? else {
        return Ok(None);
    };

    blockstore
        .set(&sig_cid.to_bytes(), &sig_cbor)
        .await
        .map_err(|e| format!("Failed to store signature block: {}", e))?;

    Ok(Some(sig_cid))
}

/// Result of building blocks from a document mutation.
#[derive(Debug, Clone)]
pub struct BlockResult {
    /// The CID of the composite (root) block
    pub cid: Cid,
    /// The raw composite block bytes (DAG-CBOR encoded)
    pub block: Vec<u8>,
    /// The document ID
    pub doc_id: String,
    /// CIDs of all field blocks created
    pub field_cids: Vec<Cid>,
}

/// Encode a NormalValue as CBOR bytes.
pub(super) fn encode_value_as_cbor(value: &NormalValue) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes)
        .map_err(|e| format!("Failed to encode value as CBOR: {}", e))?;
    Ok(bytes)
}

/// Encode a priority as a varint (matching Go's binary.PutUvarint).
pub(super) fn encode_priority_varint(priority: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10); // Max varint64 is 10 bytes
    let mut n = priority;
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
    buf
}

/// Decode a varint to priority (matching Go's binary.Uvarint).
pub(super) fn decode_priority_varint(buf: &[u8]) -> u64 {
    let mut n: u64 = 0;
    let mut shift: u32 = 0;
    for &byte in buf {
        if shift >= 64 {
            return 0; // Overflow protection
        }
        n |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return n;
        }
        shift += 7;
    }
    n
}

/// A single head entry for a document field.
#[derive(Clone)]
pub(super) struct FieldHeadEntry {
    /// The CID of the head
    pub(super) cid: Cid,
    /// The full key (for deletion when replacing)
    pub(super) key: Vec<u8>,
}

/// Get all existing heads for a specific field of a document.
///
/// During concurrent P2P updates, a field can have multiple heads (branches).
/// Returns all current head CIDs for the field, sorted by CID string
/// representation to match Go's deterministic head ordering.
pub(super) async fn get_all_field_heads(
    headstore: &NamespaceView,
    doc_id: &str,
    field_id: &str,
) -> Result<Vec<FieldHeadEntry>, String> {
    use storage::corekv::IterOptions;

    let prefix = HeadstoreDocKey::field_prefix(doc_id, field_id);
    let opts = IterOptions::new().with_prefix(prefix);

    let mut iter = headstore
        .iterator(opts)
        .await
        .map_err(|e| format!("Failed to create headstore iterator: {}", e))?;

    let mut entries = Vec::new();
    while let Some(kv_pair) = iter
        .next()
        .await
        .map_err(|e| format!("Failed to iterate headstore: {}", e))?
    {
        // Parse CID from key: /d/{doc_id}/{field_id}/{cid}
        let key_str = String::from_utf8_lossy(&kv_pair.key);
        let parts: Vec<&str> = key_str.split('/').collect();
        if let Some(cid_str) = parts.last() {
            if let Ok(cid) = cid_str.parse::<Cid>() {
                entries.push(FieldHeadEntry {
                    cid,
                    key: kv_pair.key.clone(),
                });
            }
        }
    }

    // Sort by CID string representation to match Go's Block.New() sorting
    entries.sort_by(|a, b| a.cid.to_string().cmp(&b.cid.to_string()));
    Ok(entries)
}

/// Snapshot of all head entries for a single document, loaded in one scan.
///
/// Replaces multiple overlapping headstore scans with a single prefix scan
/// of `/d/{doc_id}/`.
pub(super) struct DocHeadsSnapshot {
    max_priority: u64,
    entries_by_field: HashMap<String, Vec<FieldHeadEntry>>,
}

impl DocHeadsSnapshot {
    /// Load all heads for a document in a single headstore scan.
    pub async fn load(headstore: &NamespaceView, doc_id: &str) -> Result<Self, String> {
        use storage::corekv::IterOptions;

        let prefix = HeadstoreDocKey::document_prefix(doc_id);
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = headstore
            .iterator(opts)
            .await
            .map_err(|e| format!("Failed to create headstore iterator: {}", e))?;

        let mut max_priority: u64 = 0;
        let mut entries_by_field: HashMap<String, Vec<FieldHeadEntry>> = HashMap::new();

        while let Some(kv_pair) = iter
            .next()
            .await
            .map_err(|e| format!("Failed to iterate headstore: {}", e))?
        {
            let priority = decode_priority_varint(&kv_pair.value);
            if priority > max_priority {
                max_priority = priority;
            }

            // Parse key: /d/{doc_id}/{field_id}/{cid}
            let key_str = String::from_utf8_lossy(&kv_pair.key);
            let parts: Vec<&str> = key_str.split('/').collect();
            // parts: ["", "d", doc_id, field_id, cid]
            if parts.len() >= 5 {
                let field_id = parts[3].to_string();
                if let Ok(cid) = parts[4].parse::<Cid>() {
                    entries_by_field
                        .entry(field_id)
                        .or_default()
                        .push(FieldHeadEntry {
                            cid,
                            key: kv_pair.key.clone(),
                        });
                }
            }
        }

        // Sort each field's entries by CID string to match Go's deterministic ordering.
        for entries in entries_by_field.values_mut() {
            entries.sort_by(|a, b| a.cid.to_string().cmp(&b.cid.to_string()));
        }

        Ok(Self {
            max_priority,
            entries_by_field,
        })
    }

    pub fn max_priority(&self) -> u64 {
        self.max_priority
    }

    /// Get heads for a specific field (returns empty vec if none).
    pub fn field_heads(&self, field_id: &str) -> Vec<FieldHeadEntry> {
        self.entries_by_field
            .get(field_id)
            .cloned()
            .unwrap_or_default()
    }
}
