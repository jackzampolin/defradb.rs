//! IPLD block builder for document mutations.
//!
//! Creates proper Block structures with CRDT delta payloads for P2P synchronization.
//! Matches Go DefraDB's block format for wire compatibility.
//!
//! This module provides two main functions:
//! - `build_blocks_from_document`: For P2P broadcast (uses external blockstore)
//! - `write_document_blocks`: For FFI/local storage (uses transaction stores)

mod build;
pub mod collection;
mod compute;
mod write;

pub use build::build_blocks_from_document;
pub use collection::write_collection_block;
pub use compute::{compute_document_blocks, insert_computed_blocks, ComputedBlocks};
pub use write::{write_delete_block, write_document_blocks};

use bytes::Bytes;
use std::collections::HashMap;

use tracing::warn;

use cid::Cid;
use crypto::PrivateKey;
use datastore::NamespaceView;
use defra_core::block::{
    generate_cid_from_bytes, Block, CollectionDeltaPayload, CompositeDeltaPayload,
    CounterDeltaPayload, CrdtDelta, DAGLink, Encryption, LwwDeltaPayload, Signature,
    SignatureHeader,
};
use defra_core::encryption::EncryptionConfig;
use defra_core::signing::SigningConfig;
use document::{DocID, Document, NormalValue};
use storage::corekv::Key;
use storage::keys::doc_id_index::DocRef;
use storage::keys::headstore::{HeadstoreColKey, HeadstoreDocKey, HeadstorePriorityKey};

/// Node-local storage identity of a document.
///
/// Storage keys are built from the doc short ID; the public DocID exists
/// only after the genesis composite block CID is known.
#[derive(Debug, Clone, Copy)]
pub struct DocStorageIdentity {
    pub collection_short_id: u32,
    pub doc_short_id: u64,
}

impl DocStorageIdentity {
    pub fn new(collection_short_id: u32, doc_short_id: u64) -> Self {
        Self {
            collection_short_id,
            doc_short_id,
        }
    }

    /// Encoded DocRef, used as the encryption key derivation identity
    /// (mirrors Go's `EncodeDocRef` encryptor cache key).
    pub fn doc_ref_bytes(&self) -> Vec<u8> {
        DocRef::new(self.collection_short_id, self.doc_short_id).encode()
    }
}

/// Derive the public DocID string from the genesis composite block CID.
pub fn derive_doc_id(genesis_cid: &Cid) -> String {
    DocID::new_v0(*genesis_cid).to_string()
}

pub(crate) fn encrypt_delta(plaintext: &[u8], key: &[u8; 32]) -> Result<Vec<u8>, String> {
    let (ciphertext, _nonce) = crypto::encryption::aes::encrypt_aes(plaintext, key, &[], true)
        .map_err(|e| format!("encryption failed: {}", e))?;
    Ok(ciphertext)
}

/// Compute a signature block without writing to storage.
///
/// Pure function: returns `(sig_cid, sig_cbor_bytes)` for the caller to store.
/// Returns `None` for field blocks with priority > 1 (not signed per Go behavior).
pub fn compute_signature(
    block: &Block,
    signer: &SigningConfig,
) -> Result<Option<(Cid, Bytes)>, String> {
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

    let sig_type: defra_core::block::SignatureType = signer.key_type.into();

    // A Go peer refuses a signature type it cannot map to a key type, so a block
    // signed with a Rust-only type replicates between Rust nodes and is rejected
    // by Go ones. Emitting one is a deployment decision, so it is refused unless
    // the node has said it accepts a partitioned network.
    if !sig_type.is_go_verifiable()
        && !defra_core::block::go_verifiable_policy::non_go_verifiable_signing_allowed()
    {
        return Err(format!(
            "refusing to sign a block with {sig_type:?} ({}): Go peers cannot verify it and \
             will reject the block during replication. Set {}=1 to allow it on this node.",
            signer.key_type,
            defra_core::block::go_verifiable_policy::ALLOW_ENV,
        ));
    }
    if !sig_type.is_go_verifiable() {
        warn!(
            signature_type = ?sig_type,
            key_type = %signer.key_type,
            "signing a block Go peers cannot verify; it will be rejected when \
             replicating to a Go node"
        );
    }

    let sig_bytes = if let Some(remote) = signer.remote_signer.as_ref() {
        remote.sign_sync(&block_bytes, signer.signing_authorization.as_ref())?
    } else {
        match signer.key_type {
            defra_core::signing::SigningKeyType::Ed25519 => {
                let private_key = crypto::Ed25519PrivateKey::from_bytes(&signer.private_key_bytes)
                    .map_err(|e| format!("Failed to load Ed25519 private key: {}", e))?;
                private_key
                    .sign(&block_bytes)
                    .map_err(|e| format!("Failed to sign block: {}", e))?
            }
            defra_core::signing::SigningKeyType::Secp256k1 => {
                let private_key =
                    crypto::Secp256k1PrivateKey::from_bytes(&signer.private_key_bytes)
                        .map_err(|e| format!("Failed to load secp256k1 private key: {}", e))?;
                private_key
                    .sign(&block_bytes)
                    .map_err(|e| format!("Failed to sign block: {}", e))?
            }
            defra_core::signing::SigningKeyType::Bls => {
                return Err("BLS signing requires a remote signer".to_string());
            }
            defra_core::signing::SigningKeyType::Secp256r1 => {
                return Err(
                    "secp256r1 signing requires a remote signer: a Secure Enclave key cannot be \
                     exported"
                        .to_string(),
                );
            }
            other => {
                return Err(format!("Unsupported key type for signing: {}", other));
            }
        }
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

    Ok(Some((sig_cid, sig_cbor.into())))
}

/// Sign a block and store the signature as a separate IPLD block.
///
/// Delegates to `compute_signature()` for the pure computation, then writes
/// the signature block to blockstore. The caller must then set
/// `block.signature = Some(sig_cid)` and re-serialize.
pub(crate) async fn sign_block(
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
    pub block: Bytes,
    /// The document ID
    pub doc_id: String,
    /// CIDs of all field blocks created
    pub field_cids: Vec<Cid>,
    /// CIDs of all Encryption blocks created (mapped to the owning DocID
    /// alongside composite + field blocks, mirroring Go's save()).
    pub encryption_cids: Vec<Cid>,
}

/// Encode a NormalValue as CBOR bytes.
pub fn encode_value_as_cbor(value: &NormalValue) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes)
        .map_err(|e| format!("Failed to encode value as CBOR: {}", e))?;
    Ok(bytes)
}

/// Encode a priority as a varint (matching Go's binary.PutUvarint).
pub(crate) fn encode_priority_varint(priority: u64) -> Vec<u8> {
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
pub fn decode_priority_varint(buf: &[u8]) -> u64 {
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

pub(crate) fn priority_index_key(doc_short_id: u64, priority: u64, cid: Cid) -> Vec<u8> {
    HeadstorePriorityKey::new(doc_short_id, priority, cid).bytes()
}

/// A single head entry for a document field.
#[derive(Clone)]
pub struct FieldHeadEntry {
    /// The CID of the head
    pub cid: Cid,
    /// The full key (for deletion when replacing)
    pub key: Vec<u8>,
}

/// Get all existing heads for a specific field of a document.
///
/// During concurrent P2P updates, a field can have multiple heads (branches).
/// Returns all current head CIDs for the field, sorted by CID string
/// representation to match Go's deterministic head ordering.
pub async fn get_all_field_heads(
    headstore: &NamespaceView,
    doc_short_id: u64,
    field_id: &str,
) -> Result<Vec<FieldHeadEntry>, String> {
    use storage::corekv::IterOptions;

    let prefix = HeadstoreDocKey::field_prefix(doc_short_id, field_id);
    let prefix_len = prefix.len();
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
        // Key: /d/{doc_short_id}/{field_id}/{cid} — the CID is the suffix
        // after the scanned prefix (the short ID segment is binary, so the
        // key cannot be split on '/').
        let cid_str = String::from_utf8_lossy(&kv_pair.key[prefix_len..]);
        if let Ok(cid) = cid_str.parse::<Cid>() {
            entries.push(FieldHeadEntry {
                cid,
                key: kv_pair.key.clone(),
            });
        }
    }

    // Sort by CID string representation to match Go's Block.New() sorting.
    entries.sort_by_cached_key(|a| a.cid.to_string());
    Ok(entries)
}

/// Snapshot of all head entries for a single document, loaded in one scan.
///
/// Replaces multiple overlapping headstore scans with a single prefix scan
/// of `/d/{doc_id}/`.
pub struct DocHeadsSnapshot {
    max_priority: u64,
    max_priority_by_field: HashMap<String, u64>,
    entries_by_field: HashMap<String, Vec<FieldHeadEntry>>,
}

impl DocHeadsSnapshot {
    /// Load all heads for a document in a single headstore scan.
    pub async fn load(headstore: &NamespaceView, doc_short_id: u64) -> Result<Self, String> {
        use storage::corekv::IterOptions;

        let prefix = HeadstoreDocKey::document_prefix(doc_short_id);
        let prefix_len = prefix.len();
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = headstore
            .iterator(opts)
            .await
            .map_err(|e| format!("Failed to create headstore iterator: {}", e))?;

        let mut max_priority: u64 = 0;
        let mut max_priority_by_field: HashMap<String, u64> = HashMap::new();
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

            // Key: /d/{doc_short_id}/{field_id}/{cid} — parse the suffix
            // after the scanned prefix (the short ID segment is binary).
            let suffix = String::from_utf8_lossy(&kv_pair.key[prefix_len..]);
            if let Some((field_id, cid_str)) = suffix.split_once('/') {
                if let Ok(cid) = cid_str.parse::<Cid>() {
                    max_priority_by_field
                        .entry(field_id.to_string())
                        .and_modify(|max| *max = (*max).max(priority))
                        .or_insert(priority);
                    entries_by_field
                        .entry(field_id.to_string())
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
            entries.sort_by_cached_key(|a| a.cid.to_string());
        }

        Ok(Self {
            max_priority,
            max_priority_by_field,
            entries_by_field,
        })
    }

    pub fn max_priority(&self) -> u64 {
        self.max_priority
    }

    pub fn field_max_priority(&self, field_id: &str) -> u64 {
        self.max_priority_by_field
            .get(field_id)
            .copied()
            .unwrap_or_default()
    }

    /// Get heads for a specific field (returns empty vec if none).
    pub fn field_heads(&self, field_id: &str) -> Vec<FieldHeadEntry> {
        self.entries_by_field
            .get(field_id)
            .cloned()
            .unwrap_or_default()
    }
}
