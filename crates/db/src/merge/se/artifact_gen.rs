//! SE artifact generation from documents.
//!
//! Generates search tags for encrypted-indexed fields using HMAC-SHA256.
//! Matches Go's internal/se/se.go generateDocArtifacts and generateFieldArtifact.
//! Passing `None` for `identity_pubkey` preserves Go-compatible tags. Passing
//! `Some` enables Rust-only per-identity tag isolation and should be limited to
//! Rust-only deployments until Go supports the same isolation input.

use crypto::se::{generate_equality_tag, Artifact};
use document::NormalValue;
use schema::{EncryptedIndexDescription, EncryptedIndexType};
use storage::field_value::encode_field_value;

use storage::corekv::Result;

/// Generate a search artifact for a single field value.
///
/// Creates a deterministic search tag for the given field value that can be
/// used for equality queries on replicator nodes.
///
/// # Arguments
///
/// * `collection_id` - Collection version ID
/// * `doc_id` - Document ID
/// * `enc_idx` - Encrypted index description
/// * `field_value` - The field value to generate a tag for
/// * `identity_pubkey` - Identity's public key (for tag isolation)
/// * `enc_key` - SE encryption key (32 bytes)
///
/// # Returns
///
/// An artifact containing the search tag for this field value.
pub fn generate_field_artifact(
    collection_id: &str,
    doc_id: &str,
    enc_idx: &EncryptedIndexDescription,
    field_value: &NormalValue,
    identity_pubkey: Option<&[u8]>,
    enc_key: &[u8],
) -> Result<Artifact> {
    // Encode the field value using order-preserving encoding (ascending)
    let value_bytes = encode_field_value(Vec::new(), field_value, false)?;

    // Get identity bytes (empty if no identity)
    let identity_bytes = identity_pubkey.unwrap_or(&[]);

    // Generate tag based on index type
    let tag = match &enc_idx.index_type {
        EncryptedIndexType::Equality => generate_equality_tag(
            enc_key,
            identity_bytes,
            collection_id,
            &enc_idx.field_name,
            &value_bytes,
        )
        .map_err(|e| storage::corekv::Error::Other(e.to_string()))?,
        _ => {
            return Err(storage::corekv::Error::Other(format!(
                "unsupported encrypted index type: {:?}",
                enc_idx.index_type
            )))
        }
    };

    Ok(Artifact::new(
        collection_id,
        doc_id,
        &enc_idx.field_name, // IndexID is the field name
        tag.to_vec(),
    ))
}

/// Generate SE artifacts for specified fields of a document.
///
/// # Arguments
///
/// * `collection_id` - Collection version ID
/// * `doc_id` - Document ID
/// * `encrypted_indexes` - List of encrypted indexes for the collection
/// * `field_names` - Fields to generate artifacts for (empty = all encrypted fields)
/// * `field_values` - Map of field name to value
/// * `identity_pubkey` - Identity's public key (for tag isolation)
/// * `enc_key` - SE encryption key
///
/// # Returns
///
/// Vector of artifacts for the specified fields that have encrypted indexes.
pub fn generate_doc_artifacts(
    collection_id: &str,
    doc_id: &str,
    encrypted_indexes: &[EncryptedIndexDescription],
    field_names: &[String],
    field_values: &std::collections::HashMap<String, NormalValue>,
    identity_pubkey: Option<&[u8]>,
    enc_key: &[u8],
) -> Result<Vec<Artifact>> {
    if encrypted_indexes.is_empty() {
        return Ok(Vec::new());
    }

    let mut artifacts = Vec::new();

    for enc_idx in encrypted_indexes {
        // If field_names is specified, only process those fields
        if !field_names.is_empty() && !field_names.contains(&enc_idx.field_name) {
            continue;
        }

        // Get the field value
        let Some(field_value) = field_values.get(&enc_idx.field_name) else {
            continue;
        };

        let artifact = generate_field_artifact(
            collection_id,
            doc_id,
            enc_idx,
            field_value,
            identity_pubkey,
            enc_key,
        )?;

        artifacts.push(artifact);
    }

    Ok(artifacts)
}
