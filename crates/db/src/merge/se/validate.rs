//! SE artifact validation.
//!
//! Validates incoming SE artifacts before storage to ensure structural
//! integrity. Prevents malformed or oversized artifacts from polluting
//! the search index.

use crypto::se::{Artifact, SEARCH_TAG_SIZE};

/// Maximum length for collection_id, index_id, and doc_id fields.
pub const MAX_FIELD_LEN: usize = 512;

/// Validation error for SE artifacts.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ValidationError {
    #[error("search tag has invalid size: expected {expected}, got {actual}")]
    InvalidTagSize { expected: usize, actual: usize },

    #[error("empty {field}")]
    EmptyField { field: &'static str },

    #[error("{field} exceeds maximum length of {max}: got {actual}")]
    FieldTooLong {
        field: &'static str,
        max: usize,
        actual: usize,
    },
}

/// Validate a single SE artifact.
///
/// Checks:
/// - search_tag is exactly SEARCH_TAG_SIZE (16) bytes
/// - collection_id, index_id, doc_id are non-empty
/// - No field exceeds MAX_FIELD_LEN
pub fn validate_artifact(artifact: &Artifact) -> Result<(), ValidationError> {
    if artifact.search_tag.len() != SEARCH_TAG_SIZE {
        return Err(ValidationError::InvalidTagSize {
            expected: SEARCH_TAG_SIZE,
            actual: artifact.search_tag.len(),
        });
    }

    validate_field_length("collection_id", &artifact.collection_id)?;
    validate_field_length("index_id", &artifact.index_id)?;
    validate_field_length("doc_id", &artifact.doc_id)?;

    Ok(())
}

/// Validate a batch of artifacts, returning errors for invalid ones.
///
/// Returns a vec of (index, error) pairs for each invalid artifact.
/// An empty vec means all artifacts are valid.
pub fn validate_batch(artifacts: &[Artifact]) -> Vec<(usize, ValidationError)> {
    artifacts
        .iter()
        .enumerate()
        .filter_map(|(i, a)| validate_artifact(a).err().map(|e| (i, e)))
        .collect()
}

fn validate_field_length(field: &'static str, value: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::EmptyField { field });
    }
    if value.len() > MAX_FIELD_LEN {
        return Err(ValidationError::FieldTooLong {
            field,
            max: MAX_FIELD_LEN,
            actual: value.len(),
        });
    }
    Ok(())
}
