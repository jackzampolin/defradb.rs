//! SE artifact validation.
//!
//! Validates incoming SE artifacts before storage to ensure structural
//! integrity. Prevents malformed or oversized artifacts from polluting
//! the search index.

use crypto::se::{Artifact, SEARCH_TAG_SIZE};

/// Maximum length for collection_id, index_id, and doc_id fields.
const MAX_FIELD_LEN: usize = 512;

/// Validation error for SE artifacts.
#[derive(Debug, thiserror::Error)]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_artifact() -> Artifact {
        Artifact::new("col_v1", "bae123", "age", vec![0u8; SEARCH_TAG_SIZE])
    }

    #[test]
    fn test_valid_artifact() {
        assert!(validate_artifact(&valid_artifact()).is_ok());
    }

    #[test]
    fn test_invalid_tag_size_too_short() {
        let mut a = valid_artifact();
        a.search_tag = vec![0u8; 8];
        let err = validate_artifact(&a).unwrap_err();
        assert!(matches!(err, ValidationError::InvalidTagSize { .. }));
    }

    #[test]
    fn test_invalid_tag_size_too_long() {
        let mut a = valid_artifact();
        a.search_tag = vec![0u8; 32];
        let err = validate_artifact(&a).unwrap_err();
        assert!(matches!(err, ValidationError::InvalidTagSize { .. }));
    }

    #[test]
    fn test_empty_collection_id() {
        let a = Artifact::new("", "doc1", "field", vec![0u8; SEARCH_TAG_SIZE]);
        let err = validate_artifact(&a).unwrap_err();
        assert!(matches!(
            err,
            ValidationError::EmptyField {
                field: "collection_id"
            }
        ));
    }

    #[test]
    fn test_empty_doc_id() {
        let a = Artifact::new("col", "", "field", vec![0u8; SEARCH_TAG_SIZE]);
        let err = validate_artifact(&a).unwrap_err();
        assert!(matches!(
            err,
            ValidationError::EmptyField { field: "doc_id" }
        ));
    }

    #[test]
    fn test_empty_index_id() {
        let a = Artifact::new("col", "doc", "", vec![0u8; SEARCH_TAG_SIZE]);
        let err = validate_artifact(&a).unwrap_err();
        assert!(matches!(
            err,
            ValidationError::EmptyField { field: "index_id" }
        ));
    }

    #[test]
    fn test_field_too_long() {
        let long_id = "x".repeat(MAX_FIELD_LEN + 1);
        let a = Artifact::new(&long_id, "doc", "field", vec![0u8; SEARCH_TAG_SIZE]);
        let err = validate_artifact(&a).unwrap_err();
        assert!(matches!(err, ValidationError::FieldTooLong { .. }));
    }

    #[test]
    fn test_validate_batch_all_valid() {
        let batch = vec![valid_artifact(), valid_artifact()];
        assert!(validate_batch(&batch).is_empty());
    }

    #[test]
    fn test_validate_batch_with_invalid() {
        let mut bad = valid_artifact();
        bad.search_tag = vec![0u8; 1];
        let batch = vec![valid_artifact(), bad, valid_artifact()];
        let errors = validate_batch(&batch);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, 1);
    }
}
