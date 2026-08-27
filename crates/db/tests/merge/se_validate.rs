use crypto::se::Artifact;
use crypto::se::SEARCH_TAG_SIZE;
use db::merge::se::validate::*;

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
