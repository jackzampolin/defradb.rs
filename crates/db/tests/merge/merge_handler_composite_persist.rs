use db::merge::merge_handler::composite_persist::is_unique_constraint_violation;

#[test]
fn unique_constraint_violation_is_classified() {
    let e = db::index::Error::Storage(storage::Error::UniqueConstraintViolation);
    assert!(is_unique_constraint_violation(&e));
}

#[test]
fn non_unique_storage_error_is_not_classified() {
    let e = db::index::Error::Storage(storage::Error::Other("disk full".to_string()));
    assert!(!is_unique_constraint_violation(&e));
}

#[test]
fn non_storage_index_error_is_not_classified() {
    let e = db::index::Error::Other("index misconfigured".to_string());
    assert!(!is_unique_constraint_violation(&e));
}
