//! Integration tests for Field

use document::{Error, Field};
use schema::CType;

#[test]
fn test_new_field() {
    let field = Field::new("name", CType::LwwRegister).unwrap();
    assert_eq!(field.name(), "name");
    assert_eq!(field.crdt_type(), CType::LwwRegister);
}

#[test]
fn test_lww_field() {
    let field = Field::lww("title").unwrap();
    assert_eq!(field.name(), "title");
    assert_eq!(field.crdt_type(), CType::LwwRegister);
}

#[test]
fn test_counter_field() {
    let field = Field::counter("views").unwrap();
    assert_eq!(field.name(), "views");
    assert_eq!(field.crdt_type(), CType::PnCounter);
}

#[test]
fn test_display() {
    let field = Field::lww("email").unwrap();
    assert_eq!(field.to_string(), "email");
}

#[test]
fn test_equality() {
    let f1 = Field::lww("name").unwrap();
    let f2 = Field::lww("name").unwrap();
    let f3 = Field::lww("other").unwrap();
    let f4 = Field::counter("name").unwrap();

    assert_eq!(f1, f2);
    assert_ne!(f1, f3);
    assert_ne!(f1, f4); // Same name, different CRDT type
}

#[test]
fn test_empty_name_rejected() {
    let result = Field::new("", CType::LwwRegister);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::EmptyFieldName));
}

#[test]
fn test_empty_name_rejected_lww() {
    let result = Field::lww("");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::EmptyFieldName));
}

#[test]
fn test_empty_name_rejected_counter() {
    let result = Field::counter("");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::EmptyFieldName));
}
