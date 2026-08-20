use db::collection::name::*;
use db::error::Error;

#[test]
fn test_valid_collection_name() {
    let name = CollectionName::new("Users").unwrap();
    assert_eq!(name.as_str(), "Users");
}

#[test]
fn test_valid_collection_name_with_special_chars() {
    let name = CollectionName::new("User_Posts-2024").unwrap();
    assert_eq!(name.as_str(), "User_Posts-2024");
}

#[test]
fn test_empty_name_fails() {
    let result = CollectionName::new("");
    assert!(matches!(result, Err(Error::InvalidCollectionName(_))));
}

#[test]
fn test_name_with_slash_fails() {
    let result = CollectionName::new("Users/Posts");
    assert!(matches!(result, Err(Error::InvalidCollectionName(_))));
}

#[test]
fn test_name_with_null_byte_fails() {
    let result = CollectionName::new("Users\0");
    assert!(matches!(result, Err(Error::InvalidCollectionName(_))));
}

#[test]
fn test_display() {
    let name = CollectionName::new("Users").unwrap();
    assert_eq!(format!("{}", name), "Users");
}

#[test]
fn test_as_ref() {
    let name = CollectionName::new("Users").unwrap();
    let s: &str = name.as_ref();
    assert_eq!(s, "Users");
}

// Edge case tests matching Go behavior - Go only validates non-empty

#[test]
fn test_whitespace_only_name_allowed() {
    // Go allows whitespace-only names (they're not empty strings)
    let name = CollectionName::new("   ");
    assert!(
        name.is_ok(),
        "Whitespace-only names should be allowed per Go behavior"
    );
    assert_eq!(name.unwrap().as_str(), "   ");
}

#[test]
fn test_leading_trailing_whitespace_allowed() {
    // Go allows leading/trailing whitespace
    let name = CollectionName::new("  Users  ");
    assert!(
        name.is_ok(),
        "Leading/trailing whitespace should be allowed per Go behavior"
    );
    assert_eq!(name.unwrap().as_str(), "  Users  ");
}

#[test]
fn test_tab_and_newline_allowed() {
    // Go allows control characters
    let name = CollectionName::new("Users\tTable");
    assert!(
        name.is_ok(),
        "Tab characters should be allowed per Go behavior"
    );

    let name = CollectionName::new("Users\nTable");
    assert!(
        name.is_ok(),
        "Newline characters should be allowed per Go behavior"
    );
}

#[test]
fn test_unicode_names_allowed() {
    // Go allows Unicode
    let name = CollectionName::new("用户表");
    assert!(name.is_ok(), "Unicode names should be allowed");

    let name = CollectionName::new("Пользователи");
    assert!(name.is_ok(), "Cyrillic names should be allowed");

    let name = CollectionName::new("🚀Users");
    assert!(name.is_ok(), "Emoji in names should be allowed");
}

#[test]
fn test_very_long_name_allowed() {
    // Go has no length limit
    let long_name = "a".repeat(10000);
    let name = CollectionName::new(&long_name);
    assert!(
        name.is_ok(),
        "Very long names should be allowed per Go behavior"
    );
}
