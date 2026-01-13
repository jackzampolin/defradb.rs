//! Schema validation utilities

use crate::{CollectionVersion, Result, SchemaError};
use std::collections::{HashMap, HashSet};

/// Validates a set of collections for cross-collection constraints
pub struct SchemaValidator<'a> {
    collections: &'a HashMap<String, CollectionVersion>,
}

impl<'a> SchemaValidator<'a> {
    pub fn new(collections: &'a HashMap<String, CollectionVersion>) -> Self {
        Self { collections }
    }

    /// Run all validations
    pub fn validate_all(&self) -> Result<()> {
        self.validate_unique_names()?;
        self.validate_all_collections()?;
        self.validate_relation_primaries()?;
        Ok(())
    }

    /// Ensure no duplicate collection names
    fn validate_unique_names(&self) -> Result<()> {
        let mut seen = HashSet::new();
        for coll in self.collections.values() {
            if !seen.insert(&coll.name) {
                return Err(SchemaError::DuplicateCollectionName(coll.name.clone()));
            }
        }
        Ok(())
    }

    /// Validate each collection individually
    fn validate_all_collections(&self) -> Result<()> {
        for coll in self.collections.values() {
            coll.validate_with_collections(self.collections)?;
        }
        Ok(())
    }

    /// Ensure exactly one side of each relation is marked primary
    fn validate_relation_primaries(&self) -> Result<()> {
        let mut relation_primaries: HashMap<String, Vec<(&str, &str, bool)>> = HashMap::new();

        for coll in self.collections.values() {
            for field in coll.relation_fields() {
                if let Some(rel_name) = &field.relation_name {
                    relation_primaries
                        .entry(rel_name.clone())
                        .or_default()
                        .push((&coll.name, &field.name, field.is_primary));
                }
            }
        }

        for (relation_name, sides) in relation_primaries {
            if sides.len() < 2 {
                continue;
            }

            let primary_count = sides
                .iter()
                .filter(|(_, _, is_primary)| *is_primary)
                .count();

            if primary_count != 1 {
                return Err(SchemaError::RelationPrimaryConflict { relation_name });
            }
        }

        Ok(())
    }
}

/// Convenience function to validate a set of collections
pub fn validate_schema(collections: &HashMap<String, CollectionVersion>) -> Result<()> {
    SchemaValidator::new(collections).validate_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FieldDescription, FieldKind};

    fn user_collection() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "coll-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        )
    }

    fn post_collection_with_author(is_primary: bool) -> CollectionVersion {
        let mut author_field =
            FieldDescription::new("1", "author", FieldKind::relation("users", false))
                .with_relation_name("user_posts");

        if is_primary {
            author_field = author_field.as_primary();
        }

        CollectionVersion::new(
            "posts",
            "v1",
            "coll-posts",
            vec![
                FieldDescription::new("0", "_docID", FieldKind::doc_id()),
                author_field,
            ],
        )
    }

    fn user_collection_with_posts(is_primary: bool) -> CollectionVersion {
        let mut posts_field =
            FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
                .with_relation_name("user_posts");

        if is_primary {
            posts_field = posts_field.as_primary();
        }

        CollectionVersion::new(
            "users",
            "v1",
            "coll-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                posts_field,
            ],
        )
    }

    #[test]
    fn test_validate_empty_schema() {
        let collections = HashMap::new();
        assert!(validate_schema(&collections).is_ok());
    }

    #[test]
    fn test_validate_single_collection() {
        let mut collections = HashMap::new();
        collections.insert("users".to_string(), user_collection());
        assert!(validate_schema(&collections).is_ok());
    }

    #[test]
    fn test_duplicate_collection_names_fails() {
        let mut collections = HashMap::new();
        collections.insert("users".to_string(), user_collection());
        let mut dup = user_collection();
        dup.collection_id = "coll-users-2".into();
        // name stays "users" - should fail
        collections.insert("users-2".to_string(), dup);

        let result = validate_schema(&collections);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::DuplicateCollectionName(_)
        ));
    }

    #[test]
    fn test_unique_collection_names_ok() {
        let mut collections = HashMap::new();
        collections.insert("users".to_string(), user_collection());
        let mut other = user_collection();
        other.name = "admins".into();
        other.collection_id = "coll-admins".into();
        collections.insert("admins".to_string(), other);

        assert!(validate_schema(&collections).is_ok());
    }

    #[test]
    fn test_relation_one_primary_valid() {
        let mut collections = HashMap::new();
        collections.insert("users".to_string(), user_collection_with_posts(false));
        collections.insert("posts".to_string(), post_collection_with_author(true));

        assert!(validate_schema(&collections).is_ok());
    }

    #[test]
    fn test_relation_both_primary_invalid() {
        let mut collections = HashMap::new();
        collections.insert("users".to_string(), user_collection_with_posts(true));
        collections.insert("posts".to_string(), post_collection_with_author(true));

        let result = validate_schema(&collections);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::RelationPrimaryConflict { .. }
        ));
    }

    #[test]
    fn test_relation_neither_primary_invalid() {
        let mut collections = HashMap::new();
        collections.insert("users".to_string(), user_collection_with_posts(false));
        collections.insert("posts".to_string(), post_collection_with_author(false));

        let result = validate_schema(&collections);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::RelationPrimaryConflict { .. }
        ));
    }

    #[test]
    fn test_single_sided_relation_no_primary_check() {
        let mut collections = HashMap::new();
        collections.insert("posts".to_string(), post_collection_with_author(false));

        let result = validate_schema(&collections);
        // Should fail because relation points to nonexistent "users" collection
        assert!(result.is_err());
    }
}
