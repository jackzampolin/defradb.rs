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
