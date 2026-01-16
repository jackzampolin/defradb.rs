//! Relation finalization for collection schemas
//!
//! This module handles the auto-generation of `_id` fields and auto-determination
//! of primary sides for relation fields. It matches Go DefraDB's `finalizeRelations()`.

use crate::{CType, CollectionVersion, FieldDescription, FieldKind, Result, SchemaError};
use std::collections::{BTreeMap, HashMap, HashSet};

impl CollectionVersion {
    /// Generate the `_id` field name for a relation field
    ///
    /// Go DefraDB uses `{fieldname}_id` as the convention for storing the foreign key
    /// in non-array relation fields. For example, a field `author` gets `author_id`.
    pub fn relation_id_field_name(field_name: &str) -> String {
        format!("{}_id", field_name)
    }

    /// Check if an `_id` field exists for a given relation field name
    pub fn has_relation_id_field(&self, relation_field_name: &str) -> bool {
        let id_field_name = Self::relation_id_field_name(relation_field_name);
        self.fields.iter().any(|f| f.name == id_field_name)
    }

    /// Add `_id` fields for all non-array relation fields that don't already have one
    ///
    /// This matches Go's behavior in `fieldsFromAST()` and `finalizeRelations()`.
    /// For a relation field `author: User`, this generates an `author_id: ID` field
    /// with the same relation_name and is_primary status.
    ///
    /// The `next_field_id` function is called to generate unique field IDs.
    ///
    /// # Errors
    /// Returns `SchemaError::DuplicateFieldId` if the generated field ID already exists.
    pub fn add_relation_id_fields(
        &mut self,
        mut next_field_id: impl FnMut() -> String,
    ) -> Result<()> {
        let existing_ids: HashSet<&str> = self.fields.iter().map(|f| f.id.as_str()).collect();
        let mut fields_to_add = Vec::new();
        let mut new_ids = HashSet::new();

        for field in &self.fields {
            // Only process non-array relation fields
            if !field.kind.is_relation() || field.kind.is_array() {
                continue;
            }

            let id_field_name = Self::relation_id_field_name(&field.name);

            // Skip if _id field already exists
            if self.fields.iter().any(|f| f.name == id_field_name) {
                continue;
            }

            // Generate new field ID and validate uniqueness
            let new_id = next_field_id();
            if existing_ids.contains(new_id.as_str()) || new_ids.contains(&new_id) {
                return Err(SchemaError::DuplicateFieldId(new_id));
            }
            new_ids.insert(new_id.clone());

            // Create the _id field with same relation_name and is_primary
            let id_field = FieldDescription::new(new_id, id_field_name, FieldKind::doc_id())
                .with_crdt_type(CType::LwwRegister);

            let id_field = if let Some(rel_name) = &field.relation_name {
                id_field.with_relation_name(rel_name.clone())
            } else {
                id_field
            };

            let id_field = if field.is_primary {
                id_field.as_primary()
            } else {
                id_field
            };

            fields_to_add.push((field.name.clone(), id_field));
        }

        // Insert each _id field immediately after its corresponding relation field
        for (relation_field_name, id_field) in fields_to_add {
            let pos = self
                .fields
                .iter()
                .position(|f| f.name == relation_field_name)
                .ok_or_else(|| {
                    SchemaError::InternalError(format!(
                        "invariant violation: relation field '{}' disappeared during _id field generation",
                        relation_field_name
                    ))
                })?;
            self.fields.insert(pos + 1, id_field);
        }

        Ok(())
    }

    /// Finalize relation fields for a set of collections
    ///
    /// This is called after all collections are parsed to:
    /// 1. Auto-generate missing `_id` fields for non-array relations
    /// 2. Auto-determine which side is primary for one-to-many relations
    ///
    /// Uses `BTreeMap` for deterministic processing order.
    /// Matches Go's `finalizeRelations()` function.
    ///
    /// # Errors
    /// Returns an error if field ID generation produces duplicates.
    pub fn finalize_relations(
        collections: &mut BTreeMap<String, CollectionVersion>,
        mut next_field_id: impl FnMut() -> String,
    ) -> Result<()> {
        let collection_names: Vec<String> = collections.keys().cloned().collect();

        for name in collection_names {
            let mut collection = collections
                .remove(&name)
                .ok_or_else(|| SchemaError::CollectionNotFound(name.clone()))?;

            // Find fields that need _id and/or auto-primary
            let mut updates = Vec::new();

            for (idx, field) in collection.fields.iter().enumerate() {
                if !field.kind.is_relation() {
                    continue;
                }

                // Non-array relations are the "one" side (or one-to-one primary)
                // Array relations are the "many" side
                if !field.kind.is_array() {
                    // Relation fields must have a relation_name
                    let rel_name = field.relation_name.as_ref().ok_or_else(|| {
                        SchemaError::MissingRequiredField(format!(
                            "relation field '{}.{}' missing required relation_name",
                            collection.name, field.name
                        ))
                    })?;

                    // Invariant: is_relation() implies relation_collection_id() returns Some
                    let other_col_id = field.kind.relation_collection_id().ok_or_else(|| {
                        SchemaError::InternalError(format!(
                            "invariant violation: relation field '{}.{}' has is_relation()=true but no collection_id",
                            collection.name, field.name
                        ))
                    })?;

                    // Handle self-referential relations: current collection was removed from map
                    let other_col_opt = if other_col_id == collection.name {
                        Some(&collection)
                    } else {
                        collections.get(other_col_id)
                    };

                    // Look up the other side of the relation
                    let other_field = other_col_opt.and_then(|col| {
                        col.field_by_relation(rel_name, &collection.name, &field.name)
                    });

                    // If other side doesn't exist or is an array, this side is primary
                    if other_field.is_none()
                        || other_field.map(|f| f.kind.is_array()).unwrap_or(false)
                    {
                        updates.push((idx, true)); // Mark as primary
                    }
                }
            }

            // Apply primary updates
            for (idx, is_primary) in updates {
                collection.fields[idx].is_primary = is_primary;
            }

            // Add missing _id fields
            collection.add_relation_id_fields(&mut next_field_id)?;

            collections.insert(name, collection);
        }

        Ok(())
    }

    /// Finalize relation fields using a HashMap (convenience wrapper)
    ///
    /// Converts the HashMap to a BTreeMap for deterministic processing,
    /// then converts back. Use `finalize_relations` directly with BTreeMap
    /// for better performance with large schemas.
    pub fn finalize_relations_hashmap(
        collections: &mut HashMap<String, CollectionVersion>,
        next_field_id: impl FnMut() -> String,
    ) -> Result<()> {
        let mut btree: BTreeMap<String, CollectionVersion> = collections.drain().collect();
        Self::finalize_relations(&mut btree, next_field_id)?;
        collections.extend(btree);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CType;
    use std::collections::BTreeMap;

    #[test]
    fn test_relation_id_field_name() {
        assert_eq!(
            CollectionVersion::relation_id_field_name("author"),
            "author_id"
        );
        assert_eq!(
            CollectionVersion::relation_id_field_name("posts"),
            "posts_id"
        );
    }

    #[test]
    fn test_add_relation_id_fields() {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("user_posts")
                .as_primary(),
        ];
        let mut coll = CollectionVersion::new("posts", "v1", "coll-posts", fields);

        let mut counter = 100;
        coll.add_relation_id_fields(|| {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        assert_eq!(coll.fields.len(), 3);

        // Verify _id field was added
        let id_field = coll.field_by_name("author_id").unwrap();
        assert_eq!(id_field.id, "101");
        assert_eq!(id_field.kind, FieldKind::doc_id());
        assert_eq!(id_field.relation_name, Some("user_posts".to_string()));
        assert!(id_field.is_primary);
        assert_eq!(id_field.crdt_type, CType::LwwRegister);

        // Verify _id field is after relation field
        let author_idx = coll.fields.iter().position(|f| f.name == "author").unwrap();
        let author_id_idx = coll
            .fields
            .iter()
            .position(|f| f.name == "author_id")
            .unwrap();
        assert_eq!(author_id_idx, author_idx + 1);
    }

    #[test]
    fn test_add_relation_id_fields_skips_arrays() {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            // Array relation (one-to-many from the "many" side) - no _id field needed
            FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                .with_relation_name("user_posts"),
        ];
        let mut coll = CollectionVersion::new("users", "v1", "coll-users", fields);

        coll.add_relation_id_fields(|| "999".to_string()).unwrap();

        // No _id field should be added for array relations
        assert_eq!(coll.fields.len(), 2);
        assert!(coll.field_by_name("posts_id").is_none());
    }

    #[test]
    fn test_add_relation_id_fields_skips_existing() {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("user_posts"),
            // _id field already exists
            FieldDescription::new("3", "author_id", FieldKind::doc_id())
                .with_relation_name("user_posts"),
        ];
        let mut coll = CollectionVersion::new("posts", "v1", "coll-posts", fields);

        coll.add_relation_id_fields(|| "999".to_string()).unwrap();

        // No new _id field should be added
        assert_eq!(coll.fields.len(), 3);
        // Original _id field should remain
        assert_eq!(coll.field_by_name("author_id").unwrap().id, "3");
    }

    #[test]
    fn test_has_relation_id_field() {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false)),
            FieldDescription::new("3", "author_id", FieldKind::doc_id()),
        ];
        let coll = CollectionVersion::new("posts", "v1", "coll-posts", fields);

        assert!(coll.has_relation_id_field("author"));
        assert!(!coll.has_relation_id_field("publisher"));
    }

    #[test]
    fn test_finalize_relations_adds_id_fields() {
        let users_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // One-to-many (array) - no _id field needed
            FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
                .with_relation_name("user_posts"),
        ];
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            // Many-to-one (non-array) - _id field needed
            FieldDescription::new("3", "author", FieldKind::relation("users", false))
                .with_relation_name("user_posts"),
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "users".to_string(),
            CollectionVersion::new("users", "v1", "coll-users", users_fields),
        );
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );

        let mut counter = 100;
        CollectionVersion::finalize_relations(&mut collections, || {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        let users = collections.get("users").unwrap();
        let posts = collections.get("posts").unwrap();

        // Users shouldn't have _id field (array relation)
        assert!(users.field_by_name("posts_id").is_none());

        // Posts should have _id field (non-array relation)
        assert!(posts.field_by_name("author_id").is_some());

        // Posts.author should be marked as primary (other side is array)
        assert!(posts.field_by_name("author").unwrap().is_primary);
    }

    #[test]
    fn test_add_relation_id_fields_rejects_duplicate_id() {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("user_posts"),
        ];
        let mut coll = CollectionVersion::new("posts", "v1", "coll-posts", fields);

        // Generator that returns an existing field ID ("1" already exists)
        let result = coll.add_relation_id_fields(|| "1".to_string());

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::DuplicateFieldId(id) if id == "1"
        ));
    }

    #[test]
    fn test_finalize_relations_hashmap() {
        let mut collections = HashMap::new();
        collections.insert(
            "users".to_string(),
            CollectionVersion::new(
                "users",
                "v1",
                "coll-users",
                vec![
                    FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                    FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                        .with_relation_name("user_posts"),
                ],
            ),
        );
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new(
                "posts",
                "v1",
                "coll-posts",
                vec![
                    FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                    FieldDescription::new("2", "author", FieldKind::relation("users", false))
                        .with_relation_name("user_posts"),
                ],
            ),
        );

        let mut counter = 100;
        CollectionVersion::finalize_relations_hashmap(&mut collections, || {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        // Verify HashMap was updated in place
        let posts = collections.get("posts").unwrap();
        assert!(
            posts.field_by_name("author_id").is_some(),
            "author_id field should be added"
        );

        // Verify auto-primary was applied (author side is primary since users.posts is array)
        assert!(
            posts.field_by_name("author").unwrap().is_primary,
            "author should be marked as primary"
        );

        // Verify users collection is also in the HashMap
        let users = collections.get("users").unwrap();
        assert!(users.field_by_name("posts").is_some());
    }
}
