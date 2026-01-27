//! Relation finalization for collection schemas
//!
//! This module handles the auto-generation of `_id` fields and auto-determination
//! of primary sides for relation fields. It matches Go DefraDB's `finalizeRelations()`.

use crate::{CType, CollectionVersion, FieldDescription, FieldKind, Result, SchemaError};
use std::collections::{BTreeMap, HashMap, HashSet};

impl CollectionVersion {
    /// Generate the `_id` field name for a relation field
    ///
    /// Go DefraDB uses `_{fieldname}ID` as the convention for storing the foreign key
    /// in non-array relation fields. For example, a field `author` gets `_authorID`.
    pub fn relation_id_field_name(field_name: &str) -> String {
        // Go DefraDB uses: underscore + fieldname + uppercase "ID"
        format!("_{}ID", field_name)
    }

    /// Check if an `_id` field exists for a given relation field name
    pub fn has_relation_id_field(&self, relation_field_name: &str) -> bool {
        let id_field_name = Self::relation_id_field_name(relation_field_name);
        self.fields.iter().any(|f| f.name == id_field_name)
    }

    /// Add `_id` fields for all non-array relation fields that don't already have one
    ///
    /// This matches Go's behavior in `fieldsFromAST()` and `finalizeRelations()`.
    /// For a relation field `author: User`, this generates an `_authorID: ID` field
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
