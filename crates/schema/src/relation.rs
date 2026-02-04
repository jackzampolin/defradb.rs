//! Relation finalization for collection schemas
//!
//! This module handles the auto-generation of `_id` fields and auto-determination
//! of primary sides for relation fields. It matches Go DefraDB's `finalizeRelations()`.

use crate::{
    CType, CollectionVersion, FieldDescription, FieldKind, IndexDescription, Result, SchemaError,
};
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

    /// Check if an index exists with one of the given field names as its first field.
    ///
    /// Go DefraDB checks for indexes by either the relation field name (e.g., `address`)
    /// or the ID field name (e.g., `_addressID`). This allows users to define indexes
    /// on either the relation field or its backing ID field.
    ///
    /// Returns `Some(true)` if a unique index exists, `Some(false)` if a non-unique
    /// index exists, or `None` if no index exists on either field.
    fn has_unique_index_on_relation_field(
        &self,
        id_field_name: &str,
        relation_field_name: &str,
    ) -> Option<bool> {
        for index in &self.indexes {
            if !index.fields.is_empty() {
                let first_field = &index.fields[0].name;
                if first_field == id_field_name || first_field == relation_field_name {
                    return Some(index.unique);
                }
            }
        }
        None
    }

    /// Ensure a unique index exists for a one-to-one relation's _id field.
    ///
    /// Returns `Ok(Some(index))` if a new index should be added.
    /// Returns `Ok(None)` if an existing unique index is sufficient.
    /// Returns `Err` if an existing non-unique index violates the constraint.
    pub fn ensure_one_to_one_unique_index(
        &self,
        relation_field_name: &str,
        next_index_id: &mut impl FnMut() -> u32,
    ) -> Result<Option<IndexDescription>> {
        let id_field_name = Self::relation_id_field_name(relation_field_name);

        // Check for existing index on the _id field or the relation field.
        // Go DefraDB allows users to define indexes on either, so we check both.
        if let Some(is_unique) =
            self.has_unique_index_on_relation_field(&id_field_name, relation_field_name)
        {
            return if is_unique {
                Ok(None) // User's unique index is sufficient
            } else {
                Err(SchemaError::OneToOneRequiresUniqueIndex {
                    object: self.name.clone(),
                    field: relation_field_name.to_string(),
                })
            };
        }

        // No existing index - create automatic unique index
        // Go names these as {Collection}__{fieldWithoutPrefix}_ASC (e.g., User__bossID_ASC)
        // The id_field_name is "_bossID", so strip leading underscore for the index name
        let field_for_name = id_field_name.trim_start_matches('_');
        let index_name = format!("{}__{}_ASC", self.name, field_for_name);
        let mut index = IndexDescription::new(index_name)
            .with_field(id_field_name, false)
            .as_unique();
        index.id = next_index_id();

        Ok(Some(index))
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
    /// 3. Auto-create unique indexes for one-to-one relations
    ///
    /// Uses `BTreeMap` for deterministic processing order.
    /// Matches Go's `finalizeRelations()` function.
    ///
    /// # Errors
    /// Returns an error if field ID generation produces duplicates or if a
    /// one-to-one relation has a non-unique index defined.
    pub fn finalize_relations(
        collections: &mut BTreeMap<String, CollectionVersion>,
        mut next_field_id: impl FnMut() -> String,
        mut next_index_id: impl FnMut() -> u32,
    ) -> Result<()> {
        let collection_names: Vec<String> = collections.keys().cloned().collect();

        for name in collection_names {
            let mut collection = collections
                .remove(&name)
                .ok_or_else(|| SchemaError::CollectionNotFound(name.clone()))?;

            // Find fields that need _id and/or auto-primary, and track one-to-one relations
            let mut updates = Vec::new();
            let mut one_to_one_fields = Vec::new();

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

                    // Check if other side is also non-array (one-to-one)
                    let other_is_array = other_field.map(|f| f.kind.is_array()).unwrap_or(false);

                    // If other side doesn't exist or is an array, this side is primary
                    if other_field.is_none() || other_is_array {
                        updates.push((idx, true)); // Mark as primary
                    } else {
                        // Other side exists and is non-array: this is a one-to-one relation
                        // Don't auto-mark as primary - rely on @primary directive from schema
                        // But track for unique index creation if this side is marked primary
                        if field.is_primary {
                            one_to_one_fields.push(field.name.clone());
                        }
                    }
                }
            }

            // Apply primary updates
            for (idx, is_primary) in updates {
                collection.fields[idx].is_primary = is_primary;
            }

            // Add unique indexes for one-to-one relations
            let mut indexes_to_add = Vec::new();
            for field_name in one_to_one_fields {
                if let Some(index) =
                    collection.ensure_one_to_one_unique_index(&field_name, &mut next_index_id)?
                {
                    indexes_to_add.push(index);
                }
            }
            for index in indexes_to_add {
                collection.indexes.push(index);
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
        next_index_id: impl FnMut() -> u32,
    ) -> Result<()> {
        let mut btree: BTreeMap<String, CollectionVersion> = collections.drain().collect();
        Self::finalize_relations(&mut btree, next_field_id, next_index_id)?;
        collections.extend(btree);
        Ok(())
    }
}
