//! JoinSide - encapsulates one side of a join operation

use schema::{CollectionVersion, FieldDescription};
use tracing::warn;

use query_types::error::{QueryError, Result};

/// Represents one side of a join operation.
///
/// Encapsulates the collection schema, relation field, and field indexes needed
/// for join operations. Automatically derives the FK field index from the
/// relation field name for non-array relations.
#[derive(Clone, Debug)]
pub struct JoinSide {
    /// The collection schema for this side
    collection: CollectionVersion,
    /// The relation field description
    relation_field: FieldDescription,
    /// Index of the relation field in the document
    relation_field_index: usize,
    /// Index of the FK field (e.g., `author_id` for an "author" relation) if this side holds the foreign key.
    /// For array relations (one-to-many from this side), this is None since the FK lives on the other side.
    relation_id_field_index: Option<usize>,
}

impl JoinSide {
    /// Create a new join side.
    ///
    /// Automatically derives the FK field index for non-array relations by looking
    /// up the `{relation_field_name}_id` field in the collection schema.
    ///
    /// The `relation_field_index` is the position in the output document mapping
    /// where the joined data will be stored, not an index into `collection.fields`.
    ///
    /// # Errors
    ///
    /// Returns an error if `require_fk` is true and the FK field cannot be found.
    /// This validates that non-array relations have their expected FK field in the schema.
    pub fn new(
        collection: CollectionVersion,
        relation_field: FieldDescription,
        relation_field_index: usize,
    ) -> Result<Self> {
        Self::new_with_fk_requirement(collection, relation_field, relation_field_index, false)
    }

    /// Create a new join side, optionally requiring the FK field to exist.
    ///
    /// When `require_fk` is true and the relation is non-array, returns an error
    /// if the FK field (e.g., `author_id` for an `author` relation) is not found
    /// in the collection schema.
    pub fn new_with_fk_requirement(
        collection: CollectionVersion,
        relation_field: FieldDescription,
        relation_field_index: usize,
        require_fk: bool,
    ) -> Result<Self> {
        // Auto-derive the FK field index for non-array relations.
        // IMPORTANT: Only use the FK field if the relation field is PRIMARY.
        // Secondary relations (is_primary=false) should use inverted joins,
        // looking up by the child's FK field instead.
        let relation_id_field_index = if !relation_field.kind.is_array()
            && relation_field.is_primary
        {
            let id_field_name = CollectionVersion::relation_id_field_name(&relation_field.name);
            let idx = collection
                .fields
                .iter()
                .position(|f| f.name == id_field_name);

            // Validate FK field exists when required
            if require_fk && idx.is_none() {
                return Err(QueryError::internal(format!(
                    "non-array relation '{}' on collection '{}' is missing its FK field '{}'. \
                     This indicates a schema misconfiguration.",
                    relation_field.name, collection.name, id_field_name
                )));
            }

            // Log warning when FK field is missing (but not required)
            // This helps diagnose schema misconfigurations that could cause silent failures
            if !require_fk && idx.is_none() {
                warn!(
                    collection = %collection.name,
                    relation_field = %relation_field.name,
                    expected_fk_field = %id_field_name,
                    "Non-array primary relation is missing its FK field. This may indicate a schema \
                     misconfiguration. The join will use inverted lookup (by parent's _docID)."
                );
            }

            idx
        } else {
            // Array relations always use inverted joins (FK is on the "many" side)
            // Secondary relations also use inverted joins (FK is on the primary side, not here)
            None
        };

        Ok(Self {
            collection,
            relation_field,
            relation_field_index,
            relation_id_field_index,
        })
    }

    pub fn collection(&self) -> &CollectionVersion {
        &self.collection
    }

    pub fn relation_field(&self) -> &FieldDescription {
        &self.relation_field
    }

    pub fn relation_field_index(&self) -> usize {
        self.relation_field_index
    }

    /// Get the FK field index (e.g., `author_id`) if this side holds the FK.
    /// Returns None for array relations since the FK lives on the "many" side.
    pub fn relation_id_field_index(&self) -> Option<usize> {
        self.relation_id_field_index
    }
}
