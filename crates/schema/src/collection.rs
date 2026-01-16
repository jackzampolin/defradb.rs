//! Collection version definitions

use crate::{
    CollectionSetDescription, CollectionSource, EncryptedIndexDescription, FieldDescription,
    FieldKind, IndexDescription, PolicyDescription, QuerySource, Result, SchemaError,
    VectorEmbeddingDescription,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Helper to deserialize `null` as an empty Vec (Go serializes empty slices as null).
fn deserialize_null_as_empty_vec<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let opt: Option<Vec<T>> = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

/// A versioned collection schema.
///
/// This matches Go's CollectionVersion struct.
/// Field names use serde rename to match Go's JSON format (PascalCase).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionVersion {
    /// Human-readable collection name
    #[serde(rename = "Name")]
    pub name: String,

    /// Content hash of this version (immutable)
    #[serde(rename = "VersionID")]
    pub version_id: String,

    /// Stable collection ID across versions
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Information about this collection's membership in a collection set.
    /// Collections form a set when they have circular relations at creation time.
    #[serde(
        rename = "CollectionSet",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub collection_set: Option<CollectionSetDescription>,

    /// Query source for views that derive data from queries.
    #[serde(rename = "Query", default, skip_serializing_if = "Option::is_none")]
    pub query: Option<QuerySource>,

    /// Path to the previous collection version (for schema migrations).
    #[serde(
        rename = "PreviousVersion",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub previous_version: Option<CollectionSource>,

    /// Fields in this collection
    #[serde(rename = "Fields", deserialize_with = "deserialize_null_as_empty_vec")]
    pub fields: Vec<FieldDescription>,

    /// Secondary indexes on this collection
    #[serde(
        rename = "Indexes",
        default,
        deserialize_with = "deserialize_null_as_empty_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub indexes: Vec<IndexDescription>,

    /// Encrypted indexes for searchable encryption
    #[serde(
        rename = "EncryptedIndexes",
        default,
        deserialize_with = "deserialize_null_as_empty_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub encrypted_indexes: Vec<EncryptedIndexDescription>,

    /// Access control policy for this collection
    #[serde(rename = "Policy", default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyDescription>,

    /// Whether this is the active version
    #[serde(rename = "IsActive", default = "default_active")]
    pub is_active: bool,

    /// Whether collection items are cached (materialized) or computed at query-time
    #[serde(rename = "IsMaterialized", default)]
    pub is_materialized: bool,

    /// Whether the collection history is tracked as a single verifiable entity
    #[serde(rename = "IsBranchable", default)]
    pub is_branchable: bool,

    /// Whether this collection exists only as embedded child objects
    #[serde(rename = "IsEmbeddedOnly", default)]
    pub is_embedded_only: bool,

    /// Whether this is a placeholder version waiting to be defined
    #[serde(rename = "IsPlaceholder", default)]
    pub is_placeholder: bool,

    /// Configuration for generating embedding vectors (AI/ML)
    #[serde(
        rename = "VectorEmbeddings",
        default,
        deserialize_with = "deserialize_null_as_empty_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub vector_embeddings: Vec<VectorEmbeddingDescription>,
}

fn default_active() -> bool {
    true
}

impl CollectionVersion {
    /// Create a new collection version with default values for optional fields.
    pub fn new(
        name: impl Into<String>,
        version_id: impl Into<String>,
        collection_id: impl Into<String>,
        fields: Vec<FieldDescription>,
    ) -> Self {
        Self {
            name: name.into(),
            version_id: version_id.into(),
            collection_id: collection_id.into(),
            collection_set: None,
            query: None,
            previous_version: None,
            fields,
            indexes: Vec::new(),
            encrypted_indexes: Vec::new(),
            policy: None,
            is_active: true,
            is_materialized: false,
            is_branchable: false,
            is_embedded_only: false,
            is_placeholder: false,
            vector_embeddings: Vec::new(),
        }
    }

    /// Set the previous version source (for migrations).
    pub fn with_previous_version(mut self, source: CollectionSource) -> Self {
        self.previous_version = Some(source);
        self
    }

    /// Set the collection set membership.
    pub fn with_collection_set(mut self, set: CollectionSetDescription) -> Self {
        self.collection_set = Some(set);
        self
    }

    /// Add an index to the collection.
    pub fn with_index(mut self, index: IndexDescription) -> Self {
        self.indexes.push(index);
        self
    }

    /// Set the access control policy.
    pub fn with_policy(mut self, policy: PolicyDescription) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Mark as branchable (history tracked as verifiable entity).
    pub fn as_branchable(mut self) -> Self {
        self.is_branchable = true;
        self
    }

    /// Mark as embedded only (not directly queryable).
    pub fn as_embedded_only(mut self) -> Self {
        self.is_embedded_only = true;
        self
    }

    /// Get a field by name
    pub fn field_by_name(&self, name: &str) -> Option<&FieldDescription> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get a field by ID
    pub fn field_by_id(&self, id: &str) -> Option<&FieldDescription> {
        self.fields.iter().find(|f| f.id == id)
    }

    /// Get a field by relation name (simple lookup)
    pub fn field_by_relation_name(&self, relation_name: &str) -> Option<&FieldDescription> {
        self.fields
            .iter()
            .find(|f| f.relation_name.as_deref() == Some(relation_name))
    }

    /// Get a field by relation (matches Go's GetFieldByRelation)
    ///
    /// Returns the field on this collection that is part of the given relation,
    /// excluding the field that matches the "other" collection/field pair.
    /// This is needed to find the "other side" of a relation when both sides
    /// have the same relation_name.
    ///
    /// # Arguments
    /// * `relation_name` - The name of the relation
    /// * `other_collection_name` - The name of the other collection (to exclude self-matches)
    /// * `other_field_name` - The name of the other field (to exclude self-matches)
    ///
    /// # Returns
    /// Returns `None` in any of these cases:
    /// - No field exists with the given `relation_name`
    /// - All matching fields were excluded by the `other_collection_name`/`other_field_name` filter
    /// - All matching fields are `DocID` scalars (auto-generated `_id` fields are excluded)
    ///
    /// This means callers cannot distinguish "relation doesn't exist" from
    /// "relation exists but was filtered out". Use `field_by_relation_name()` for
    /// simple lookups when you don't need the exclusion filter.
    pub fn field_by_relation(
        &self,
        relation_name: &str,
        other_collection_name: &str,
        other_field_name: &str,
    ) -> Option<&FieldDescription> {
        self.fields.iter().find(|f| {
            f.relation_name.as_deref() == Some(relation_name)
                && !(self.name == other_collection_name && f.name == other_field_name)
                && !matches!(f.kind, FieldKind::Scalar(crate::ScalarKind::DocID))
        })
    }

    /// Get all relation fields
    pub fn relation_fields(&self) -> impl Iterator<Item = &FieldDescription> {
        self.fields.iter().filter(|f| f.kind.is_relation())
    }

    /// Generate the `_id` field name for a relation field
    ///
    /// Go uses `{fieldname}_id` as the convention for storing the foreign key
    /// in relation fields. This constant is defined in client/request/consts.go
    /// as `RelatedObjectID = "_id"` and used like `field.Name + request.RelatedObjectID`.
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
        // Collect existing field IDs for uniqueness validation
        let existing_ids: HashSet<&str> = self.fields.iter().map(|f| f.id.as_str()).collect();

        // Collect info about relation fields that need _id fields
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
                .with_crdt_type(crate::CType::LwwRegister);

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
                .expect("relation field must exist - collected from same fields list");
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
        // BTreeMap provides deterministic iteration order (sorted by key)
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
                    // Check if the other side exists and is an array
                    if let Some(rel_name) = &field.relation_name {
                        if let Some(other_col_id) = field.kind.relation_collection_id() {
                            if let Some(other_col) = collections.get(other_col_id) {
                                let other_field = other_col.field_by_relation(
                                    rel_name,
                                    &collection.name,
                                    &field.name,
                                );

                                // If other side doesn't exist or is an array, this side is primary
                                if other_field.is_none()
                                    || other_field.map(|f| f.kind.is_array()).unwrap_or(false)
                                {
                                    updates.push((idx, true)); // Mark as primary
                                }
                            }
                        }
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
        // Convert to BTreeMap for deterministic processing
        let mut btree: BTreeMap<String, CollectionVersion> = collections.drain().collect();
        Self::finalize_relations(&mut btree, next_field_id)?;
        // Convert back to HashMap
        collections.extend(btree);
        Ok(())
    }

    /// Validate the collection schema
    pub fn validate(&self) -> Result<()> {
        self.validate_no_duplicate_names()?;
        self.validate_fields()?;
        Ok(())
    }

    /// Validate with access to other collections for relation checking
    pub fn validate_with_collections(
        &self,
        collections: &HashMap<String, CollectionVersion>,
    ) -> Result<()> {
        self.validate()?;
        self.validate_relations(collections)?;
        Ok(())
    }

    fn validate_no_duplicate_names(&self) -> Result<()> {
        let mut seen = HashSet::new();
        for field in &self.fields {
            if !seen.insert(&field.name) {
                return Err(SchemaError::DuplicateFieldName(field.name.clone()));
            }
        }
        Ok(())
    }

    fn validate_fields(&self) -> Result<()> {
        for field in &self.fields {
            field.validate()?;
        }
        Ok(())
    }

    fn validate_relations(&self, collections: &HashMap<String, CollectionVersion>) -> Result<()> {
        for field in self.relation_fields() {
            if let Some(collection_id) = field.kind.relation_collection_id() {
                if !collections.contains_key(collection_id) {
                    return Err(SchemaError::InvalidRelation {
                        field_name: field.name.clone(),
                        collection_id: collection_id.to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Builder for creating collection versions
pub struct CollectionBuilder {
    name: String,
    collection_id: String,
    fields: Vec<FieldDescription>,
}

impl CollectionBuilder {
    /// Start building a new collection
    pub fn new(name: impl Into<String>, collection_id: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            collection_id: collection_id.into(),
            fields: Vec::new(),
        }
    }

    /// Add a field to the collection
    pub fn field(mut self, field: FieldDescription) -> Self {
        self.fields.push(field);
        self
    }

    /// Add a simple scalar field
    pub fn scalar(
        mut self,
        id: impl Into<String>,
        name: impl Into<String>,
        kind: FieldKind,
    ) -> Self {
        self.fields.push(FieldDescription::new(id, name, kind));
        self
    }

    /// Build the collection version (generates version_id from content hash)
    pub fn build(self) -> CollectionVersion {
        let version_id = self.compute_version_id();
        CollectionVersion::new(self.name, version_id, self.collection_id, self.fields)
    }

    fn compute_version_id(&self) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.collection_id.hash(&mut hasher);
        for field in &self.fields {
            field.id.hash(&mut hasher);
            field.name.hash(&mut hasher);
        }
        format!("v{:x}", hasher.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CType;

    fn sample_fields() -> Vec<FieldDescription> {
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ]
    }

    #[test]
    fn test_new_collection() {
        let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
        assert_eq!(coll.name, "users");
        assert_eq!(coll.version_id, "v1");
        assert!(coll.is_active);
        assert_eq!(coll.fields.len(), 3);
    }

    #[test]
    fn test_field_by_name() {
        let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
        let field = coll.field_by_name("name").unwrap();
        assert_eq!(field.id, "2");
    }

    #[test]
    fn test_field_by_id() {
        let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
        let field = coll.field_by_id("3").unwrap();
        assert_eq!(field.name, "age");
    }

    #[test]
    fn test_field_by_relation_name() {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author"),
        ];
        let coll = CollectionVersion::new("posts", "v1", "coll-1", fields);

        let field = coll.field_by_relation_name("post_author").unwrap();
        assert_eq!(field.name, "author");
        assert!(coll.field_by_relation_name("nonexistent").is_none());
    }

    #[test]
    fn test_field_by_relation() {
        // Create posts collection with author field
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("author_posts"),
        ];
        let posts = CollectionVersion::new("posts", "v1", "coll-posts", posts_fields);

        // Create users collection with posts field
        let users_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                .with_relation_name("author_posts"),
        ];
        let users = CollectionVersion::new("users", "v1", "coll-users", users_fields);

        // From posts collection, find the field in users that is part of "author_posts"
        // but not the "author" field from "posts"
        let field = users
            .field_by_relation("author_posts", "posts", "author")
            .unwrap();
        assert_eq!(field.name, "posts");

        // From users collection, find the field in posts that is part of "author_posts"
        // but not the "posts" field from "users"
        let field = posts
            .field_by_relation("author_posts", "users", "posts")
            .unwrap();
        assert_eq!(field.name, "author");

        // Nonexistent relation should return None
        assert!(posts
            .field_by_relation("nonexistent", "users", "posts")
            .is_none());
    }

    #[test]
    fn test_validate_duplicate_names_fails() {
        let fields = vec![
            FieldDescription::new("1", "name", FieldKind::string()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ];
        let coll = CollectionVersion::new("users", "v1", "coll-1", fields);

        let result = coll.validate();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::DuplicateFieldName(_)
        ));
    }

    #[test]
    fn test_validate_invalid_crdt_fails() {
        let fields = vec![FieldDescription::new("1", "title", FieldKind::string())
            .with_crdt_type(CType::PnCounter)];
        let coll = CollectionVersion::new("posts", "v1", "coll-1", fields);

        let result = coll.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_valid_collection() {
        let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
        assert!(coll.validate().is_ok());
    }

    #[test]
    fn test_builder() {
        let coll = CollectionBuilder::new("users", "coll-1")
            .scalar("1", "_docID", FieldKind::doc_id())
            .scalar("2", "name", FieldKind::string())
            .field(
                FieldDescription::new("3", "score", FieldKind::int())
                    .with_crdt_type(CType::PnCounter),
            )
            .build();

        assert_eq!(coll.name, "users");
        assert_eq!(coll.fields.len(), 3);
        assert!(coll.version_id.starts_with('v'));
    }

    #[test]
    fn test_relation_validation() {
        let author_field =
            FieldDescription::new("1", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author");

        let posts = CollectionVersion::new("posts", "v1", "coll-posts", vec![author_field]);

        let mut collections = HashMap::new();
        collections.insert(
            "users".to_string(),
            CollectionVersion::new(
                "users",
                "v1",
                "coll-users",
                vec![FieldDescription::new("1", "name", FieldKind::string())],
            ),
        );

        assert!(posts.validate_with_collections(&collections).is_ok());
    }

    #[test]
    fn test_relation_to_unknown_collection_fails() {
        let author_field =
            FieldDescription::new("1", "author", FieldKind::relation("unknown", false))
                .with_relation_name("post_author");

        let posts = CollectionVersion::new("posts", "v1", "coll-posts", vec![author_field]);

        let collections = HashMap::new();
        let result = posts.validate_with_collections(&collections);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::InvalidRelation { .. }
        ));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let coll = CollectionVersion::new("users", "v1", "coll-1", sample_fields());
        let json = serde_json::to_string(&coll).unwrap();
        let parsed: CollectionVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(coll, parsed);
    }

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

        // Users should NOT get a posts_id field (array relation)
        assert!(users.field_by_name("posts_id").is_none());
        assert_eq!(users.fields.len(), 3);

        // Posts SHOULD get an author_id field (non-array relation)
        let author_id = posts.field_by_name("author_id").unwrap();
        assert_eq!(author_id.kind, FieldKind::doc_id());
        assert_eq!(posts.fields.len(), 4);
    }

    #[test]
    fn test_finalize_relations_auto_sets_primary() {
        // One-to-many: non-array side should auto-become primary
        let users_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            // Array side - should NOT be primary
            FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                .with_relation_name("user_posts"),
        ];
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            // Non-array side - should auto-become primary
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
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

        // Array side should NOT be primary
        let posts_field = users.field_by_name("posts").unwrap();
        assert!(!posts_field.is_primary, "Array side should not be primary");

        // Non-array side should be auto-set to primary
        let author_field = posts.field_by_name("author").unwrap();
        assert!(
            author_field.is_primary,
            "Non-array side should auto-become primary"
        );

        // The _id field should also be primary
        let author_id = posts.field_by_name("author_id").unwrap();
        assert!(author_id.is_primary, "_id field should also be primary");
    }

    #[test]
    fn test_multiple_relations_same_collections() {
        // Post has both author and reviewer pointing to users
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author")
                .as_primary(),
            FieldDescription::new("4", "reviewer", FieldKind::relation("users", false))
                .with_relation_name("post_reviewer")
                .as_primary(),
        ];

        let mut coll = CollectionVersion::new("posts", "v1", "coll-posts", posts_fields);

        let mut counter = 100;
        coll.add_relation_id_fields(|| {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        // Both relations should get _id fields
        let author_id = coll.field_by_name("author_id");
        let reviewer_id = coll.field_by_name("reviewer_id");

        assert!(author_id.is_some(), "author_id should be generated");
        assert!(reviewer_id.is_some(), "reviewer_id should be generated");

        // Each _id should have correct relation_name
        assert_eq!(
            author_id.unwrap().relation_name,
            Some("post_author".to_string())
        );
        assert_eq!(
            reviewer_id.unwrap().relation_name,
            Some("post_reviewer".to_string())
        );

        // Should have 6 fields total: _docID, title, author, author_id, reviewer, reviewer_id
        assert_eq!(coll.fields.len(), 6);
    }

    #[test]
    fn test_self_referential_relation() {
        // Node with parent (non-array) and children (array)
        let node_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // Non-array self-ref - should get _id field (empty relative_id = same collection)
            FieldDescription::new("3", "parent", FieldKind::self_ref("", false))
                .with_relation_name("node_hierarchy"),
            // Array self-ref - should NOT get _id field
            FieldDescription::new("4", "children", FieldKind::self_ref("", true))
                .with_relation_name("node_hierarchy"),
        ];

        let mut coll = CollectionVersion::new("nodes", "v1", "coll-nodes", node_fields);

        let mut counter = 100;
        coll.add_relation_id_fields(|| {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        // parent should get parent_id
        let parent_id = coll.field_by_name("parent_id");
        assert!(parent_id.is_some(), "parent_id should be generated");
        assert_eq!(
            parent_id.unwrap().relation_name,
            Some("node_hierarchy".to_string())
        );

        // children should NOT get children_id
        assert!(
            coll.field_by_name("children_id").is_none(),
            "children_id should not be generated for array"
        );

        // Should have 5 fields: _docID, name, parent, parent_id, children
        assert_eq!(coll.fields.len(), 5);
    }

    #[test]
    fn test_finalize_orphaned_relation() {
        // Post has author pointing to nonexistent "users" collection
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("user_posts"),
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "posts".to_string(),
            CollectionVersion::new("posts", "v1", "coll-posts", posts_fields),
        );
        // Note: "users" collection intentionally NOT added

        let mut counter = 100;
        CollectionVersion::finalize_relations(&mut collections, || {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        let posts = collections.get("posts").unwrap();

        // Should still add author_id field even though target doesn't exist
        // (validation catches missing target separately)
        let author_id = posts.field_by_name("author_id");
        assert!(
            author_id.is_some(),
            "author_id should be generated even for orphaned relation"
        );
    }

    #[test]
    fn test_relation_id_field_position() {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("users", false))
                .with_relation_name("user_posts"),
            FieldDescription::new("4", "tags", FieldKind::string_array()),
        ];
        let mut coll = CollectionVersion::new("posts", "v1", "coll-posts", fields);

        coll.add_relation_id_fields(|| "99".to_string()).unwrap();

        // Find positions
        let author_idx = coll.fields.iter().position(|f| f.name == "author").unwrap();
        let author_id_idx = coll
            .fields
            .iter()
            .position(|f| f.name == "author_id")
            .unwrap();
        let tags_idx = coll.fields.iter().position(|f| f.name == "tags").unwrap();

        // author_id should be immediately after author
        assert_eq!(
            author_id_idx,
            author_idx + 1,
            "author_id should be immediately after author"
        );

        // tags should come after author_id
        assert!(tags_idx > author_id_idx, "tags should come after author_id");
    }

    #[test]
    fn test_finalize_relations_idempotent() {
        let users_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
                .with_relation_name("user_posts"),
        ];
        let posts_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
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

        // First finalization
        CollectionVersion::finalize_relations(&mut collections, || {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        let posts_after_first = collections.get("posts").unwrap().clone();
        let first_field_count = posts_after_first.fields.len();

        // Second finalization (should be idempotent)
        CollectionVersion::finalize_relations(&mut collections, || {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        let posts_after_second = collections.get("posts").unwrap();

        // Field count should not change
        assert_eq!(
            posts_after_second.fields.len(),
            first_field_count,
            "Field count should not change on second finalization"
        );

        // Should still have exactly one author_id
        let author_id_count = posts_after_second
            .fields
            .iter()
            .filter(|f| f.name == "author_id")
            .count();
        assert_eq!(
            author_id_count, 1,
            "Should have exactly one author_id field"
        );
    }

    #[test]
    fn test_one_to_one_both_non_array() {
        // One-to-one: Book <-> Author (neither is array)
        // The side with @primary should be primary
        let books_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
            FieldDescription::new("3", "author", FieldKind::relation("authors", false))
                .with_relation_name("book_author")
                .as_primary(),
        ];
        let authors_fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            // Secondary side - not primary
            FieldDescription::new("3", "published", FieldKind::relation("books", false))
                .with_relation_name("book_author"),
        ];

        let mut collections = BTreeMap::new();
        collections.insert(
            "books".to_string(),
            CollectionVersion::new("books", "v1", "coll-books", books_fields),
        );
        collections.insert(
            "authors".to_string(),
            CollectionVersion::new("authors", "v1", "coll-authors", authors_fields),
        );

        let mut counter = 100;
        CollectionVersion::finalize_relations(&mut collections, || {
            counter += 1;
            counter.to_string()
        })
        .unwrap();

        let books = collections.get("books").unwrap();
        let authors = collections.get("authors").unwrap();

        // Primary side should have _id field
        assert!(
            books.field_by_name("author_id").is_some(),
            "Primary side should have _id field"
        );

        // Secondary side should also have _id field (one-to-one)
        assert!(
            authors.field_by_name("published_id").is_some(),
            "Secondary side in one-to-one should also have _id field"
        );

        // Verify is_primary flags preserved
        assert!(books.field_by_name("author").unwrap().is_primary);
        assert!(!authors.field_by_name("published").unwrap().is_primary);
    }
}
