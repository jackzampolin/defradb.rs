//! Collection version definitions

use crate::{
    CollectionSetDescription, CollectionSource, EncryptedIndexDescription, FieldDescription,
    FieldKind, IndexDescription, PolicyDescription, QuerySource, Result, SchemaError,
    VectorEmbeddingDescription,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{HashMap, HashSet};

/// Sentinel collection ID for placeholder versions whose real collection is unknown.
pub const ORPHAN_COLLECTION_ID: &str = "OrphanCollectionID";

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
    #[serde(rename = "Name", default)]
    pub name: String,

    /// Content hash of this version (immutable)
    #[serde(rename = "VersionID", default)]
    pub version_id: String,

    /// Stable collection ID across versions
    #[serde(rename = "CollectionID", default)]
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
    #[serde(
        rename = "Fields",
        default,
        deserialize_with = "deserialize_null_as_empty_vec"
    )]
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

    /// Sequential short ID for storage prefixes (matches Go's monotonic counter).
    ///
    /// Not serialized — stored separately in system store at /collection/shortID/{collection_id}.
    /// Set during collection creation or loaded from store. 0 means not yet assigned.
    #[serde(skip)]
    pub root_id: u32,
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
            root_id: 0,
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
    /// - All matching fields are `DocID` scalars (foreign key `_id` fields like `author_id`
    ///   are filtered out to return only actual relation fields, not their backing scalars)
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
                // Filter out _id backing fields (e.g., author_id) to return only actual
                // relation fields, not their auto-generated foreign key scalars
                && !matches!(f.kind, FieldKind::Scalar(crate::ScalarKind::DocID))
        })
    }

    /// Get all relation fields
    pub fn relation_fields(&self) -> impl Iterator<Item = &FieldDescription> {
        self.fields.iter().filter(|f| f.kind.is_relation())
    }

    /// Validate the collection schema
    pub fn validate(&self) -> Result<()> {
        self.validate_no_duplicate_names()?;
        self.validate_fields()?;
        self.validate_policy()?;
        Ok(())
    }

    fn validate_policy(&self) -> Result<()> {
        if let Some(ref policy) = self.policy {
            policy.validate()?;
        }
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
