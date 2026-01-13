//! Collection version definitions

use crate::{FieldDescription, FieldKind, Result, SchemaError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// A versioned collection schema.
///
/// This matches Go's CollectionVersion struct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionVersion {
    /// Human-readable collection name
    pub name: String,
    /// Content hash of this version (immutable)
    pub version_id: String,
    /// Stable collection ID across versions
    pub collection_id: String,
    /// Fields in this collection
    pub fields: Vec<FieldDescription>,
    /// Whether this is the active version
    #[serde(default = "default_active")]
    pub is_active: bool,
}

fn default_active() -> bool {
    true
}

impl CollectionVersion {
    /// Create a new collection version
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
            fields,
            is_active: true,
        }
    }

    /// Get a field by name
    pub fn field_by_name(&self, name: &str) -> Option<&FieldDescription> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get a field by ID
    pub fn field_by_id(&self, id: &str) -> Option<&FieldDescription> {
        self.fields.iter().find(|f| f.id == id)
    }

    /// Get a field by relation name (matches Go's GetFieldByRelation)
    pub fn field_by_relation(&self, relation_name: &str) -> Option<&FieldDescription> {
        self.fields
            .iter()
            .find(|f| f.relation_name.as_deref() == Some(relation_name))
    }

    /// Get all relation fields
    pub fn relation_fields(&self) -> impl Iterator<Item = &FieldDescription> {
        self.fields.iter().filter(|f| f.kind.is_relation())
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
    fn test_field_by_relation() {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "author", FieldKind::relation("users", false))
                .with_relation_name("post_author"),
        ];
        let coll = CollectionVersion::new("posts", "v1", "coll-1", fields);

        let field = coll.field_by_relation("post_author").unwrap();
        assert_eq!(field.name, "author");
        assert!(coll.field_by_relation("nonexistent").is_none());
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
}
