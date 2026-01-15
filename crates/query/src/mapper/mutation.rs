//! Mutation types for GraphQL mutation operations
//!
//! Defines types for CREATE, UPDATE, and DELETE mutations following
//! Go DefraDB's mutation patterns.

use serde_json::Value as JsonValue;
use std::collections::HashMap;

use super::{Filter, Requestable};
use crate::document::DocumentMapping;

/// Type of mutation operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationType {
    /// Create new documents
    Create,
    /// Update existing documents
    Update,
    /// Delete existing documents
    Delete,
    /// Create or update documents (insert if not exists, update if exists)
    Upsert,
}

impl MutationType {
    /// Parse mutation type from operation prefix.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// MutationType::from_prefix("create") // Some(Create)
    /// MutationType::from_prefix("update") // Some(Update)
    /// MutationType::from_prefix("delete") // Some(Delete)
    /// ```
    pub fn from_prefix(prefix: &str) -> Option<Self> {
        match prefix.to_lowercase().as_str() {
            "create" => Some(Self::Create),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            "upsert" => Some(Self::Upsert),
            _ => None,
        }
    }

    /// Get the operation prefix string.
    pub fn as_prefix(&self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Upsert => "upsert",
        }
    }
}

/// A GraphQL mutation operation.
///
/// Represents a single mutation like `create_Users(input: [...]) { ... }`.
#[derive(Debug, Clone)]
pub struct Mutation {
    /// Type of mutation (create, update, delete)
    pub mutation_type: MutationType,
    /// Target collection name
    pub collection_name: String,
    /// For CREATE: Array of documents to create (each is a field-value map)
    pub create_input: Vec<HashMap<String, JsonValue>>,
    /// For UPDATE: Fields to update (patch)
    pub update_input: HashMap<String, JsonValue>,
    /// For UPDATE/DELETE: Specific document IDs to target
    pub doc_ids: Option<Vec<String>>,
    /// For UPDATE/DELETE: Filter to find documents to target
    pub filter: Option<Filter>,
    /// Fields to return after mutation
    pub fields: Vec<Requestable>,
    /// Document mapping for result fields
    pub document_mapping: DocumentMapping,
}

impl Mutation {
    /// Create a new CREATE mutation.
    pub fn create(collection_name: impl Into<String>) -> Self {
        Self {
            mutation_type: MutationType::Create,
            collection_name: collection_name.into(),
            create_input: Vec::new(),
            update_input: HashMap::new(),
            doc_ids: None,
            filter: None,
            fields: Vec::new(),
            document_mapping: DocumentMapping::new(),
        }
    }

    /// Create a new UPDATE mutation.
    pub fn update(collection_name: impl Into<String>) -> Self {
        Self {
            mutation_type: MutationType::Update,
            collection_name: collection_name.into(),
            create_input: Vec::new(),
            update_input: HashMap::new(),
            doc_ids: None,
            filter: None,
            fields: Vec::new(),
            document_mapping: DocumentMapping::new(),
        }
    }

    /// Create a new DELETE mutation.
    pub fn delete(collection_name: impl Into<String>) -> Self {
        Self {
            mutation_type: MutationType::Delete,
            collection_name: collection_name.into(),
            create_input: Vec::new(),
            update_input: HashMap::new(),
            doc_ids: None,
            filter: None,
            fields: Vec::new(),
            document_mapping: DocumentMapping::new(),
        }
    }

    /// Create a new UPSERT mutation.
    pub fn upsert(collection_name: impl Into<String>) -> Self {
        Self {
            mutation_type: MutationType::Upsert,
            collection_name: collection_name.into(),
            create_input: Vec::new(),
            update_input: HashMap::new(),
            doc_ids: None,
            filter: None,
            fields: Vec::new(),
            document_mapping: DocumentMapping::new(),
        }
    }

    /// Set create input (array of documents to create).
    pub fn with_create_input(mut self, input: Vec<HashMap<String, JsonValue>>) -> Self {
        self.create_input = input;
        self
    }

    /// Set update input (fields to update).
    pub fn with_update_input(mut self, input: HashMap<String, JsonValue>) -> Self {
        self.update_input = input;
        self
    }

    /// Set document IDs to target.
    pub fn with_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        self.doc_ids = Some(doc_ids);
        self
    }

    /// Set filter to find documents.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set fields to return.
    pub fn with_fields(mut self, fields: Vec<Requestable>) -> Self {
        self.fields = fields;
        self
    }

    /// Set document mapping.
    pub fn with_document_mapping(mut self, mapping: DocumentMapping) -> Self {
        self.document_mapping = mapping;
        self
    }

    /// Get all requested simple fields (not nested selects or aggregates).
    pub fn requested_fields(&self) -> Vec<&super::Field> {
        self.fields
            .iter()
            .filter_map(|r| match r {
                Requestable::Field(f) => Some(f),
                _ => None,
            })
            .collect()
    }
}

/// Parse a mutation field name into (operation_type, collection_name).
///
/// # Examples
///
/// ```ignore
/// parse_mutation_name("create_Users") // Ok((Create, "Users"))
/// parse_mutation_name("update_my_collection") // Ok((Update, "my_collection"))
/// parse_mutation_name("delete_Posts") // Ok((Delete, "Posts"))
/// parse_mutation_name("Users") // Err("...")
/// ```
pub fn parse_mutation_name(name: &str) -> Result<(MutationType, String), String> {
    // Find the first underscore
    let underscore_pos = name.find('_').ok_or_else(|| {
        format!(
            "Invalid mutation name '{}': expected format 'operation_collection' (e.g., 'create_Users')",
            name
        )
    })?;

    let prefix = &name[..underscore_pos];
    let collection = &name[underscore_pos + 1..];

    if collection.is_empty() {
        return Err(format!(
            "Invalid mutation name '{}': collection name cannot be empty",
            name
        ));
    }

    let mutation_type = MutationType::from_prefix(prefix).ok_or_else(|| {
        format!(
            "Invalid mutation prefix '{}': expected 'create', 'update', 'delete', or 'upsert'",
            prefix
        )
    })?;

    Ok((mutation_type, collection.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mutation_type_from_prefix() {
        assert_eq!(
            MutationType::from_prefix("create"),
            Some(MutationType::Create)
        );
        assert_eq!(
            MutationType::from_prefix("CREATE"),
            Some(MutationType::Create)
        );
        assert_eq!(
            MutationType::from_prefix("update"),
            Some(MutationType::Update)
        );
        assert_eq!(
            MutationType::from_prefix("delete"),
            Some(MutationType::Delete)
        );
        assert_eq!(
            MutationType::from_prefix("upsert"),
            Some(MutationType::Upsert)
        );
        assert_eq!(
            MutationType::from_prefix("UPSERT"),
            Some(MutationType::Upsert)
        );
        assert_eq!(MutationType::from_prefix("invalid"), None);
    }

    #[test]
    fn test_parse_mutation_name() {
        let (typ, name) = parse_mutation_name("create_Users").unwrap();
        assert_eq!(typ, MutationType::Create);
        assert_eq!(name, "Users");

        let (typ, name) = parse_mutation_name("update_my_collection").unwrap();
        assert_eq!(typ, MutationType::Update);
        assert_eq!(name, "my_collection");

        let (typ, name) = parse_mutation_name("delete_Posts").unwrap();
        assert_eq!(typ, MutationType::Delete);
        assert_eq!(name, "Posts");
    }

    #[test]
    fn test_parse_mutation_name_errors() {
        assert!(parse_mutation_name("Users").is_err());
        assert!(parse_mutation_name("create_").is_err());
        assert!(parse_mutation_name("invalid_Users").is_err());
    }

    #[test]
    fn test_mutation_builders() {
        let create = Mutation::create("Users").with_create_input(vec![{
            let mut m = HashMap::new();
            m.insert("name".to_string(), JsonValue::String("Alice".to_string()));
            m
        }]);

        assert_eq!(create.mutation_type, MutationType::Create);
        assert_eq!(create.collection_name, "Users");
        assert_eq!(create.create_input.len(), 1);

        let update = Mutation::update("Users")
            .with_doc_ids(vec!["bae-123".to_string()])
            .with_update_input({
                let mut m = HashMap::new();
                m.insert(
                    "email".to_string(),
                    JsonValue::String("new@example.com".to_string()),
                );
                m
            });

        assert_eq!(update.mutation_type, MutationType::Update);
        assert!(update.doc_ids.is_some());

        let delete = Mutation::delete("Users").with_doc_ids(vec!["bae-456".to_string()]);

        assert_eq!(delete.mutation_type, MutationType::Delete);
    }
}
