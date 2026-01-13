//! Field description for collection schemas

use crate::{CType, FieldKind, Result, SchemaError};
use serde::{Deserialize, Serialize};

/// Describes a field within a collection schema.
///
/// This matches Go's CollectionFieldDescription struct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDescription {
    /// Immutable field ID (stable across schema versions).
    /// Only fields persisted in the DAG will have a value.
    pub id: String,

    /// Human-readable field name. Must contain a valid value.
    pub name: String,

    /// The data type this field holds.
    pub kind: FieldKind,

    /// Which CRDT to use (defaults to LwwRegister).
    #[serde(default)]
    pub crdt_type: CType,

    /// Name of the relation (for relation fields).
    pub relation_name: Option<String>,

    /// For relations: which side holds the foreign key.
    #[serde(default)]
    pub is_primary: bool,

    /// Default value for this field.
    pub default_value: Option<serde_json::Value>,

    /// Size constraint for array fields.
    /// Has no effect on non-array fields.
    pub size: Option<usize>,
}

impl FieldDescription {
    /// Create a new field description
    pub fn new(id: impl Into<String>, name: impl Into<String>, kind: FieldKind) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            kind,
            crdt_type: CType::default(),
            relation_name: None,
            is_primary: false,
            default_value: None,
            size: None,
        }
    }

    /// Set the CRDT type
    pub fn with_crdt_type(mut self, crdt_type: CType) -> Self {
        self.crdt_type = crdt_type;
        self
    }

    /// Set the relation name
    pub fn with_relation_name(mut self, name: impl Into<String>) -> Self {
        self.relation_name = Some(name.into());
        self
    }

    /// Mark this side as primary (holds the FK)
    pub fn as_primary(mut self) -> Self {
        self.is_primary = true;
        self
    }

    /// Set a default value
    pub fn with_default(mut self, value: serde_json::Value) -> Self {
        self.default_value = Some(value);
        self
    }

    /// Set size constraint for array fields
    pub fn with_size(mut self, size: usize) -> Self {
        self.size = Some(size);
        self
    }

    /// Validate this field description
    pub fn validate(&self) -> Result<()> {
        if !self.crdt_type.is_compatible_with(&self.kind) {
            return Err(SchemaError::InvalidCrdtForKind {
                field_name: self.name.clone(),
                crdt_type: self.crdt_type.to_string(),
            });
        }

        if self.kind.is_relation() && self.relation_name.is_none() {
            return Err(SchemaError::MissingRequiredField(format!(
                "relation_name required for relation field '{}'",
                self.name
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_field() {
        let field = FieldDescription::new("1", "name", FieldKind::string());
        assert_eq!(field.id, "1");
        assert_eq!(field.name, "name");
        assert_eq!(field.kind, FieldKind::string());
        assert_eq!(field.crdt_type, CType::LwwRegister);
        assert!(!field.is_primary);
        assert!(field.size.is_none());
    }

    #[test]
    fn test_builder_pattern() {
        let field = FieldDescription::new("1", "count", FieldKind::int())
            .with_crdt_type(CType::PnCounter)
            .with_default(serde_json::json!(0));

        assert_eq!(field.crdt_type, CType::PnCounter);
        assert_eq!(field.default_value, Some(serde_json::json!(0)));
    }

    #[test]
    fn test_array_with_size() {
        let field = FieldDescription::new("1", "tags", FieldKind::string_array()).with_size(10);

        assert_eq!(field.size, Some(10));
        assert!(field.kind.is_array());
    }

    #[test]
    fn test_relation_field() {
        let field = FieldDescription::new("1", "author", FieldKind::relation("users", false))
            .with_relation_name("author_posts")
            .as_primary();

        assert!(field.is_primary);
        assert_eq!(field.relation_name, Some("author_posts".into()));
    }

    #[test]
    fn test_validate_counter_on_string_fails() {
        let field = FieldDescription::new("1", "title", FieldKind::string())
            .with_crdt_type(CType::PnCounter);

        let result = field.validate();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SchemaError::InvalidCrdtForKind { .. }));
    }

    #[test]
    fn test_validate_counter_on_int_succeeds() {
        let field =
            FieldDescription::new("1", "count", FieldKind::int()).with_crdt_type(CType::PnCounter);

        assert!(field.validate().is_ok());
    }

    #[test]
    fn test_validate_relation_without_name_fails() {
        let field = FieldDescription::new("1", "author", FieldKind::relation("users", false));

        let result = field.validate();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SchemaError::MissingRequiredField(_)
        ));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let field = FieldDescription::new("1", "name", FieldKind::string())
            .with_crdt_type(CType::LwwRegister)
            .with_default(serde_json::json!(""))
            .with_size(100);

        let json = serde_json::to_string(&field).unwrap();
        let parsed: FieldDescription = serde_json::from_str(&json).unwrap();
        assert_eq!(field, parsed);
    }
}
