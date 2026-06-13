//! Field description for collection schemas

use crate::{CType, FieldKind, Result, SchemaError};
use serde::{Deserialize, Serialize};

/// Default function for serde to return CType::None (matches Go's zero value).
fn default_ctype_none() -> CType {
    CType::None
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Describes a field within a collection schema.
///
/// This matches Go's CollectionFieldDescription struct.
/// Field names use serde rename to match Go's JSON format (PascalCase).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDescription {
    /// Immutable field ID (stable across schema versions).
    /// Only fields persisted in the DAG will have a value.
    #[serde(rename = "FieldID", default)]
    pub id: String,

    /// Human-readable field name. Must contain a valid value.
    #[serde(rename = "Name", default)]
    pub name: String,

    /// The data type this field holds.
    #[serde(rename = "Kind", default)]
    pub kind: FieldKind,

    /// Which CRDT to use.
    /// Go uses "Typ" as the JSON key.
    /// Defaults to CType::None to match Go's zero value behavior when deserializing.
    #[serde(rename = "Typ", default = "default_ctype_none")]
    pub crdt_type: CType,

    /// Name of the relation (for relation fields).
    /// Go serializes this as null when None, so we include it always.
    #[serde(rename = "RelationName", default)]
    pub relation_name: Option<String>,

    /// For relations: which side holds the foreign key.
    #[serde(rename = "IsPrimary", default)]
    pub is_primary: bool,

    /// Default value for this field.
    /// Go serializes this as null when None, so we include it always.
    #[serde(rename = "DefaultValue", default)]
    pub default_value: Option<serde_json::Value>,

    /// Size constraint for array fields.
    /// Has no effect on non-array fields.
    /// Go uses int with 0 meaning no constraint.
    #[serde(rename = "Size", default)]
    pub size: usize,

    /// Whether the field is write-once after document creation.
    #[serde(rename = "Immutable", default, skip_serializing_if = "is_false")]
    pub immutable: bool,
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
            size: 0, // 0 means no constraint (matches Go)
            immutable: false,
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

    /// Set size constraint for array fields (0 means no constraint)
    pub fn with_size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }

    /// Mark the field as immutable after document creation.
    pub fn as_immutable(mut self) -> Self {
        self.immutable = true;
        self
    }

    /// Returns true if this is a secondary (non-primary) relation field.
    ///
    /// Secondary relation fields are local-only and do not get saved in the blockstore.
    /// This matches Go DefraDB behavior where:
    /// ```go
    /// if new.RelationName.HasValue() && !new.IsPrimary {
    ///     // secondary fields are local-only
    ///     return nil, false, nil
    /// }
    /// ```
    pub fn is_secondary_relation(&self) -> bool {
        self.relation_name.is_some() && !self.is_primary
    }

    /// Validate this field description
    pub fn validate(&self) -> Result<()> {
        if !self.crdt_type.is_compatible_with(&self.kind) {
            return Err(SchemaError::InvalidCrdtForKind {
                crdt_type: self.crdt_type.to_string().to_lowercase(),
                field_kind: self.kind.graphql_type_name().to_string(),
            });
        }

        if self.immutable {
            if self.crdt_type != CType::LwwRegister {
                return Err(SchemaError::InvalidImmutableField {
                    field_name: self.name.clone(),
                    reason: "only LWW register fields can be immutable".to_string(),
                });
            }
            if !self.kind.is_scalar() {
                return Err(SchemaError::InvalidImmutableField {
                    field_name: self.name.clone(),
                    reason: "only scalar fields can be immutable".to_string(),
                });
            }
        }

        // Float counter merge is not order-independent: IEEE-754 addition is not
        // associative, so replicas can converge to different values. The behavior
        // matches Go (parity), so this is a warning, not an error. See NumericKind
        // in crates/crdt and `float_add_not_assoc` in proofs/lean.
        if self.crdt_type.is_counter() && self.kind.is_float() {
            tracing::warn!(
                field = %self.name,
                kind = %self.kind.graphql_type_name(),
                "float counter field is not convergence-safe across replicas \
                 (IEEE-754 addition is not associative); prefer an integer counter"
            );
        }

        if self.kind.is_relation() && self.relation_name.is_none() {
            return Err(SchemaError::MissingRequiredField(format!(
                "relation name cannot be empty. Field: {}",
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
        assert_eq!(field.size, 0); // 0 means no constraint
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

        assert_eq!(field.size, 10);
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

    #[test]
    fn test_immutable_scalar_lww_field_validates_and_serializes() {
        let field = FieldDescription::new("1", "agent_did", FieldKind::string()).as_immutable();

        field.validate().unwrap();
        let json = serde_json::to_string(&field).unwrap();
        assert!(
            json.contains(r#""Immutable":true"#),
            "immutable fields must persist the Rust extension metadata"
        );
        let parsed: FieldDescription = serde_json::from_str(&json).unwrap();
        assert!(parsed.immutable);
    }

    #[test]
    fn test_non_immutable_field_omits_rust_extension() {
        let field = FieldDescription::new("1", "agent_did", FieldKind::string());

        let json = serde_json::to_string(&field).unwrap();
        assert!(
            !json.contains("Immutable"),
            "false immutable metadata must be omitted for Go-compatible schema JSON"
        );
    }

    #[test]
    fn test_immutable_counter_field_fails_validation() {
        let field = FieldDescription::new("1", "score", FieldKind::int())
            .with_crdt_type(CType::PnCounter)
            .as_immutable();

        let result = field.validate();
        assert!(matches!(
            result,
            Err(SchemaError::InvalidImmutableField { .. })
        ));
    }

    #[test]
    fn test_immutable_array_field_fails_validation() {
        let field = FieldDescription::new("1", "tags", FieldKind::string_array()).as_immutable();

        let result = field.validate();
        assert!(matches!(
            result,
            Err(SchemaError::InvalidImmutableField { .. })
        ));
    }

    #[test]
    fn test_is_secondary_relation() {
        // Primary relation field - NOT secondary
        let primary = FieldDescription::new("1", "author", FieldKind::relation("users", false))
            .with_relation_name("author_posts")
            .as_primary();
        assert!(!primary.is_secondary_relation());

        // Secondary relation field (has relation_name but not primary)
        let secondary = FieldDescription::new("2", "posts", FieldKind::relation("posts", true))
            .with_relation_name("author_posts");
        assert!(secondary.is_secondary_relation());

        // Non-relation field - NOT secondary
        let scalar = FieldDescription::new("3", "name", FieldKind::string());
        assert!(!scalar.is_secondary_relation());

        // Relation field without relation_name - NOT secondary (invalid state, but test anyway)
        let no_name = FieldDescription::new("4", "ref", FieldKind::relation("other", false));
        assert!(!no_name.is_secondary_relation());
    }
}
