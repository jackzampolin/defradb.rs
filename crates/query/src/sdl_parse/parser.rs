//! SDL parser implementation
//!
//! Parses GraphQL Schema Definition Language (SDL) into DefraDB CollectionVersion schemas.
//! Designed for compatibility with Go DefraDB's SDL parsing behavior.

use crate::error::{QueryError, Result};
use graphql_parser::schema::{
    Definition, Directive, Document, Field, ObjectType, Type, TypeDefinition,
};
use schema::{
    CType, CollectionVersion, FieldDescription, FieldKind, IndexDescription,
    IndexedFieldDescription, ScalarKind,
};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Parsed directive information from a field
#[derive(Debug, Default, Clone)]
pub struct ParsedDirectives {
    /// Whether this field is the primary side of a relation
    pub is_primary: bool,
    /// CRDT type override (default is LwwRegister)
    pub crdt_type: Option<CType>,
    /// Index configuration
    pub index: Option<IndexConfig>,
    /// Explicit relation name from @relation directive
    pub relation_name: Option<String>,
    /// Default value from @default directive
    pub default_value: Option<serde_json::Value>,
    /// Array size constraint from @constraints directive
    pub size_constraint: Option<usize>,
}

/// Index configuration from @index directive
#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub name: Option<String>,
    pub unique: bool,
    pub direction: IndexDirection,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum IndexDirection {
    #[default]
    Asc,
    Desc,
}

/// SDL parser for DefraDB schemas
pub struct SdlParser<'a> {
    sdl: &'a str,
    /// Parsed type definitions by name
    type_defs: HashMap<String, ParsedTypeDef>,
}

#[derive(Debug)]
struct ParsedTypeDef {
    name: String,
    fields: Vec<ParsedField>,
    directives: ParsedTypeDirectives,
}

/// Type-level directives
#[derive(Debug, Default)]
struct ParsedTypeDirectives {
    indexes: Vec<CompositeIndex>,
    is_materialized: bool,
    is_branchable: bool,
}

#[derive(Debug)]
struct CompositeIndex {
    fields: Vec<String>,
    name: Option<String>,
    unique: bool,
}

#[derive(Debug)]
struct ParsedField {
    name: String,
    field_type: ParsedType,
    directives: ParsedDirectives,
}

#[derive(Debug)]
struct ParsedType {
    base_type: String,
    is_list: bool,
    is_non_null: bool,
    element_non_null: bool,
}

impl<'a> SdlParser<'a> {
    pub fn new(sdl: &'a str) -> Self {
        Self {
            sdl,
            type_defs: HashMap::new(),
        }
    }

    /// Parse the SDL and return collection versions
    pub fn parse(&mut self) -> Result<Vec<CollectionVersion>> {
        // Handle empty or whitespace-only input
        if self.sdl.trim().is_empty() {
            return Ok(Vec::new());
        }

        let doc: Document<'_, String> =
            graphql_parser::parse_schema(self.sdl).map_err(|e| QueryError::parse(e.to_string()))?;

        // First pass: collect all type definitions
        for def in &doc.definitions {
            if let Definition::TypeDefinition(TypeDefinition::Object(obj)) = def {
                self.parse_object_type(obj)?;
            }
        }

        // Second pass: resolve relations and build CollectionVersions
        self.build_collections()
    }

    fn parse_object_type(&mut self, obj: &ObjectType<'_, String>) -> Result<()> {
        let name = obj.name.clone();
        let mut fields = Vec::new();

        for field in &obj.fields {
            let parsed_field = self.parse_field(field)?;
            fields.push(parsed_field);
        }

        let type_directives = self.parse_type_directives(&obj.directives)?;

        self.type_defs.insert(
            name.clone(),
            ParsedTypeDef {
                name,
                fields,
                directives: type_directives,
            },
        );

        Ok(())
    }

    fn parse_field(&self, field: &Field<'_, String>) -> Result<ParsedField> {
        let field_type = parse_graphql_type(&field.field_type);
        let directives = self.parse_field_directives(&field.directives)?;

        Ok(ParsedField {
            name: field.name.clone(),
            field_type,
            directives,
        })
    }

    fn parse_field_directives(
        &self,
        directives: &[Directive<'_, String>],
    ) -> Result<ParsedDirectives> {
        let mut result = ParsedDirectives::default();

        for directive in directives {
            match directive.name.as_str() {
                "primary" => {
                    result.is_primary = true;
                }
                "crdt" => {
                    result.crdt_type = Some(self.parse_crdt_directive(directive)?);
                }
                "index" => {
                    result.index = Some(self.parse_index_directive(directive)?);
                }
                "relation" => {
                    result.relation_name = self.get_directive_string(directive, "name");
                }
                "default" => {
                    result.default_value = Some(self.parse_default_directive(directive)?);
                }
                "constraints" => {
                    if let Some(size) = self.get_directive_int(directive, "size") {
                        result.size_constraint = Some(size as usize);
                    }
                }
                _ => {
                    // Unknown directives are ignored for forward compatibility
                }
            }
        }

        Ok(result)
    }

    fn parse_type_directives(
        &self,
        directives: &[Directive<'_, String>],
    ) -> Result<ParsedTypeDirectives> {
        let mut result = ParsedTypeDirectives::default();

        for directive in directives {
            match directive.name.as_str() {
                "index" => {
                    let fields = self.get_directive_string_list(directive, "fields");
                    let name = self.get_directive_string(directive, "name");
                    let unique = self
                        .get_directive_bool(directive, "unique")
                        .unwrap_or(false);

                    if !fields.is_empty() {
                        result.indexes.push(CompositeIndex {
                            fields,
                            name,
                            unique,
                        });
                    }
                }
                "materialized" => {
                    // @materialized or @materialized(if: true)
                    result.is_materialized =
                        self.get_directive_bool(directive, "if").unwrap_or(true);
                }
                "branchable" => {
                    // @branchable or @branchable(if: true)
                    result.is_branchable =
                        self.get_directive_bool(directive, "if").unwrap_or(true);
                }
                _ => {}
            }
        }

        Ok(result)
    }

    fn parse_crdt_directive(&self, directive: &Directive<'_, String>) -> Result<CType> {
        let type_name = self
            .get_directive_string(directive, "type")
            .ok_or_else(|| QueryError::parse("@crdt directive requires 'type' argument"))?;

        match type_name.to_lowercase().as_str() {
            "lwwregister" | "lww" | "lww_register" => Ok(CType::LwwRegister),
            "pncounter" | "counter" | "pn_counter" => Ok(CType::PnCounter),
            "pcounter" | "p_counter" => Ok(CType::PCounter),
            other => Err(QueryError::parse(format!("unknown CRDT type: {}", other))),
        }
    }

    fn parse_index_directive(&self, directive: &Directive<'_, String>) -> Result<IndexConfig> {
        let name = self.get_directive_string(directive, "name");
        let unique = self
            .get_directive_bool(directive, "unique")
            .unwrap_or(false);
        let direction = match self.get_directive_string(directive, "direction").as_deref() {
            Some("DESC") | Some("desc") | Some("Descending") => IndexDirection::Desc,
            _ => IndexDirection::Asc,
        };

        Ok(IndexConfig {
            name,
            unique,
            direction,
        })
    }

    fn parse_default_directive(
        &self,
        directive: &Directive<'_, String>,
    ) -> Result<serde_json::Value> {
        // Go supports: string, bool, int, float, float32, float64, dateTime, json, blob
        // We check each argument type
        for (name, value) in &directive.arguments {
            match name.as_str() {
                "string" | "value" => {
                    if let graphql_parser::schema::Value::String(s) = value {
                        return Ok(serde_json::Value::String(s.clone()));
                    }
                }
                "bool" => {
                    if let graphql_parser::schema::Value::Boolean(b) = value {
                        return Ok(serde_json::Value::Bool(*b));
                    }
                }
                "int" => {
                    if let graphql_parser::schema::Value::Int(n) = value {
                        return Ok(serde_json::Value::Number(
                            serde_json::Number::from(n.as_i64().unwrap_or(0)),
                        ));
                    }
                }
                "float" | "float64" => {
                    if let graphql_parser::schema::Value::Float(f) = value {
                        if let Some(n) = serde_json::Number::from_f64(*f) {
                            return Ok(serde_json::Value::Number(n));
                        }
                    }
                }
                "json" => {
                    if let graphql_parser::schema::Value::String(s) = value {
                        if let Ok(parsed) = serde_json::from_str(s) {
                            return Ok(parsed);
                        }
                    }
                }
                _ => {}
            }
        }

        Err(QueryError::parse(
            "@default directive requires a value argument",
        ))
    }

    fn get_directive_string(
        &self,
        directive: &Directive<'_, String>,
        arg_name: &str,
    ) -> Option<String> {
        directive.arguments.iter().find_map(|(name, value)| {
            if name == arg_name {
                match value {
                    graphql_parser::schema::Value::String(s) => Some(s.clone()),
                    graphql_parser::schema::Value::Enum(s) => Some(s.clone()),
                    _ => None,
                }
            } else {
                None
            }
        })
    }

    fn get_directive_bool(
        &self,
        directive: &Directive<'_, String>,
        arg_name: &str,
    ) -> Option<bool> {
        directive.arguments.iter().find_map(|(name, value)| {
            if name == arg_name {
                match value {
                    graphql_parser::schema::Value::Boolean(b) => Some(*b),
                    _ => None,
                }
            } else {
                None
            }
        })
    }

    fn get_directive_int(&self, directive: &Directive<'_, String>, arg_name: &str) -> Option<i64> {
        directive.arguments.iter().find_map(|(name, value)| {
            if name == arg_name {
                match value {
                    graphql_parser::schema::Value::Int(n) => n.as_i64(),
                    _ => None,
                }
            } else {
                None
            }
        })
    }

    fn get_directive_string_list(
        &self,
        directive: &Directive<'_, String>,
        arg_name: &str,
    ) -> Vec<String> {
        directive
            .arguments
            .iter()
            .find_map(|(name, value)| {
                if name == arg_name {
                    match value {
                        graphql_parser::schema::Value::List(items) => Some(
                            items
                                .iter()
                                .filter_map(|v| match v {
                                    graphql_parser::schema::Value::String(s) => Some(s.clone()),
                                    graphql_parser::schema::Value::Enum(s) => Some(s.clone()),
                                    _ => None,
                                })
                                .collect(),
                        ),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    fn build_collections(&self) -> Result<Vec<CollectionVersion>> {
        let mut collections = Vec::new();

        // Build collection names set for relation detection
        let type_names: std::collections::HashSet<_> = self.type_defs.keys().cloned().collect();

        for type_def in self.type_defs.values() {
            let collection = self.build_collection(type_def, &type_names)?;
            collections.push(collection);
        }

        Ok(collections)
    }

    fn build_collection(
        &self,
        type_def: &ParsedTypeDef,
        type_names: &std::collections::HashSet<String>,
    ) -> Result<CollectionVersion> {
        let collection_id = generate_collection_id(&type_def.name);
        let mut fields = Vec::new();
        let mut indexes = Vec::new();
        let mut field_id_counter = 1u32;

        // Add implicit _docID field
        let doc_id_field_id = generate_field_id(&type_def.name, "_docID");
        fields.push(FieldDescription::new(
            &doc_id_field_id,
            "_docID",
            FieldKind::doc_id(),
        ));
        field_id_counter += 1;

        // Process user-defined fields
        for parsed_field in &type_def.fields {
            let field_id = generate_field_id(&type_def.name, &parsed_field.name);
            let kind =
                self.resolve_field_kind(&parsed_field.field_type, type_names, &type_def.name)?;

            let mut field = FieldDescription::new(&field_id, &parsed_field.name, kind.clone());

            // Apply directives
            if parsed_field.directives.is_primary {
                field = field.as_primary();
            }
            if let Some(crdt_type) = parsed_field.directives.crdt_type {
                field = field.with_crdt_type(crdt_type);
            }
            if let Some(ref default_value) = parsed_field.directives.default_value {
                field = field.with_default(default_value.clone());
            }
            if let Some(size) = parsed_field.directives.size_constraint {
                field = field.with_size(size);
            }

            // Set relation name - use explicit @relation(name:) if provided, otherwise auto-generate
            if kind.is_relation() {
                let relation_name = parsed_field
                    .directives
                    .relation_name
                    .clone()
                    .unwrap_or_else(|| {
                        // Go uses lexicographic sort of type names for auto-generated relation names
                        generate_relation_name(
                            &type_def.name,
                            &parsed_field.name,
                            &parsed_field.field_type.base_type,
                        )
                    });
                field = field.with_relation_name(relation_name);
            }

            // Handle field-level @index directive
            if let Some(ref idx_config) = parsed_field.directives.index {
                let idx_name = idx_config
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{}_{}_idx", type_def.name, parsed_field.name));

                indexes.push(IndexDescription {
                    name: idx_name,
                    id: field_id_counter,
                    fields: vec![IndexedFieldDescription {
                        name: parsed_field.name.clone(),
                        descending: matches!(idx_config.direction, IndexDirection::Desc),
                    }],
                    unique: idx_config.unique,
                });
            }

            fields.push(field);
            field_id_counter += 1;
        }

        // Handle type-level @index directives (composite indexes)
        for composite_idx in &type_def.directives.indexes {
            let idx_name = composite_idx.name.clone().unwrap_or_else(|| {
                format!("{}_{}_idx", type_def.name, composite_idx.fields.join("_"))
            });

            let indexed_fields: Vec<IndexedFieldDescription> = composite_idx
                .fields
                .iter()
                .map(|f| IndexedFieldDescription {
                    name: f.clone(),
                    descending: false,
                })
                .collect();

            indexes.push(IndexDescription {
                name: idx_name,
                id: field_id_counter,
                fields: indexed_fields,
                unique: composite_idx.unique,
            });
            field_id_counter += 1;
        }

        // Generate version ID from content
        let version_id = generate_version_id(&type_def.name, &fields);

        let mut collection =
            CollectionVersion::new(&type_def.name, &version_id, &collection_id, fields);
        collection.indexes = indexes;
        collection.is_materialized = type_def.directives.is_materialized;
        collection.is_branchable = type_def.directives.is_branchable;

        Ok(collection)
    }

    fn resolve_field_kind(
        &self,
        parsed_type: &ParsedType,
        type_names: &std::collections::HashSet<String>,
        current_type: &str,
    ) -> Result<FieldKind> {
        let base = &parsed_type.base_type;

        // Check if it's a scalar type
        if let Some(scalar_kind) = graphql_to_scalar_kind(base) {
            if parsed_type.is_list {
                let array_kind = if parsed_type.element_non_null {
                    scalar_kind.to_array_kind()
                } else {
                    scalar_kind.to_nillable_array_kind()
                };

                return array_kind.map(FieldKind::ScalarArray).ok_or_else(|| {
                    QueryError::parse(format!("scalar type {} cannot be used in arrays", base))
                });
            }
            return Ok(FieldKind::Scalar(scalar_kind));
        }

        // Check if it's a self-reference
        if base == current_type || base == "Self" {
            return Ok(FieldKind::self_ref("", parsed_type.is_list));
        }

        // Check if it references another type in the schema
        if type_names.contains(base) {
            // This is a relation to another type
            // We use Named here because collection IDs aren't known yet
            // The relation will be resolved later during schema finalization
            return Ok(FieldKind::named(base, parsed_type.is_list));
        }

        // Unknown type - treat as named reference
        Ok(FieldKind::named(base, parsed_type.is_list))
    }
}

/// Parse a GraphQL type into our ParsedType representation
fn parse_graphql_type(ty: &Type<'_, String>) -> ParsedType {
    match ty {
        Type::NamedType(name) => ParsedType {
            base_type: name.clone(),
            is_list: false,
            is_non_null: false,
            element_non_null: false,
        },
        Type::NonNullType(inner) => {
            let mut parsed = parse_graphql_type(inner);
            parsed.is_non_null = true;
            parsed
        }
        Type::ListType(inner) => {
            let inner_parsed = parse_graphql_type(inner);
            ParsedType {
                base_type: inner_parsed.base_type,
                is_list: true,
                is_non_null: false,
                element_non_null: inner_parsed.is_non_null,
            }
        }
    }
}

/// Convert GraphQL scalar type name to FieldKind's ScalarKind
fn graphql_to_scalar_kind(name: &str) -> Option<ScalarKind> {
    match name {
        "String" => Some(ScalarKind::String),
        "Int" => Some(ScalarKind::Int),
        "Float" | "Float64" => Some(ScalarKind::Float64),
        "Float32" => Some(ScalarKind::Float32),
        "Boolean" => Some(ScalarKind::Bool),
        "ID" => Some(ScalarKind::DocID),
        "DateTime" => Some(ScalarKind::DateTime),
        "JSON" => Some(ScalarKind::Json),
        "Blob" => Some(ScalarKind::Blob),
        _ => None,
    }
}

/// Generate a deterministic collection ID from the type name
fn generate_collection_id(type_name: &str) -> String {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    type_name.hash(&mut hasher);
    format!("coll_{:x}", hasher.finish())
}

/// Generate a deterministic field ID from collection name and field name
fn generate_field_id(type_name: &str, field_name: &str) -> String {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    type_name.hash(&mut hasher);
    field_name.hash(&mut hasher);
    format!("field_{:x}", hasher.finish())
}

/// Generate a deterministic version ID from collection name and fields
fn generate_version_id(name: &str, fields: &[FieldDescription]) -> String {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    for field in fields {
        field.name.hash(&mut hasher);
        field.id.hash(&mut hasher);
    }
    format!("v{:x}", hasher.finish())
}

/// Generate a relation name following Go DefraDB conventions.
/// Go uses lexicographic sort of type names to create deterministic relation names.
fn generate_relation_name(from_type: &str, field_name: &str, to_type: &str) -> String {
    // Go convention: sort type names lexicographically for deterministic naming
    let (first, second) = if from_type.to_lowercase() < to_type.to_lowercase() {
        (from_type.to_lowercase(), to_type.to_lowercase())
    } else {
        (to_type.to_lowercase(), from_type.to_lowercase())
    };
    format!("{}_{}_{}", first, field_name, second)
}

/// Parse SDL string into CollectionVersion schemas
pub fn parse_sdl(sdl: &str) -> Result<Vec<CollectionVersion>> {
    let mut parser = SdlParser::new(sdl);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_type() {
        let sdl = r#"
            type User {
                name: String
                age: Int
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(collections.len(), 1);

        let user = &collections[0];
        assert_eq!(user.name, "User");
        // _docID + name + age = 3 fields
        assert_eq!(user.fields.len(), 3);

        let name_field = user.field_by_name("name").unwrap();
        assert_eq!(name_field.kind, FieldKind::string());

        let age_field = user.field_by_name("age").unwrap();
        assert_eq!(age_field.kind, FieldKind::int());
    }

    #[test]
    fn test_parse_non_null_type() {
        let sdl = r#"
            type Post {
                title: String!
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let post = &collections[0];

        let title_field = post.field_by_name("title").unwrap();
        // In DefraDB, all fields are nillable. Non-null is only used for array elements.
        assert_eq!(title_field.kind, FieldKind::string());
    }

    #[test]
    fn test_parse_array_type() {
        let sdl = r#"
            type User {
                tags: [String!]
                scores: [Int]
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        // [String!] -> non-nillable elements
        let tags = user.field_by_name("tags").unwrap();
        assert_eq!(tags.kind, FieldKind::string_array());

        // [Int] -> nillable elements
        let scores = user.field_by_name("scores").unwrap();
        assert_eq!(scores.kind, FieldKind::nillable_int_array());
    }

    #[test]
    fn test_parse_crdt_directive() {
        let sdl = r#"
            type Counter {
                value: Int @crdt(type: "pncounter")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let counter = &collections[0];

        let value = counter.field_by_name("value").unwrap();
        assert_eq!(value.crdt_type, CType::PnCounter);
    }

    #[test]
    fn test_parse_primary_directive() {
        let sdl = r#"
            type Post {
                author: User @primary
            }
            type User {
                name: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let post = collections.iter().find(|c| c.name == "Post").unwrap();

        let author = post.field_by_name("author").unwrap();
        assert!(author.is_primary);
    }

    #[test]
    fn test_parse_relation() {
        let sdl = r#"
            type User {
                name: String
                posts: [Post]
            }
            type Post {
                title: String
                author: User
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(collections.len(), 2);

        let user = collections.iter().find(|c| c.name == "User").unwrap();
        let posts_field = user.field_by_name("posts").unwrap();
        assert!(posts_field.kind.is_relation());
        assert!(posts_field.kind.is_array());

        let post = collections.iter().find(|c| c.name == "Post").unwrap();
        let author_field = post.field_by_name("author").unwrap();
        assert!(author_field.kind.is_relation());
        assert!(!author_field.kind.is_array());
    }

    #[test]
    fn test_parse_self_reference() {
        let sdl = r#"
            type Category {
                name: String
                parent: Category
                children: [Category]
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let category = &collections[0];

        let parent = category.field_by_name("parent").unwrap();
        assert!(matches!(
            parent.kind,
            FieldKind::SelfRef {
                is_array: false,
                ..
            }
        ));

        let children = category.field_by_name("children").unwrap();
        assert!(matches!(
            children.kind,
            FieldKind::SelfRef { is_array: true, .. }
        ));
    }

    #[test]
    fn test_parse_index_directive() {
        let sdl = r#"
            type User {
                email: String @index(unique: true)
                name: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        assert_eq!(user.indexes.len(), 1);
        let idx = &user.indexes[0];
        assert!(idx.unique);
        assert_eq!(idx.fields.len(), 1);
        assert_eq!(idx.fields[0].name, "email");
    }

    #[test]
    fn test_parse_all_scalar_types() {
        let sdl = r#"
            type AllTypes {
                s: String
                i: Int
                f: Float
                f32: Float32
                f64: Float64
                b: Boolean
                id: ID
                dt: DateTime
                j: JSON
                blob: Blob
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let all = &collections[0];

        assert_eq!(all.field_by_name("s").unwrap().kind, FieldKind::string());
        assert_eq!(all.field_by_name("i").unwrap().kind, FieldKind::int());
        assert_eq!(all.field_by_name("f").unwrap().kind, FieldKind::float64());
        assert_eq!(all.field_by_name("f32").unwrap().kind, FieldKind::float32());
        assert_eq!(all.field_by_name("f64").unwrap().kind, FieldKind::float64());
        assert_eq!(all.field_by_name("b").unwrap().kind, FieldKind::bool());
        assert_eq!(all.field_by_name("id").unwrap().kind, FieldKind::doc_id());
        assert_eq!(all.field_by_name("dt").unwrap().kind, FieldKind::datetime());
        assert_eq!(all.field_by_name("j").unwrap().kind, FieldKind::json());
        assert_eq!(all.field_by_name("blob").unwrap().kind, FieldKind::blob());
    }

    #[test]
    fn test_parse_issue_example() {
        // Example from issue #14
        let sdl = r#"
            type User {
                name: String
                age: Int
                posts: [Post] @primary
            }

            type Post {
                title: String!
                content: String
                author: User
                viewCount: Int @crdt(type: "pncounter")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(collections.len(), 2);

        let user = collections.iter().find(|c| c.name == "User").unwrap();
        let posts_field = user.field_by_name("posts").unwrap();
        assert!(posts_field.is_primary);
        assert!(posts_field.kind.is_array());

        let post = collections.iter().find(|c| c.name == "Post").unwrap();
        let view_count = post.field_by_name("viewCount").unwrap();
        assert_eq!(view_count.crdt_type, CType::PnCounter);
    }

    #[test]
    fn test_parse_empty_sdl() {
        let sdl = "";
        let collections = parse_sdl(sdl).unwrap();
        assert!(collections.is_empty());
    }

    #[test]
    fn test_parse_invalid_sdl() {
        let sdl = "not valid graphql { {";
        let result = parse_sdl(sdl);
        assert!(result.is_err());
    }

    #[test]
    fn test_doc_id_always_present() {
        let sdl = r#"
            type Simple {
                name: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let simple = &collections[0];

        let doc_id = simple.field_by_name("_docID");
        assert!(doc_id.is_some());
        assert_eq!(doc_id.unwrap().kind, FieldKind::doc_id());
    }

    #[test]
    fn test_collection_and_field_ids_generated() {
        let sdl = r#"
            type User {
                name: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        // collection_id and version_id should be non-empty
        assert!(!user.collection_id.is_empty());
        assert!(!user.version_id.is_empty());

        // field IDs should be non-empty
        for field in &user.fields {
            assert!(!field.id.is_empty());
        }
    }

    #[test]
    fn test_relation_names_generated() {
        let sdl = r#"
            type Author {
                books: [Book]
            }
            type Book {
                author: Author
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();

        for coll in &collections {
            for field in coll.relation_fields() {
                // Relation fields should have relation names
                assert!(field.relation_name.is_some());
            }
        }
    }

    // =========================================================================
    // Go Compatibility Tests
    // =========================================================================

    #[test]
    fn test_relation_directive_explicit_name() {
        let sdl = r#"
            type User {
                posts: [Post] @relation(name: "user_authored_posts")
            }
            type Post {
                author: User @relation(name: "user_authored_posts") @primary
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();

        let user = collections.iter().find(|c| c.name == "User").unwrap();
        let posts = user.field_by_name("posts").unwrap();
        assert_eq!(
            posts.relation_name.as_deref(),
            Some("user_authored_posts"),
            "explicit @relation name should be used"
        );

        let post = collections.iter().find(|c| c.name == "Post").unwrap();
        let author = post.field_by_name("author").unwrap();
        assert_eq!(
            author.relation_name.as_deref(),
            Some("user_authored_posts"),
            "explicit @relation name should be used"
        );
        assert!(author.is_primary, "@primary should mark the primary side");
    }

    #[test]
    fn test_relation_name_auto_generation() {
        let sdl = r#"
            type Author {
                books: [Book]
            }
            type Book {
                writer: Author
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();

        let author = collections.iter().find(|c| c.name == "Author").unwrap();
        let books = author.field_by_name("books").unwrap();
        // Lexicographic: "author" < "book", so format is author_books_book
        assert_eq!(books.relation_name.as_deref(), Some("author_books_book"));

        let book = collections.iter().find(|c| c.name == "Book").unwrap();
        let writer = book.field_by_name("writer").unwrap();
        // Lexicographic: "author" < "book", so format is author_writer_book
        assert_eq!(writer.relation_name.as_deref(), Some("author_writer_book"));
    }

    #[test]
    fn test_default_directive_string() {
        let sdl = r#"
            type User {
                role: String @default(string: "member")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        let role = user.field_by_name("role").unwrap();
        assert_eq!(
            role.default_value,
            Some(serde_json::Value::String("member".to_string()))
        );
    }

    #[test]
    fn test_default_directive_int() {
        let sdl = r#"
            type Counter {
                count: Int @default(int: 0)
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let counter = &collections[0];

        let count = counter.field_by_name("count").unwrap();
        assert_eq!(
            count.default_value,
            Some(serde_json::Value::Number(0.into()))
        );
    }

    #[test]
    fn test_default_directive_bool() {
        let sdl = r#"
            type Settings {
                enabled: Boolean @default(bool: true)
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let settings = &collections[0];

        let enabled = settings.field_by_name("enabled").unwrap();
        assert_eq!(enabled.default_value, Some(serde_json::Value::Bool(true)));
    }

    #[test]
    fn test_constraints_directive_array_size() {
        let sdl = r#"
            type Article {
                tags: [String!] @constraints(size: 10)
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let article = &collections[0];

        let tags = article.field_by_name("tags").unwrap();
        assert_eq!(tags.size, 10, "@constraints(size:) should set field.size");
    }

    #[test]
    fn test_materialized_directive() {
        let sdl = r#"
            type CachedView @materialized {
                data: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let view = &collections[0];

        assert!(
            view.is_materialized,
            "@materialized should set is_materialized = true"
        );
    }

    #[test]
    fn test_branchable_directive() {
        let sdl = r#"
            type VersionedDoc @branchable {
                content: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let doc = &collections[0];

        assert!(
            doc.is_branchable,
            "@branchable should set is_branchable = true"
        );
    }

    #[test]
    fn test_materialized_directive_with_if_false() {
        let sdl = r#"
            type NotCached @materialized(if: false) {
                data: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let view = &collections[0];

        assert!(
            !view.is_materialized,
            "@materialized(if: false) should set is_materialized = false"
        );
    }

    #[test]
    fn test_float32_scalar_type() {
        let sdl = r#"
            type Sensor {
                temperature: Float32
                values: [Float32!]
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let sensor = &collections[0];

        let temp = sensor.field_by_name("temperature").unwrap();
        assert_eq!(temp.kind, FieldKind::float32());

        let values = sensor.field_by_name("values").unwrap();
        assert_eq!(values.kind, FieldKind::float32_array());
    }

    #[test]
    fn test_multiple_field_indexes() {
        let sdl = r#"
            type User {
                email: String @index(unique: true)
                username: String @index(unique: true)
                age: Int @index
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        assert_eq!(user.indexes.len(), 3, "should have 3 separate indexes");

        let email_idx = user.indexes.iter().find(|i| i.fields[0].name == "email");
        assert!(email_idx.is_some());
        assert!(email_idx.unwrap().unique);

        let age_idx = user.indexes.iter().find(|i| i.fields[0].name == "age");
        assert!(age_idx.is_some());
        assert!(!age_idx.unwrap().unique);
    }

    #[test]
    fn test_type_level_composite_index() {
        let sdl = r#"
            type User @index(fields: ["firstName", "lastName"], name: "full_name_idx") {
                firstName: String
                lastName: String
                email: String
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let user = &collections[0];

        assert_eq!(user.indexes.len(), 1);
        let idx = &user.indexes[0];
        assert_eq!(idx.name, "full_name_idx");
        assert_eq!(idx.fields.len(), 2);
        assert_eq!(idx.fields[0].name, "firstName");
        assert_eq!(idx.fields[1].name, "lastName");
    }

    #[test]
    fn test_crdt_directive_variations() {
        let sdl = r#"
            type Counters {
                lww: Int @crdt(type: "lww")
                lwwRegister: Int @crdt(type: "LWW_REGISTER")
                pn: Int @crdt(type: "pncounter")
                pnCounter: Int @crdt(type: "PN_COUNTER")
                p: Int @crdt(type: "pcounter")
                pCounter: Int @crdt(type: "P_COUNTER")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let counters = &collections[0];

        assert_eq!(
            counters.field_by_name("lww").unwrap().crdt_type,
            CType::LwwRegister
        );
        assert_eq!(
            counters.field_by_name("lwwRegister").unwrap().crdt_type,
            CType::LwwRegister
        );
        assert_eq!(
            counters.field_by_name("pn").unwrap().crdt_type,
            CType::PnCounter
        );
        assert_eq!(
            counters.field_by_name("pnCounter").unwrap().crdt_type,
            CType::PnCounter
        );
        assert_eq!(
            counters.field_by_name("p").unwrap().crdt_type,
            CType::PCounter
        );
        assert_eq!(
            counters.field_by_name("pCounter").unwrap().crdt_type,
            CType::PCounter
        );
    }

    #[test]
    fn test_crdt_validation_fails_for_non_numeric() {
        let sdl = r#"
            type Article {
                title: String @crdt(type: "pncounter")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let article = &collections[0];

        // Parsing succeeds, but validation should fail
        let result = article.validate();
        assert!(
            result.is_err(),
            "PnCounter on String should fail validation"
        );
    }

    #[test]
    fn test_collection_ids_are_deterministic() {
        let id1 = generate_collection_id("User");
        let id2 = generate_collection_id("User");
        assert_eq!(id1, id2, "same type name should produce same collection ID");

        let id3 = generate_collection_id("Post");
        assert_ne!(id1, id3, "different type names should produce different IDs");
    }

    #[test]
    fn test_field_ids_are_deterministic() {
        let id1 = generate_field_id("User", "name");
        let id2 = generate_field_id("User", "name");
        assert_eq!(
            id1, id2,
            "same type+field should produce same field ID"
        );

        let id3 = generate_field_id("User", "email");
        assert_ne!(
            id1, id3,
            "different field names should produce different IDs"
        );
    }

    #[test]
    fn test_index_descending_direction() {
        let sdl = r#"
            type Event {
                timestamp: DateTime @index(direction: "DESC")
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        let event = &collections[0];

        assert_eq!(event.indexes.len(), 1);
        assert!(event.indexes[0].fields[0].descending);
    }
}
