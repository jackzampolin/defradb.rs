//! SDL parsing for the WASM client.
//!
//! Parses GraphQL Schema Definition Language into CollectionVersion schemas.
//! This is a simplified version for WASM that doesn't depend on the query crate.

use graphql_parser::schema::{Definition, Document, Field, ObjectType, Type, TypeDefinition};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::error::{Result, WasmError};

/// Parse GraphQL SDL into collection definitions.
///
/// This is a simplified parser for the WASM client. It supports:
/// - Type definitions with scalar fields
/// - Basic field types (String, Int, Float, Boolean, DateTime, Blob, JSON)
/// - Array types ([String], [Int], etc.)
/// - Relations between types
///
/// Directives (@index, @policy, etc.) are not yet supported in the WASM client.
pub fn parse_sdl(sdl: &str) -> Result<Vec<CollectionVersion>> {
    let document: Document<'_, String> = graphql_parser::parse_schema(sdl)
        .map_err(|e| WasmError::Schema(format!("SDL parse error: {}", e)))?;

    let mut collections = Vec::new();
    let mut type_names: HashMap<String, bool> = HashMap::new();

    // First pass: collect all type names
    for def in &document.definitions {
        if let Definition::TypeDefinition(TypeDefinition::Object(obj)) = def {
            type_names.insert(obj.name.clone(), true);
        }
    }

    // Second pass: parse type definitions
    for def in document.definitions {
        if let Definition::TypeDefinition(TypeDefinition::Object(obj)) = def {
            let collection = parse_object_type(&obj, &type_names)?;
            collections.push(collection);
        }
    }

    Ok(collections)
}

fn parse_object_type(
    obj: &ObjectType<'_, String>,
    type_names: &HashMap<String, bool>,
) -> Result<CollectionVersion> {
    let name = obj.name.clone();
    let mut fields = Vec::new();
    let mut field_id = 1u32;

    for field in &obj.fields {
        let field_desc = parse_field(field, &mut field_id, type_names)?;
        fields.push(field_desc);
    }

    // Generate stable IDs
    let version_id = generate_version_id(&name, &fields);
    let collection_id = generate_collection_id(&name);

    Ok(CollectionVersion::new(
        name,
        version_id,
        collection_id,
        fields,
    ))
}

fn parse_field(
    field: &Field<'_, String>,
    field_id: &mut u32,
    type_names: &HashMap<String, bool>,
) -> Result<FieldDescription> {
    let name = field.name.clone();
    let kind = parse_field_type(&field.field_type, type_names)?;
    let id = field_id.to_string();
    *field_id += 1;

    Ok(FieldDescription::new(id, name, kind))
}

fn parse_field_type(
    field_type: &Type<'_, String>,
    type_names: &HashMap<String, bool>,
) -> Result<FieldKind> {
    match field_type {
        Type::NamedType(name) => parse_named_type(name, type_names, false),
        Type::ListType(inner) => {
            // Array type
            match inner.as_ref() {
                Type::NamedType(name) => parse_named_type(name, type_names, true),
                Type::NonNullType(inner) => {
                    if let Type::NamedType(name) = inner.as_ref() {
                        parse_named_type(name, type_names, true)
                    } else {
                        Err(WasmError::Schema("Nested arrays not supported".to_string()))
                    }
                }
                _ => Err(WasmError::Schema("Nested arrays not supported".to_string())),
            }
        }
        Type::NonNullType(inner) => parse_field_type(inner, type_names),
    }
}

fn parse_named_type(
    name: &str,
    type_names: &HashMap<String, bool>,
    is_array: bool,
) -> Result<FieldKind> {
    // Check if it's a relation to another type
    if type_names.contains_key(name) {
        return Ok(FieldKind::relation(name, is_array));
    }

    // Parse as scalar
    match (name, is_array) {
        ("String", false) => Ok(FieldKind::string()),
        ("String", true) => Ok(FieldKind::string_array()),
        ("Int", false) => Ok(FieldKind::int()),
        ("Int", true) => Ok(FieldKind::int_array()),
        ("Float", false) => Ok(FieldKind::float64()),
        ("Float", true) => Ok(FieldKind::float64_array()),
        ("Boolean" | "Bool", false) => Ok(FieldKind::bool()),
        ("Boolean" | "Bool", true) => Ok(FieldKind::bool_array()),
        ("DateTime", false) => Ok(FieldKind::datetime()),
        ("DateTime", true) => Err(WasmError::Schema(
            "DateTime arrays not supported".to_string(),
        )),
        ("Blob", false) => Ok(FieldKind::blob()),
        ("Blob", true) => Err(WasmError::Schema("Blob arrays not supported".to_string())),
        ("JSON", false) => Ok(FieldKind::json()),
        ("JSON", true) => Err(WasmError::Schema("JSON arrays not supported".to_string())),
        ("ID", false) => Ok(FieldKind::doc_id()),
        ("ID", true) => Err(WasmError::Schema("ID arrays not supported".to_string())),
        _ => Err(WasmError::Schema(format!("Unknown type: {}", name))),
    }
}

/// Generate a stable version ID from type name and fields.
fn generate_version_id(name: &str, fields: &[FieldDescription]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    for field in fields {
        hasher.update(field.name.as_bytes());
        hasher.update(format!("{:?}", field.kind).as_bytes());
    }
    let hash = hasher.finalize();
    hex::encode(&hash[..8]) // Use first 8 bytes for shorter ID
}

/// Generate a stable collection ID from type name.
fn generate_collection_id(name: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"collection:");
    hasher.update(name.as_bytes());
    let hash = hasher.finalize();
    hex::encode(&hash[..8])
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
                active: Boolean
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].name, "User");
        assert_eq!(collections[0].fields.len(), 3);
    }

    #[test]
    fn test_parse_array_fields() {
        let sdl = r#"
            type Post {
                tags: [String]
                scores: [Int]
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(collections.len(), 1);
        assert!(collections[0].fields[0].kind.is_array());
    }

    #[test]
    fn test_parse_relation() {
        let sdl = r#"
            type User {
                name: String
            }

            type Post {
                title: String
                author: User
            }
        "#;

        let collections = parse_sdl(sdl).unwrap();
        assert_eq!(collections.len(), 2);
    }

    #[test]
    fn test_parse_error() {
        let sdl = "invalid { syntax";
        assert!(parse_sdl(sdl).is_err());
    }
}
