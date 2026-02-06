//! SDL parser implementation
//!
//! Parses GraphQL Schema Definition Language (SDL) into DefraDB CollectionVersion schemas.
//! Designed for compatibility with Go DefraDB's SDL parsing behavior.

use crate::error::{QueryError, Result};
use graphql_parser::schema::{Definition, Document, InterfaceType, ObjectType, TypeDefinition};
use schema::CollectionVersion;
use std::collections::HashMap;

use super::directives::ParsedDirectives;
use super::helpers::{detect_missing_field_types, preprocess_empty_types};
use super::warnings::{ParseOutput, ParseWarning};

/// Placeholder field name used to make empty types parseable.
/// graphql_parser requires at least one field per type, but Go DefraDB allows empty types.
pub(super) const EMPTY_TYPE_PLACEHOLDER: &str = "__defradb_empty_type_placeholder__";

pub struct SdlParser<'a> {
    pub(super) sdl: &'a str,
    /// Parsed type definitions by name
    pub(super) type_defs: HashMap<String, ParsedTypeDef>,
    /// Type names in SDL definition order (Go returns collections in this order)
    pub(super) definition_order: Vec<String>,
    /// Warnings collected during parsing
    pub(super) warnings: Vec<ParseWarning>,
    /// Current type being parsed (for warning context)
    pub(super) current_type: Option<String>,
    /// External type names (e.g. existing collection types) that can be referenced
    /// in field types but are not defined in the SDL being parsed.
    pub(super) known_external_types: std::collections::HashSet<String>,
    /// Accumulated errors from parsing (for multi-error reporting)
    pub(super) errors: Vec<String>,
}

/// A parsed type definition (either type or interface) with its fields and type-level directives.
#[derive(Debug)]
pub(super) struct ParsedTypeDef {
    pub(super) name: String,
    pub(super) fields: Vec<ParsedField>,
    pub(super) directives: ParsedTypeDirectives,
    pub(super) is_interface: bool,
}

/// Type-level directives
#[derive(Debug)]
pub(super) struct ParsedTypeDirectives {
    pub(super) indexes: Vec<CompositeIndex>,
    /// Default true for collections (Go compatibility)
    pub(super) is_materialized: bool,
    pub(super) is_branchable: bool,
    pub(super) policy: Option<PolicyConfig>,
}

impl Default for ParsedTypeDirectives {
    fn default() -> Self {
        Self {
            indexes: Vec::new(),
            // Go defaults IsMaterialized to true for regular collections
            is_materialized: true,
            is_branchable: false,
            policy: None,
        }
    }
}

/// Policy configuration from @policy directive
#[derive(Debug, Clone)]
pub(super) struct PolicyConfig {
    pub(super) id: String,
    pub(super) resource: String,
}

#[derive(Debug)]
pub(super) struct CompositeIndex {
    pub(super) fields: Vec<(String, bool)>,
    pub(super) name: Option<String>,
    pub(super) unique: bool,
}

#[derive(Debug)]
pub(super) struct ParsedField {
    pub(super) name: String,
    pub(super) field_type: ParsedType,
    pub(super) directives: ParsedDirectives,
}

#[derive(Debug)]
pub(super) struct ParsedType {
    pub(super) base_type: String,
    pub(super) is_list: bool,
    pub(super) is_non_null: bool,
    pub(super) element_non_null: bool,
}

impl<'a> SdlParser<'a> {
    pub fn new(sdl: &'a str) -> Self {
        Self {
            sdl,
            type_defs: HashMap::new(),
            definition_order: Vec::new(),
            warnings: Vec::new(),
            current_type: None,
            known_external_types: std::collections::HashSet::new(),
            errors: Vec::new(),
        }
    }

    /// Set external type names that can be referenced but aren't defined in the SDL.
    pub fn with_known_types(mut self, types: std::collections::HashSet<String>) -> Self {
        self.known_external_types = types;
        self
    }

    /// Parse the SDL and return collection versions
    pub fn parse(&mut self) -> Result<Vec<CollectionVersion>> {
        let output = self.parse_with_warnings()?;
        Ok(output.collections)
    }

    /// Parse the SDL and return collection versions with warnings
    pub fn parse_with_warnings(&mut self) -> Result<ParseOutput> {
        // Handle empty or whitespace-only input
        if self.sdl.trim().is_empty() {
            return Ok(ParseOutput {
                collections: Vec::new(),
                warnings: Vec::new(),
            });
        }

        // Detect fields with missing types before graphql_parser (Go compatibility)
        detect_missing_field_types(self.sdl)?;

        // Preprocess SDL to handle empty type definitions (Go compatibility)
        let preprocessed = preprocess_empty_types(self.sdl);

        let doc: Document<'_, String> = graphql_parser::parse_schema(&preprocessed)
            .map_err(|e| QueryError::parse(e.to_string()))?;

        // First pass: collect all type definitions (both `type` and `interface` keywords)
        for def in &doc.definitions {
            match def {
                Definition::TypeDefinition(TypeDefinition::Object(obj)) => {
                    self.parse_object_type(obj)?;
                }
                Definition::TypeDefinition(TypeDefinition::Interface(iface)) => {
                    self.parse_interface_type(iface)?;
                }
                _ => {}
            }
        }

        // Return accumulated errors from first pass (e.g., duplicate type names)
        if !self.errors.is_empty() {
            return Err(QueryError::parse(self.errors.join("\n")));
        }

        // Second pass: validate parsed types before building
        self.validate_types()?;

        // Third pass: resolve relations and build CollectionVersions
        let collections = self.build_collections()?;

        Ok(ParseOutput {
            collections,
            warnings: std::mem::take(&mut self.warnings),
        })
    }

    fn parse_object_type(&mut self, obj: &ObjectType<'_, String>) -> Result<()> {
        let name = obj.name.clone();

        // Check for duplicate type names (Go compatibility: accumulate all duplicate errors)
        if self.type_defs.contains_key(&name) {
            self.errors
                .push(format!("collection already exists. Name: {}", name));
            return Ok(());
        }

        self.current_type = Some(name.clone());
        let mut fields = Vec::new();

        for field in &obj.fields {
            // Skip placeholder fields used for empty type preprocessing
            if field.name == EMPTY_TYPE_PLACEHOLDER {
                continue;
            }
            let parsed_field = self.parse_field(field)?;
            fields.push(parsed_field);
        }

        let type_directives = self.parse_type_directives(&obj.directives)?;

        self.definition_order.push(name.clone());
        self.type_defs.insert(
            name.clone(),
            ParsedTypeDef {
                name,
                fields,
                directives: type_directives,
                is_interface: false,
            },
        );

        self.current_type = None;
        Ok(())
    }

    /// Parse an interface type definition.
    /// Go's SDL parser treats `interface` the same as `type` for view embedded schemas.
    fn parse_interface_type(&mut self, iface: &InterfaceType<'_, String>) -> Result<()> {
        let name = iface.name.clone();

        if self.type_defs.contains_key(&name) {
            self.errors
                .push(format!("collection already exists. Name: {}", name));
            return Ok(());
        }

        self.current_type = Some(name.clone());
        let mut fields = Vec::new();

        for field in &iface.fields {
            if field.name == EMPTY_TYPE_PLACEHOLDER {
                continue;
            }
            let parsed_field = self.parse_field(field)?;
            fields.push(parsed_field);
        }

        let type_directives = self.parse_type_directives(&iface.directives)?;

        self.definition_order.push(name.clone());
        self.type_defs.insert(
            name.clone(),
            ParsedTypeDef {
                name,
                fields,
                directives: type_directives,
                is_interface: true,
            },
        );

        self.current_type = None;
        Ok(())
    }
}

/// Parse SDL string into CollectionVersion schemas.
///
/// This convenience function discards all warnings. For production use where
/// you need visibility into unknown directives, invalid argument types, or
/// unimplemented features, use [`parse_sdl_with_warnings`] instead.
pub fn parse_sdl(sdl: &str) -> Result<Vec<CollectionVersion>> {
    let mut parser = SdlParser::new(sdl);
    parser.parse()
}

/// Parse SDL with knowledge of existing collection type names.
///
/// External types referenced in the SDL (e.g. `books: [Book]` where `Book` is
/// an existing collection) will be resolved as named relations instead of
/// producing "no type found" errors.
pub fn parse_sdl_with_known_types(
    sdl: &str,
    known_types: std::collections::HashSet<String>,
) -> Result<Vec<CollectionVersion>> {
    let mut parser = SdlParser::new(sdl).with_known_types(known_types);
    parser.parse()
}

/// Parse SDL string into CollectionVersion schemas with warnings.
///
/// Returns both the parsed collections and any warnings encountered during parsing.
/// Warnings are emitted for:
/// - Unknown directives (forward compatibility - ignored but noted)
/// - Unknown arguments on known directives (possible typos)
/// - Invalid argument types (e.g., string where bool expected - default used)
/// - Unimplemented directives (@embedding, @encryptedIndex, field @policy)
///
/// This is the recommended entry point for production use.
pub fn parse_sdl_with_warnings(sdl: &str) -> Result<ParseOutput> {
    let mut parser = SdlParser::new(sdl);
    parser.parse_with_warnings()
}

#[cfg(test)]
#[path = "parser_tests.rs"]
mod tests;
