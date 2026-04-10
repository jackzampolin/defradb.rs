//! Warning types for SDL parsing
//!
//! These warnings enable forward compatibility by allowing unknown directives
//! and arguments to be parsed without errors.

use schema::CollectionVersion;

/// Warnings generated during SDL parsing
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseWarning {
    /// Unknown directive encountered (forward compatibility)
    UnknownDirective {
        directive_name: String,
        location: DirectiveLocation,
        type_name: String,
        field_name: Option<String>,
    },
    /// Unknown argument on a known directive
    UnknownDirectiveArgument {
        directive_name: String,
        argument_name: String,
        type_name: String,
        field_name: Option<String>,
    },
    /// Directive is recognized but not yet implemented
    UnimplementedDirective {
        directive_name: String,
        type_name: String,
        field_name: Option<String>,
    },
    /// Argument has wrong type (e.g., string instead of bool)
    InvalidArgumentType {
        directive_name: String,
        argument_name: String,
        expected_type: String,
        type_name: String,
        field_name: Option<String>,
    },
}

impl std::fmt::Display for ParseWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseWarning::UnknownDirective {
                directive_name,
                location,
                type_name,
                field_name,
            } => {
                let location_str = match location {
                    DirectiveLocation::Type => format!("type {}", type_name),
                    DirectiveLocation::Field => {
                        format!(
                            "field {}.{}",
                            type_name,
                            field_name.as_deref().unwrap_or("?")
                        )
                    }
                };
                write!(
                    f,
                    "unknown directive @{} on {} (ignored for forward compatibility)",
                    directive_name, location_str
                )
            }
            ParseWarning::UnknownDirectiveArgument {
                directive_name,
                argument_name,
                type_name,
                field_name,
            } => {
                let location_str = format_location(type_name, field_name.as_deref());
                write!(
                    f,
                    "unknown argument '{}' on directive @{} at {}",
                    argument_name, directive_name, location_str
                )
            }
            ParseWarning::UnimplementedDirective {
                directive_name,
                type_name,
                field_name,
            } => {
                let location_str = format_location(type_name, field_name.as_deref());
                write!(
                    f,
                    "directive @{} at {} is recognized but not yet implemented",
                    directive_name, location_str
                )
            }
            ParseWarning::InvalidArgumentType {
                directive_name,
                argument_name,
                expected_type,
                type_name,
                field_name,
            } => {
                let location_str = format_location(type_name, field_name.as_deref());
                write!(
                    f,
                    "argument '{}' on @{} at {} should be {}, value ignored",
                    argument_name, directive_name, location_str, expected_type
                )
            }
        }
    }
}

/// Format a location string for warnings
pub fn format_location(type_name: &str, field_name: Option<&str>) -> String {
    if let Some(fname) = field_name {
        format!("{}.{}", type_name, fname)
    } else {
        type_name.to_string()
    }
}

/// Location where a directive was found
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DirectiveLocation {
    Type,
    Field,
}

/// Result of SDL parsing including warnings
#[derive(Debug)]
pub struct ParseOutput {
    /// Parsed collection versions
    pub collections: Vec<CollectionVersion>,
    /// Warnings generated during parsing
    pub warnings: Vec<ParseWarning>,
}
