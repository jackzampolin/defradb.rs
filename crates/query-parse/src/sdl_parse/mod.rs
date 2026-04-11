//! GraphQL SDL parsing to create CollectionVersion schemas
//!
//! This module parses GraphQL Schema Definition Language (SDL) and converts
//! type definitions into DefraDB CollectionVersion schemas.
//!
//! # Module Organization (planned)
//!
//! - `parser`: Main SdlParser struct and parse logic
//! - `directives`: Directive parsing
//! - `warnings`: Warning types
//! - `preprocess`: Schema preprocessing (placeholder)
//! - `types`: Type parsing utilities (placeholder)
//! - `fields`: Field parsing utilities (placeholder)
//! - `validation`: Schema validation (placeholder)
//! - `builder`: Collection building (placeholder)
//! - `helpers`: Helper utilities (placeholder)

mod builder;
mod builder_cycles;
mod builder_field_kinds;
mod directives;
mod fields;
mod helpers;
mod parser;
mod preprocess;
mod types;
mod validation;
mod warnings;

pub use directives::ParsedDirectives;
pub use parser::{parse_sdl, parse_sdl_with_known_types, parse_sdl_with_warnings, SdlParser};
pub use warnings::{DirectiveLocation, ParseOutput, ParseWarning};
