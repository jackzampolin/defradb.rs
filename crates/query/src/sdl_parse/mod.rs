//! GraphQL SDL parsing to create CollectionVersion schemas
//!
//! This module parses GraphQL Schema Definition Language (SDL) and converts
//! type definitions into DefraDB CollectionVersion schemas.

mod directives;
mod parser;
mod warnings;

pub use directives::ParsedDirectives;
pub use parser::{parse_sdl, parse_sdl_with_warnings, SdlParser};
pub use warnings::{DirectiveLocation, ParseOutput, ParseWarning};
