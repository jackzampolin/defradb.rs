//! GraphQL SDL parsing to create CollectionVersion schemas
//!
//! This module parses GraphQL Schema Definition Language (SDL) and converts
//! type definitions into DefraDB CollectionVersion schemas.

mod parser;

pub use parser::{parse_sdl, ParsedDirectives, SdlParser};
