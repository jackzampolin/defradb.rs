//! GraphQL query parser
//!
//! Parses GraphQL query strings into Select and Mutation operations for execution.
//!
//! This module is organized into submodules for maintainability:
//! - `parser`: Main parsing logic (entry points, field/mutation parsing)
//! - `types`: Type definitions (ExplainType, ParsedOperation)
//! - `explain`: Explain directive handling
//! - `values`: GraphQL to JSON value conversion
//! - `variables`: Variable handling and defaults

mod parser;

// Future: these modules contain extracted helper functions
// that can be used once parser.rs is refactored to use them
mod explain;
mod types;
mod values;
mod variables;

// Re-export everything from parser for backwards compatibility
pub use parser::{
    parse_mutations, parse_mutations_with_variables, parse_query, parse_query_with_variables,
    parse_request, parse_request_with_variables, ExplainType, ParsedOperation,
};
