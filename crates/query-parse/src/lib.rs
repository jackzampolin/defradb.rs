//! GraphQL query parser
//!
//! Parses GraphQL query strings into Select and Mutation operations for execution.
//!
//! This module is organized into submodules for maintainability:
//! - `parser`: Main parsing logic (entry points, field/selection parsing)
//! - `filters`: Filter argument parsing
//! - `ordering`: Order argument parsing
//! - `aggregates`: Aggregate and groupBy parsing
//! - `mutations`: Mutation operation parsing

mod aggregates;
mod explain;
mod filters;
pub mod limits;
mod mutations;
mod ordering;
mod parser;
mod provider;
mod validation;
mod values;
mod variables;

pub use limits::{MAX_QUERY_DEPTH, MAX_QUERY_WIDTH};
pub use parser::{
    ExplainType, ParsedOperation, parse_mutations, parse_mutations_with_variables, parse_query,
    parse_query_with_variables, parse_request, parse_request_with_variables,
};
pub use provider::{CollectionProvider, StaticCollectionProvider};
pub use validation::validate_parsed_operation;
