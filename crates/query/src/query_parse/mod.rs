//! GraphQL query parsing facade plus query-owned live-schema validation.

mod validation;

pub use ::query_parse::{
    ExplainType, ParsedOperation, MAX_QUERY_DEPTH, MAX_QUERY_WIDTH, parse_mutations,
    parse_mutations_with_variables, parse_query, parse_query_with_variables, parse_request,
    parse_request_with_variables,
};
pub use validation::validate_parsed_operation;
