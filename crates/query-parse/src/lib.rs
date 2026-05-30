//! GraphQL/SDL parsing and schema generation for DefraDB.
//!
//! This crate handles the parsing stage of the query pipeline:
//! GraphQL string → parsed operations → typed query structures.

pub mod query_parse;
pub mod schema_gen;
pub mod sdl_parse;
pub mod select_convert;

pub use query_parse::{
    parse_mutations, parse_mutations_with_limits, parse_query, parse_query_with_limits,
    parse_request, parse_request_with_limits, ExplainType, ParsedOperation,
};
pub use sdl_parse::{parse_sdl, parse_sdl_with_known_types};
pub use select_convert::select_to_go_json;
