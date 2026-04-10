//! GraphQL SDL parsing facade.

pub use query_sdl::sdl_parse::{
    DirectiveLocation, ParseOutput, ParseWarning, ParsedDirectives, SdlParser, parse_sdl,
    parse_sdl_with_known_types, parse_sdl_with_warnings,
};
