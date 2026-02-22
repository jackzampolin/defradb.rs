//! Query structure limits to prevent denial-of-service via deeply nested or wide queries.

use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};

/// Maximum nesting depth for GraphQL selections.
///
/// A query like `{ A { B { C { ... } } } }` has depth equal to the number of
/// nested select levels. Queries deeper than this limit are rejected.
pub const MAX_QUERY_DEPTH: usize = 20;

/// Maximum number of fields at any single selection level.
///
/// This caps the fan-out per level to prevent wide queries that would generate
/// an unbounded number of fetches.
pub const MAX_QUERY_WIDTH: usize = 100;

/// Validate a parsed Select tree against depth and width limits.
///
/// Called after parsing to reject queries that would cause excessive work.
pub fn validate_select_limits(select: &Select) -> Result<()> {
    validate_select_at_depth(select, 1)
}

fn validate_select_at_depth(select: &Select, depth: usize) -> Result<()> {
    if depth > MAX_QUERY_DEPTH {
        return Err(QueryError::parse(format!(
            "query exceeds maximum nesting depth of {}",
            MAX_QUERY_DEPTH
        )));
    }

    if select.fields.len() > MAX_QUERY_WIDTH {
        return Err(QueryError::parse(format!(
            "query exceeds maximum field width of {} at depth {}",
            MAX_QUERY_WIDTH, depth
        )));
    }

    for field in &select.fields {
        if let Requestable::Select(nested) = field {
            validate_select_at_depth(nested, depth + 1)?;
        }
    }

    Ok(())
}
