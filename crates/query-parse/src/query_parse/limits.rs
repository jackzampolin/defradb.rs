//! Query structure limits to prevent denial-of-service via deeply nested or wide queries.

use query_types::error::{QueryError, Result};
use query_types::limits::{QueryLimits, DEFAULT_MAX_QUERY_DEPTH, DEFAULT_MAX_QUERY_WIDTH};
use query_types::mapper::{Requestable, Select};

/// Maximum nesting depth for GraphQL selections.
///
/// A query like `{ A { B { C { ... } } } }` has depth equal to the number of
/// nested select levels. Queries deeper than this limit are rejected.
pub const MAX_QUERY_DEPTH: usize = DEFAULT_MAX_QUERY_DEPTH;

/// Maximum number of fields at any single selection level.
///
/// This caps the fan-out per level to prevent wide queries that would generate
/// an unbounded number of fetches.
pub const MAX_QUERY_WIDTH: usize = DEFAULT_MAX_QUERY_WIDTH;

/// Validate a parsed Select tree against depth and width limits.
///
/// Called after parsing to reject queries that would cause excessive work.
pub fn validate_select_limits(select: &Select) -> Result<()> {
    validate_select_limits_with(select, QueryLimits::default())
}

/// Validate a parsed Select tree against custom depth and width limits.
pub fn validate_select_limits_with(select: &Select, limits: QueryLimits) -> Result<()> {
    validate_select_at_depth(select, 1, limits)
}

/// Validate a parsed requestable field list against custom depth and width limits.
pub fn validate_requestable_limits_with(
    requestables: &[Requestable],
    limits: QueryLimits,
) -> Result<()> {
    validate_requestables_at_depth(requestables, 1, limits)
}

fn validate_select_at_depth(select: &Select, depth: usize, limits: QueryLimits) -> Result<()> {
    if limits.max_query_depth > 0 && depth > limits.max_query_depth {
        return Err(QueryError::parse(format!(
            "query exceeds maximum nesting depth of {}",
            limits.max_query_depth
        )));
    }

    validate_requestables_at_depth(&select.fields, depth, limits)
}

fn validate_requestables_at_depth(
    requestables: &[Requestable],
    depth: usize,
    limits: QueryLimits,
) -> Result<()> {
    if limits.max_query_width > 0 && requestables.len() > limits.max_query_width {
        return Err(QueryError::parse(format!(
            "query exceeds maximum field width of {} at depth {}",
            limits.max_query_width, depth
        )));
    }

    for field in requestables {
        if let Requestable::Select(nested) = field {
            validate_select_at_depth(nested, depth + 1, limits)?;
        }
    }

    Ok(())
}
