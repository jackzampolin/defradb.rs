//! Parser for the `_cursor` GraphQL wrapper field.
//!
//! Mirrors Go's `internal/request/graphql/parser/cursor.go` semantics.

use std::collections::{HashMap, HashSet};

use graphql_parser::query::{Field, Selection};
use serde_json::Value as JsonValue;

use query_types::error::{QueryError, Result};
use query_types::mapper::{CursorAliases, CursorPageInfoFields, CursorParams, Select};

use super::parser::{parse_field_to_select, FragmentMap};
use super::values::{parse_int_value, resolve_string_value};

/// Parse the contents of a `_cursor { ... }` wrapper field.
///
/// Expects exactly one inner collection field (e.g., `User(...)`) and optionally
/// a `_pageInfo { ... }` sibling. Returns the inner `Select` populated with cursor metadata.
pub(super) fn parse_cursor_wrapper<'a>(
    field: &'a Field<'a, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    fragments: &FragmentMap<'a>,
    visiting: &mut HashSet<String>,
) -> Result<Select> {
    let mut inner_field: Option<&'a Field<'a, String>> = None;
    let mut page_info_fields = CursorPageInfoFields::default();
    let mut inner_count = 0usize;

    for selection in &field.selection_set.items {
        match selection {
            Selection::Field(child) => {
                if child.name == "_pageInfo" {
                    page_info_fields = parse_page_info_selection(child)?;
                } else {
                    inner_count += 1;
                    if inner_field.is_none() {
                        inner_field = Some(child);
                    }
                }
            }
            _ => {
                return Err(QueryError::parse(
                    "_cursor block does not support fragments".to_string(),
                ));
            }
        }
    }

    if inner_count == 0 {
        return Err(QueryError::cursor_must_contain_query());
    }
    if inner_count > 1 {
        return Err(QueryError::cursor_multiple_queries());
    }

    let inner_field = inner_field.expect("inner_count > 0 implies Some");

    let cursor_params = extract_cursor_params(inner_field, variables)?;

    if cursor_params.is_forward() && cursor_params.is_backward() {
        return Err(QueryError::cursor_forward_backward_conflict());
    }

    // Clone the inner field and strip cursor-specific args so that
    // parse_field_to_select (which rejects unknown args) doesn't choke on them.
    let mut inner_field_clean = inner_field.clone();
    inner_field_clean
        .arguments
        .retain(|(name, _)| !matches!(name.as_str(), "first" | "after" | "last" | "before"));

    let mut select = parse_field_to_select(&inner_field_clean, variables, fragments, visiting)?;

    select.is_cursor = true;
    select.cursor_params = Some(cursor_params);
    select.cursor_page_info = page_info_fields;
    select.cursor_aliases = CursorAliases {
        wrapper_alias: field.alias.clone(),
    };

    Ok(select)
}

/// Extract `first`/`after`/`last`/`before` from a field's arguments.
///
/// Validates non-negative for `first` and `last`. Rejects `limit` and `offset`
/// because they are not valid arguments on cursor collection fields. Other args
/// are left untouched (they remain in the field for downstream parsing).
fn extract_cursor_params<'a>(
    field: &'a Field<'a, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<CursorParams> {
    let mut params = CursorParams::default();
    for (name, value) in &field.arguments {
        match name.as_str() {
            "first" => {
                let n = parse_int_value(value, variables)?;
                if n < 0 {
                    return Err(QueryError::cursor_first_must_be_non_negative());
                }
                params.first = Some(n as u64);
            }
            "after" => {
                params.after = Some(resolve_string_value(value, variables, "after")?);
            }
            "last" => {
                let n = parse_int_value(value, variables)?;
                if n < 0 {
                    return Err(QueryError::cursor_last_must_be_non_negative());
                }
                params.last = Some(n as u64);
            }
            "before" => {
                params.before = Some(resolve_string_value(value, variables, "before")?);
            }
            "limit" | "offset" => {
                return Err(QueryError::parse(format!(
                    "Unknown argument \"{name}\" on field \"{}\".",
                    field.name
                )));
            }
            _ => continue,
        }
    }
    Ok(params)
}

/// Parse `_pageInfo { hasNext hasPrev startCursor endCursor }` — returns which fields were
/// selected and the output key to use for each (alias if provided, else canonical name).
fn parse_page_info_selection(field: &Field<'_, String>) -> Result<CursorPageInfoFields> {
    let mut fields = CursorPageInfoFields::default();
    for selection in &field.selection_set.items {
        if let Selection::Field(child) = selection {
            // Output key: alias if provided, else the field's canonical name.
            let output_key = child.alias.clone().unwrap_or_else(|| child.name.clone());
            match child.name.as_str() {
                "hasNext" => fields.has_next = Some(output_key),
                "hasPrev" => fields.has_prev = Some(output_key),
                "startCursor" => fields.start_cursor = Some(output_key),
                "endCursor" => fields.end_cursor = Some(output_key),
                other => {
                    return Err(QueryError::parse(format!(
                        "Cannot query field \"{other}\" on type \"PageInfo\"."
                    )));
                }
            }
        }
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use crate::query_parse::parse_query;

    fn parse(query: &str) -> super::Result<Vec<super::Select>> {
        parse_query(query)
    }

    #[test]
    fn forward_and_backward_args_conflict() {
        let query = r#"{ _cursor { User(first: 10, last: 5, order: {age: ASC}) { name } } }"#;
        let err = parse(query).unwrap_err();
        assert_eq!(
            err.to_string(),
            "forward parameters (first/after) cannot be combined with backward parameters (last/before)"
        );
    }

    #[test]
    fn after_with_last_conflict() {
        let query = r#"{ _cursor { User(after: "abc", last: 5, order: {age: ASC}) { name } } }"#;
        let err = parse(query).unwrap_err();
        assert_eq!(
            err.to_string(),
            "forward parameters (first/after) cannot be combined with backward parameters (last/before)"
        );
    }

    #[test]
    fn negative_first_rejected() {
        let query = r#"{ _cursor { User(first: -1, order: {age: ASC}) { name } } }"#;
        let err = parse(query).unwrap_err();
        assert_eq!(err.to_string(), "first must be non-negative");
    }

    #[test]
    fn empty_cursor_block_rejected() {
        // Only _pageInfo, no collection field — must trigger our "no collection" check.
        // A bare `{ _cursor { } }` is rejected by the GraphQL parser itself (empty selection set).
        let query = "{ _cursor { _pageInfo { hasNext } } }";
        let err = parse(query).unwrap_err();
        assert_eq!(
            err.to_string(),
            "_cursor block must contain exactly one collection query"
        );
    }

    #[test]
    fn multiple_collections_in_cursor_rejected() {
        let query = r#"{ _cursor { User(first: 1, order: {age: ASC}) { name } Book(first: 1, order: {title: ASC}) { title } } }"#;
        let err = parse(query).unwrap_err();
        assert_eq!(
            err.to_string(),
            "_cursor block cannot contain multiple collection queries"
        );
    }

    #[test]
    fn valid_forward_cursor_sets_select_fields() {
        let query = r#"{ _cursor { User(first: 10, after: "abc", order: {age: ASC}) { name } _pageInfo { hasNext startCursor } } }"#;
        let selects = parse(query).unwrap();
        assert_eq!(selects.len(), 1);
        let select = &selects[0];
        assert!(select.is_cursor, "is_cursor must be true");
        let params = select.cursor_params.as_ref().unwrap();
        assert_eq!(params.first, Some(10));
        assert_eq!(params.after, Some("abc".to_string()));
        assert_eq!(select.cursor_page_info.has_next.as_deref(), Some("hasNext"));
        assert_eq!(
            select.cursor_page_info.start_cursor.as_deref(),
            Some("startCursor")
        );
        assert!(select.cursor_page_info.has_prev.is_none());
        assert!(select.cursor_page_info.end_cursor.is_none());
        assert_eq!(select.cursor_aliases.wrapper_alias, None);
        assert_eq!(select.field.name, "User");
    }

    #[test]
    fn negative_last_rejected() {
        let query = r#"{ _cursor { User(last: -1, order: {age: ASC}) { name } } }"#;
        let err = parse(query).unwrap_err();
        assert_eq!(err.to_string(), "last must be non-negative");
    }

    #[test]
    fn limit_arg_rejected_in_cursor_wrapper() {
        let query = r#"{ _cursor { User(first: 5, limit: 10, order: {age: ASC}) { name } } }"#;
        let err = parse(query).unwrap_err().to_string();
        assert!(err.contains("limit"), "error should mention limit: {err}");
    }

    #[test]
    fn offset_arg_rejected_in_cursor_wrapper() {
        let query = r#"{ _cursor { User(first: 5, offset: 3, order: {age: ASC}) { name } } }"#;
        let err = parse(query).unwrap_err().to_string();
        assert!(err.contains("offset"), "error should mention offset: {err}");
    }

    #[test]
    fn page_info_field_aliases_are_preserved() {
        let query = r#"{ _cursor { User(first: 5, order: [{age: ASC}]) { name } _pageInfo { next: hasNext } } }"#;
        let selects = parse(query).unwrap();
        let pi = &selects[0].cursor_page_info;
        assert_eq!(pi.has_next.as_deref(), Some("next"));
        assert!(pi.has_prev.is_none());
        assert!(pi.start_cursor.is_none());
        assert!(pi.end_cursor.is_none());
    }

    #[test]
    fn unknown_page_info_field_rejected() {
        let query =
            r#"{ _cursor { User(first: 5, order: [{age: ASC}]) { name } _pageInfo { bogus } } }"#;
        let err = parse(query).unwrap_err().to_string();
        assert!(
            err.contains("PageInfo") || err.contains("bogus"),
            "should mention the bad field or type: {err}"
        );
    }

    #[test]
    fn wrapper_alias_is_captured() {
        let query = r#"{ paged: _cursor { User(first: 5, order: {age: ASC}) { name } } }"#;
        let selects = parse(query).unwrap();
        assert_eq!(selects.len(), 1);
        let select = &selects[0];
        assert!(select.is_cursor);
        assert_eq!(
            select.cursor_aliases.wrapper_alias,
            Some("paged".to_string()),
            "wrapper alias must be captured from `{{ paged: _cursor {{ ... }} }}`"
        );
        // Inner collection is still User (not aliased), so select.field.name == "User"
        // and select.field.alias is None.
        assert_eq!(select.field.name, "User");
    }
}
