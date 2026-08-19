//! Parser for the `_cursor` GraphQL wrapper field.
//!
//! Mirrors Go's `internal/request/graphql/parser/cursor.go` semantics.

use std::collections::{HashMap, HashSet};

use graphql_parser::query::{Field, Selection, Value};
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
    let mut page_info_seen = false;
    let mut inner_count = 0usize;

    // Flatten the wrapper selection set, expanding fragments transparently
    // (mirrors Go's gql.CollectFields, which never errors on fragments).
    let mut flattened: Vec<&'a Field<'a, String>> = Vec::new();
    collect_fields(&field.selection_set.items, fragments, &mut flattened)?;

    for child in flattened {
        if child.name == "_pageInfo" {
            if page_info_seen {
                return Err(QueryError::parse(
                    "_cursor block cannot contain multiple _pageInfo selections".to_string(),
                ));
            }
            page_info_seen = true;
            page_info_fields = parse_page_info_selection(child, fragments)?;
        } else {
            inner_count += 1;
            if inner_field.is_none() {
                inner_field = Some(child);
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
                // Go treats a null cursor argument as absent (start from the
                // beginning), not an error.
                if is_null_value(value, variables) {
                    continue;
                }
                let token = resolve_string_value(value, variables, "after")?;
                if token.is_empty() {
                    return Err(QueryError::cursor_invalid());
                }
                params.after = Some(token);
            }
            "last" => {
                let n = parse_int_value(value, variables)?;
                if n < 0 {
                    return Err(QueryError::cursor_last_must_be_non_negative());
                }
                params.last = Some(n as u64);
            }
            "before" => {
                if is_null_value(value, variables) {
                    continue;
                }
                let token = resolve_string_value(value, variables, "before")?;
                if token.is_empty() {
                    return Err(QueryError::cursor_invalid());
                }
                params.before = Some(token);
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
/// selected. The output key is always the field's canonical name, regardless of any GraphQL
/// alias (mirrors Go's planner, which keys `PageInfo()` by `request.HasNextFieldName` etc.).
fn parse_page_info_selection<'a>(
    field: &'a Field<'a, String>,
    fragments: &FragmentMap<'a>,
) -> Result<CursorPageInfoFields> {
    let mut fields = CursorPageInfoFields::default();
    let mut flattened: Vec<&'a Field<'a, String>> = Vec::new();
    collect_fields(&field.selection_set.items, fragments, &mut flattened)?;
    for child in flattened {
        // Output key: the field's canonical name (aliases are discarded, matching Go).
        match child.name.as_str() {
            "hasNext" => fields.has_next = Some(child.name.clone()),
            "hasPrev" => fields.has_prev = Some(child.name.clone()),
            "startCursor" => fields.start_cursor = Some(child.name.clone()),
            "endCursor" => fields.end_cursor = Some(child.name.clone()),
            other => {
                return Err(QueryError::parse(format!(
                    "Cannot query field \"{other}\" on type \"PageInfo\"."
                )));
            }
        }
    }
    Ok(fields)
}

/// Flatten a selection set into plain fields, transparently expanding `FragmentSpread`
/// and `InlineFragment` selections (mirrors Go's `gql.CollectFields`).
///
/// Unknown fragment names error (Go would fail resolution too); valid fragments expand.
fn collect_fields<'a>(
    selections: &'a [Selection<'a, String>],
    fragments: &FragmentMap<'a>,
    out: &mut Vec<&'a Field<'a, String>>,
) -> Result<()> {
    for selection in selections {
        match selection {
            Selection::Field(child) => out.push(child),
            Selection::FragmentSpread(spread) => {
                let frag = fragments.get(&spread.fragment_name).ok_or_else(|| {
                    QueryError::parse(format!("Unknown fragment \"{}\".", spread.fragment_name))
                })?;
                collect_fields(&frag.selection_set.items, fragments, out)?;
            }
            Selection::InlineFragment(inline) => {
                collect_fields(&inline.selection_set.items, fragments, out)?;
            }
        }
    }
    Ok(())
}

/// Returns true if a GraphQL argument value is `null`, or a variable that resolves to null.
fn is_null_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> bool {
    match value {
        Value::Null => true,
        Value::Variable(name) => variables
            .and_then(|vars| vars.get(name))
            .map(JsonValue::is_null)
            .unwrap_or(false),
        _ => false,
    }
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
    fn empty_after_token_rejected() {
        // Go's CursorSelect.Validate() rejects an empty `after`/`before` with ErrInvalidCursor.
        let query = r#"{ _cursor { User(after: "", order: {age: ASC}) { name } } }"#;
        let err = parse(query).unwrap_err();
        assert_eq!(err.to_string(), "invalid cursor");
    }

    #[test]
    fn empty_before_token_rejected() {
        let query = r#"{ _cursor { User(before: "", order: {age: ASC}) { name } } }"#;
        let err = parse(query).unwrap_err();
        assert_eq!(err.to_string(), "invalid cursor");
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
    fn page_info_field_aliases_are_ignored() {
        // Go discards _pageInfo subfield aliases and always renders canonical names.
        let query = r#"{ _cursor { User(first: 5, order: [{age: ASC}]) { name } _pageInfo { next: hasNext } } }"#;
        let selects = parse(query).unwrap();
        let pi = &selects[0].cursor_page_info;
        assert_eq!(pi.has_next.as_deref(), Some("hasNext"));
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
    fn page_info_block_alias_is_ignored() {
        // Go discards the _pageInfo block alias; the block always renders as `_pageInfo`.
        // The selection itself still parses, and no alias is tracked.
        let query = r#"
            { _cursor {
                User(first: 5, order: [{age: ASC}]) { name }
                info: _pageInfo { hasNext }
            } }
        "#;
        let selects = parse(query).unwrap();
        let select = &selects[0];
        assert_eq!(select.cursor_page_info.has_next.as_deref(), Some("hasNext"));
        assert_eq!(select.cursor_aliases.wrapper_alias, None);
    }

    #[test]
    fn fragments_in_page_info_expand() {
        // Go expands fragments inside _pageInfo via gql.CollectFields.
        let query = r#"
            fragment F on PageInfo { hasNext endCursor }
            { _cursor {
                User(first: 5, order: [{age: ASC}]) { name }
                _pageInfo { ...F }
            } }
        "#;
        let selects = parse(query).unwrap();
        let pi = &selects[0].cursor_page_info;
        assert_eq!(pi.has_next.as_deref(), Some("hasNext"));
        assert_eq!(pi.end_cursor.as_deref(), Some("endCursor"));
        assert!(pi.has_prev.is_none());
        assert!(pi.start_cursor.is_none());
    }

    #[test]
    fn fragments_in_cursor_wrapper_expand() {
        // A fragment spread at the _cursor level expands to the collection field.
        let query = r#"
            fragment Page on Query { User(first: 5, order: [{age: ASC}]) { name } }
            { _cursor {
                ...Page
                _pageInfo { hasNext }
            } }
        "#;
        let selects = parse(query).unwrap();
        let select = &selects[0];
        assert!(select.is_cursor);
        assert_eq!(select.field.name, "User");
        assert_eq!(select.cursor_page_info.has_next.as_deref(), Some("hasNext"));
    }

    #[test]
    fn unknown_fragment_in_cursor_wrapper_rejected() {
        let query = r#"
            { _cursor {
                ...Missing
                _pageInfo { hasNext }
            } }
        "#;
        let err = parse(query).unwrap_err().to_string();
        assert!(err.contains("Unknown fragment"), "got: {err}");
    }

    #[test]
    fn after_null_treated_as_no_cursor() {
        // Go treats `after: null` as absent (start from the beginning), not an error.
        let query = r#"{ _cursor { User(first: 5, after: null, order: {age: ASC}) { name } } }"#;
        let selects = parse(query).unwrap();
        let params = selects[0].cursor_params.as_ref().unwrap();
        assert_eq!(params.first, Some(5));
        assert_eq!(params.after, None);
    }

    #[test]
    fn before_null_treated_as_no_cursor() {
        let query = r#"{ _cursor { User(last: 5, before: null, order: {age: ASC}) { name } } }"#;
        let selects = parse(query).unwrap();
        let params = selects[0].cursor_params.as_ref().unwrap();
        assert_eq!(params.last, Some(5));
        assert_eq!(params.before, None);
    }

    #[test]
    fn multiple_page_info_selections_rejected() {
        let query = r#"
            { _cursor {
                User(first: 5, order: [{age: ASC}]) { name }
                _pageInfo { hasNext }
                info: _pageInfo { hasPrev }
            } }
        "#;
        let err = parse(query).unwrap_err().to_string();
        assert!(
            err.contains("multiple _pageInfo"),
            "should reject duplicate _pageInfo: {err}"
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
