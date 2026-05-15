//! Schema generation for cursor pagination types.
//!
//! Emits `PageInfo` (singleton), `CursorQuery` (per-schema), and the
//! `_cursor` field that goes on the top-level `Query` (added by generator.rs).
//!
//! Nullability matches Go's `internal/request/graphql/schema/types/cursor.go`
//! and `schema/schema.go:82` — all fields/types bare, no NonNull wrapping.

use super::{GqlArg, GqlField, GqlObjectType, GqlType};

/// Build the `PageInfo` object type. All fields are nullable per Go.
pub fn gen_page_info_type() -> GqlObjectType {
    GqlObjectType::new("PageInfo")
        .with_description("Pagination information for cursor-based queries")
        .with_field(
            GqlField::new("hasNext", GqlType::boolean())
                .with_description("Whether there are more results after the current page"),
        )
        .with_field(
            GqlField::new("hasPrev", GqlType::boolean())
                .with_description("Whether there are results before the current page"),
        )
        .with_field(
            GqlField::new("startCursor", GqlType::string())
                .with_description("Opaque cursor for the first item in the current page"),
        )
        .with_field(
            GqlField::new("endCursor", GqlType::string())
                .with_description("Opaque cursor for the last item in the current page"),
        )
}

/// Build the `CursorQuery` object type shell with only `_pageInfo`.
///
/// Per-collection fields are added by `gen_cursor_collection_field` and
/// registered into this type by the caller (Task 13's generator wiring).
pub fn gen_cursor_query_type() -> GqlObjectType {
    GqlObjectType::new("CursorQuery")
        .with_description("Cursor-based pagination wrapper")
        .with_field(GqlField::new("_pageInfo", GqlType::named("PageInfo")))
}

/// Build a per-collection field for `CursorQuery`.
///
/// Mirrors Go's `genCursorCollectionField` (generate.go:1592-1620).
/// Returns a `[CollectionName]` (nullable list of nullable items) with
/// cursor args (first/after/last/before) plus order/filter/docIDs/cid/groupBy/showDeleted.
///
/// `limit` and `offset` are intentionally absent — cursor args replace them.
/// All arg types and the return type are nullable (no NonNull wrapping).
pub fn gen_cursor_collection_field(collection_name: &str) -> GqlField {
    let order_type_name = format!("{}OrderInput", collection_name);
    let filter_type_name = format!("{}FilterInput", collection_name);
    let group_by_type_name = format!("{}GroupBy", collection_name);

    let return_type = GqlType::list(GqlType::named(collection_name));

    let mut args = cursor_args();
    args.push(GqlArg::new(
        "docIDs",
        GqlType::list(GqlType::non_null(GqlType::id())),
    ));
    args.push(GqlArg::new("cid", GqlType::string()));
    args.push(GqlArg::new("filter", GqlType::named(filter_type_name)));
    args.push(GqlArg::new(
        "groupBy",
        GqlType::list(GqlType::non_null(GqlType::named(group_by_type_name))),
    ));
    args.push(GqlArg::new(
        "order",
        GqlType::list(GqlType::named(order_type_name)),
    ));
    args.push(GqlArg::new("showDeleted", GqlType::boolean()));

    GqlField::new(collection_name, return_type).with_args(args)
}

/// Build the four cursor pagination args (first/after/last/before), all nullable.
pub(crate) fn cursor_args() -> Vec<GqlArg> {
    vec![
        GqlArg::new("first", GqlType::int()),
        GqlArg::new("after", GqlType::string()),
        GqlArg::new("last", GqlType::int()),
        GqlArg::new("before", GqlType::string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find_field<'a>(obj: &'a GqlObjectType, name: &str) -> Option<&'a GqlField> {
        obj.fields.iter().find(|f| f.name == name)
    }

    fn is_non_null(t: &GqlType) -> bool {
        matches!(t, GqlType::NonNull(_))
    }

    fn is_list(t: &GqlType) -> bool {
        matches!(t, GqlType::List(_))
    }

    fn is_named(t: &GqlType, name: &str) -> bool {
        matches!(t, GqlType::Named(n) if n == name)
    }

    fn find_arg<'a>(field: &'a GqlField, name: &str) -> Option<&'a GqlArg> {
        field.args.iter().find(|a| a.name == name)
    }

    #[test]
    fn page_info_has_four_nullable_fields() {
        let pi = gen_page_info_type();

        assert_eq!(pi.name, "PageInfo");
        assert_eq!(pi.fields.len(), 4);

        let has_next = find_field(&pi, "hasNext").expect("hasNext field missing");
        assert!(
            !is_non_null(&has_next.field_type),
            "hasNext must be nullable"
        );
        assert!(is_named(&has_next.field_type, "Boolean"));

        let has_prev = find_field(&pi, "hasPrev").expect("hasPrev field missing");
        assert!(
            !is_non_null(&has_prev.field_type),
            "hasPrev must be nullable"
        );
        assert!(is_named(&has_prev.field_type, "Boolean"));

        let start_cursor = find_field(&pi, "startCursor").expect("startCursor field missing");
        assert!(
            !is_non_null(&start_cursor.field_type),
            "startCursor must be nullable"
        );
        assert!(is_named(&start_cursor.field_type, "String"));

        let end_cursor = find_field(&pi, "endCursor").expect("endCursor field missing");
        assert!(
            !is_non_null(&end_cursor.field_type),
            "endCursor must be nullable"
        );
        assert!(is_named(&end_cursor.field_type, "String"));
    }

    #[test]
    fn cursor_query_type_has_page_info_field() {
        let cq = gen_cursor_query_type();

        assert_eq!(cq.name, "CursorQuery");

        let page_info = find_field(&cq, "_pageInfo").expect("_pageInfo field missing");
        assert!(
            !is_non_null(&page_info.field_type),
            "_pageInfo must be nullable"
        );
        assert!(
            is_named(&page_info.field_type, "PageInfo"),
            "_pageInfo must be PageInfo type, got {:?}",
            page_info.field_type
        );
    }

    #[test]
    fn cursor_collection_field_returns_nullable_list_of_nullable_items() {
        let field = gen_cursor_collection_field("User");

        assert_eq!(field.name, "User");

        // Return type must be List(Named("User")) — bare list of bare items
        // Not NonNull(List(...)) and not List(NonNull(Named(...)))
        assert!(
            is_list(&field.field_type),
            "return type must be a List, got {:?}",
            field.field_type
        );
        assert!(
            !is_non_null(&field.field_type),
            "return type must not be NonNull"
        );
        match &field.field_type {
            GqlType::List(inner) => {
                assert!(
                    !is_non_null(inner),
                    "list item must not be NonNull, got {:?}",
                    inner
                );
                assert!(
                    is_named(inner, "User"),
                    "list item must be Named(User), got {:?}",
                    inner
                );
            }
            other => panic!("expected List, got {:?}", other),
        }

        // Has all cursor args
        assert!(find_arg(&field, "first").is_some(), "first arg missing");
        assert!(find_arg(&field, "after").is_some(), "after arg missing");
        assert!(find_arg(&field, "last").is_some(), "last arg missing");
        assert!(find_arg(&field, "before").is_some(), "before arg missing");

        // Has other standard collection args
        assert!(find_arg(&field, "filter").is_some(), "filter arg missing");
        assert!(find_arg(&field, "order").is_some(), "order arg missing");
        assert!(find_arg(&field, "docIDs").is_some(), "docIDs arg missing");
        assert!(find_arg(&field, "cid").is_some(), "cid arg missing");
        assert!(find_arg(&field, "groupBy").is_some(), "groupBy arg missing");
        assert!(
            find_arg(&field, "showDeleted").is_some(),
            "showDeleted arg missing"
        );

        // Must NOT have limit or offset
        assert!(
            find_arg(&field, "limit").is_none(),
            "limit arg must not be present (cursor replaces it)"
        );
        assert!(
            find_arg(&field, "offset").is_none(),
            "offset arg must not be present (cursor replaces it)"
        );
    }

    #[test]
    fn cursor_args_are_all_nullable() {
        let args = cursor_args();

        assert_eq!(args.len(), 4);

        let first = args
            .iter()
            .find(|a| a.name == "first")
            .expect("first missing");
        assert!(!is_non_null(&first.arg_type), "first must be nullable");
        assert!(is_named(&first.arg_type, "Int"));

        let after = args
            .iter()
            .find(|a| a.name == "after")
            .expect("after missing");
        assert!(!is_non_null(&after.arg_type), "after must be nullable");
        assert!(is_named(&after.arg_type, "String"));

        let last = args
            .iter()
            .find(|a| a.name == "last")
            .expect("last missing");
        assert!(!is_non_null(&last.arg_type), "last must be nullable");
        assert!(is_named(&last.arg_type, "Int"));

        let before = args
            .iter()
            .find(|a| a.name == "before")
            .expect("before missing");
        assert!(!is_non_null(&before.arg_type), "before must be nullable");
        assert!(is_named(&before.arg_type, "String"));
    }
}
