//! Shared subscription helpers for converting live subscription events into scoped queries.
//!
//! Used by both FFI and HTTP code paths to process subscription events.

use std::collections::{HashMap, HashSet};

use graphql_parser::query::{
    Definition, Field, FragmentDefinition, OperationDefinition, Query as GqlQuery, Selection,
    SelectionSet, Subscription, Value,
};
use serde_json::Value as JsonValue;

use crate::error::{QueryError, Result};
use crate::query_parse::{parse_request_with_limits, ParsedOperation};
use crate::{QueryLimits, QueryResponse};

type FragmentMap<'a> = HashMap<String, &'a FragmentDefinition<'static, String>>;

/// Check whether the provided request resolves to a subscription operation.
pub fn is_subscription_operation(
    query: &str,
    variables: Option<&JsonValue>,
    operation_name: Option<&str>,
) -> bool {
    is_subscription_operation_with_limits(query, variables, operation_name, QueryLimits::default())
}

/// Check whether the provided request resolves to a subscription operation with custom limits.
pub fn is_subscription_operation_with_limits(
    query: &str,
    variables: Option<&JsonValue>,
    operation_name: Option<&str>,
    limits: QueryLimits,
) -> bool {
    let variable_map = variables_to_map(variables);
    matches!(
        parse_request_with_limits(query, variable_map.as_ref(), operation_name, limits),
        Ok(ParsedOperation::Subscription { .. })
    )
}

/// Extract the parsed docID/docIDs filter from a subscription root field.
pub fn subscription_doc_ids(
    query: &str,
    variables: Option<&JsonValue>,
    operation_name: Option<&str>,
) -> Option<Vec<String>> {
    subscription_doc_ids_with_limits(query, variables, operation_name, QueryLimits::default())
}

/// Extract the parsed docID/docIDs filter from a subscription root field with custom limits.
pub fn subscription_doc_ids_with_limits(
    query: &str,
    variables: Option<&JsonValue>,
    operation_name: Option<&str>,
    limits: QueryLimits,
) -> Option<Vec<String>> {
    let variable_map = variables_to_map(variables);
    match parse_request_with_limits(query, variable_map.as_ref(), operation_name, limits).ok()? {
        ParsedOperation::Subscription { select } => select.doc_ids.clone(),
        _ => None,
    }
}

/// Return whether a live update belongs to the subscription's explicit docID filter.
pub fn subscription_accepts_doc_id(doc_ids: Option<&[String]>, event_doc_id: &str) -> bool {
    doc_ids
        .map(|ids| ids.iter().any(|id| id == event_doc_id))
        .unwrap_or(true)
}

/// Convert a subscription operation into a query scoped to the live update's docID and CID.
///
/// Normal collection subscriptions are narrowed by both `docID` and `cid`, matching Go's
/// `Select.ToSubscriptionSelect(docID, cid)` semantics. `_commits` subscriptions preserve
/// the original arguments and replace only `cid`, matching Go's commit subscription path.
pub fn subscription_to_scoped_query(
    subscription_query: &str,
    event_doc_id: &str,
    event_cid: &str,
    operation_name: Option<&str>,
) -> Result<String> {
    let document = graphql_parser::parse_query::<String>(subscription_query)
        .map_err(|err| QueryError::parse(err.to_string()))?
        .into_static();

    // Fragments are collected before anything is scoped: a definition may
    // appear after the operation that spreads it.
    let (fragments, operations): (Vec<_>, Vec<_>) = document
        .definitions
        .into_iter()
        .partition(|definition| matches!(definition, Definition::Fragment(_)));
    let fragment_map = fragment_map(&fragments);

    let mut operations = operations;
    let selected = matching_subscription(&operations, operation_name)?;
    let Definition::Operation(OperationDefinition::Subscription(subscription)) =
        operations.swap_remove(selected)
    else {
        return Err(QueryError::parse("subscription operation not found"));
    };
    let operation = Definition::Operation(OperationDefinition::Query(scoped_subscription_query(
        subscription,
        &fragment_map,
        event_doc_id,
        event_cid,
    )?));

    let mut definitions = Vec::with_capacity(1 + fragments.len());
    definitions.push(operation);
    definitions.extend(fragments);

    Ok(graphql_parser::query::Document { definitions }.to_string())
}

fn fragment_map<'a>(definitions: &'a [Definition<'static, String>]) -> FragmentMap<'a> {
    definitions
        .iter()
        .filter_map(|definition| match definition {
            Definition::Fragment(fragment) => Some((fragment.name.clone(), fragment)),
            _ => None,
        })
        .collect()
}

/// Validate a subscription document without executing it.
///
/// [`subscription_to_scoped_query`] enforces the same rules, but it runs once
/// per event from inside an already-open stream, where the only place left to
/// report a failure is the log -- so a document it will always reject is
/// answered with a healthy stream that then delivers nothing. Calling this
/// before the stream opens is what lets the caller be told instead.
pub fn validate_subscription(subscription_query: &str, operation_name: Option<&str>) -> Result<()> {
    let document = graphql_parser::parse_query::<String>(subscription_query)
        .map_err(|err| QueryError::parse(err.to_string()))?
        .into_static();

    let (fragments, operations): (Vec<_>, Vec<_>) = document
        .definitions
        .into_iter()
        .partition(|definition| matches!(definition, Definition::Fragment(_)));
    let fragment_map = fragment_map(&fragments);

    let selected = matching_subscription(&operations, operation_name)?;
    let Definition::Operation(OperationDefinition::Subscription(subscription)) =
        &operations[selected]
    else {
        return Err(QueryError::parse("subscription operation not found"));
    };
    single_root_field(&subscription.selection_set, &fragment_map).map(|_| ())
}

/// Index of the one subscription operation a request selects.
///
/// Shared by validation and scoping so the two cannot disagree about which
/// operation a request names, or about how many is too many.
fn matching_subscription(
    operations: &[Definition<'static, String>],
    operation_name: Option<&str>,
) -> Result<usize> {
    let mut selected = None;
    for (index, definition) in operations.iter().enumerate() {
        let Definition::Operation(OperationDefinition::Subscription(subscription)) = definition
        else {
            continue;
        };
        if !operation_matches(&subscription.name, operation_name) {
            continue;
        }
        if selected.is_some() {
            return Err(QueryError::parse(
                "operation name is required when multiple subscriptions are present",
            ));
        }
        selected = Some(index);
    }
    selected.ok_or_else(|| QueryError::parse("subscription operation not found"))
}

fn variables_to_map(variables: Option<&JsonValue>) -> Option<HashMap<String, JsonValue>> {
    variables.and_then(|value| {
        value.as_object().map(|map| {
            map.iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
    })
}

fn operation_matches(name: &Option<String>, operation_name: Option<&str>) -> bool {
    operation_name
        .map(|target| name.as_deref() == Some(target))
        .unwrap_or(true)
}

fn scoped_subscription_query(
    subscription: Subscription<'static, String>,
    fragments: &FragmentMap<'_>,
    event_doc_id: &str,
    event_cid: &str,
) -> Result<GqlQuery<'static, String>> {
    let mut selection_set = subscription.selection_set;
    scope_root_selection(&mut selection_set, fragments, event_doc_id, event_cid)?;

    Ok(GqlQuery {
        position: subscription.position,
        name: subscription.name,
        variable_definitions: subscription.variable_definitions,
        directives: subscription.directives,
        selection_set,
    })
}

fn scope_root_selection(
    selection_set: &mut SelectionSet<'static, String>,
    fragments: &FragmentMap<'_>,
    event_doc_id: &str,
    event_cid: &str,
) -> Result<()> {
    let mut field = single_root_field(selection_set, fragments)?;
    scope_root_field(&mut field, event_doc_id, event_cid);
    // The root becomes the resolved field. A fragment that fed only the root
    // is left unreferenced in the emitted document, which the executor's
    // parser does not mind.
    selection_set.items = vec![Selection::Field(field)];
    Ok(())
}

/// Resolve a subscription's root selection to the one field it selects.
///
/// A root fragment is legal GraphQL: the spec's "exactly one root field" rule
/// (5.2.3.1) is about the selection after fragments are expanded, not before.
/// Expansion mirrors the query parser -- same cycle detection, and inline
/// fragment type conditions are ignored for the same reason, that DefraDB has
/// no interface or union types.
fn single_root_field(
    selection_set: &SelectionSet<'static, String>,
    fragments: &FragmentMap<'_>,
) -> Result<Field<'static, String>> {
    let mut fields = Vec::new();
    collect_root_fields(
        &selection_set.items,
        fragments,
        &mut HashSet::new(),
        &mut fields,
    )?;

    if fields.len() != 1 {
        return Err(QueryError::parse(format!(
            "subscription must have exactly one root field, found {}",
            fields.len()
        )));
    }
    Ok(fields.remove(0))
}

fn collect_root_fields(
    items: &[Selection<'static, String>],
    fragments: &FragmentMap<'_>,
    visiting: &mut HashSet<String>,
    out: &mut Vec<Field<'static, String>>,
) -> Result<()> {
    for item in items {
        match item {
            Selection::Field(field) => out.push(field.clone()),
            Selection::InlineFragment(inline) => {
                collect_root_fields(&inline.selection_set.items, fragments, visiting, out)?;
            }
            Selection::FragmentSpread(spread) => {
                if !visiting.insert(spread.fragment_name.clone()) {
                    return Err(QueryError::parse(format!(
                        "circular fragment reference detected: '{}'",
                        spread.fragment_name
                    )));
                }
                let fragment = fragments.get(&spread.fragment_name).ok_or_else(|| {
                    QueryError::parse(format!("Unknown fragment \"{}\".", spread.fragment_name))
                })?;
                collect_root_fields(&fragment.selection_set.items, fragments, visiting, out)?;
                visiting.remove(&spread.fragment_name);
            }
        }
    }
    Ok(())
}

fn scope_root_field(field: &mut Field<'static, String>, event_doc_id: &str, event_cid: &str) {
    if field.name == "_commits" {
        replace_argument(field, "cid", cid_argument(event_cid));
    } else {
        remove_argument(field, "docID");
        remove_argument(field, "docIDs");
        replace_argument(field, "cid", cid_argument(event_cid));
        field
            .arguments
            .push(("docID".to_string(), Value::String(event_doc_id.to_string())));
    }
}

fn replace_argument(field: &mut Field<'static, String>, name: &str, value: Value<'static, String>) {
    remove_argument(field, name);
    field.arguments.push((name.to_string(), value));
}

fn remove_argument(field: &mut Field<'static, String>, name: &str) {
    field.arguments.retain(|(arg_name, _)| arg_name != name);
}

fn cid_argument(cid: &str) -> Value<'static, String> {
    Value::List(vec![Value::String(cid.to_string())])
}

/// Check if a query response has any non-empty data.
///
/// Returns false if data is null/empty or all top-level collection arrays are empty
/// (indicating the subscription's filter excluded the document).
pub fn response_has_data(response: &QueryResponse) -> bool {
    if let Some(serde_json::Value::Object(map)) = response.data.as_ref() {
        for value in map.values() {
            if let serde_json::Value::Array(arr) = value {
                if !arr.is_empty() {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A root fragment is legal GraphQL and has to reach the same scoped query
    /// a plain root field does.
    #[test]
    fn a_root_fragment_resolves_to_the_field_it_selects() {
        let plain =
            subscription_to_scoped_query("subscription { User { name } }", "bae-1", "cid-1", None)
                .unwrap();

        for query in [
            "subscription { ...F } fragment F on Subscription { User { name } }",
            "subscription { ... on Subscription { User { name } } }",
            // Nested, and defined before the operation that spreads it.
            "fragment Inner on Subscription { User { name } } \
             fragment Outer on Subscription { ...Inner } \
             subscription { ...Outer }",
        ] {
            let scoped = subscription_to_scoped_query(query, "bae-1", "cid-1", None).unwrap();
            let (scoped_op, _) = scoped.split_once("fragment").unwrap_or((&scoped, ""));
            let (plain_op, _) = plain.split_once("fragment").unwrap_or((&plain, ""));
            assert_eq!(scoped_op.trim(), plain_op.trim(), "for: {query}");
        }
    }

    #[test]
    fn a_fragment_expanding_to_two_fields_is_still_refused() {
        let query =
            "subscription { ...F } fragment F on Subscription { User { name } Device { label } }";
        let error = validate_subscription(query, None).unwrap_err().to_string();
        assert!(error.contains("exactly one root field"), "{error}");
        assert!(subscription_to_scoped_query(query, "bae-1", "cid-1", None).is_err());
    }

    #[test]
    fn a_circular_fragment_is_refused_rather_than_looping() {
        let query = "subscription { ...A } fragment A on Subscription { ...B } \
                     fragment B on Subscription { ...A }";
        let error = validate_subscription(query, None).unwrap_err().to_string();
        assert!(error.contains("circular fragment reference"), "{error}");
    }

    #[test]
    fn an_unknown_fragment_is_named() {
        let error = validate_subscription("subscription { ...Missing }", None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("Unknown fragment"), "{error}");
    }

    #[test]
    fn an_operation_name_selects_among_several_subscriptions() {
        let document = "subscription Watch { User { name } } \
                        subscription Other { Device { label } }";

        // Ambiguous without a name, on both paths.
        assert!(validate_subscription(document, None).is_err());
        assert!(subscription_to_scoped_query(document, "bae-1", "cid-1", None).is_err());

        validate_subscription(document, Some("Watch")).unwrap();
        let scoped =
            subscription_to_scoped_query(document, "bae-1", "cid-1", Some("Watch")).unwrap();
        assert!(scoped.contains("User"), "{scoped}");
        assert!(!scoped.contains("Device"), "{scoped}");

        let scoped =
            subscription_to_scoped_query(document, "bae-1", "cid-1", Some("Other")).unwrap();
        assert!(scoped.contains("Device"), "{scoped}");

        assert!(validate_subscription(document, Some("Missing")).is_err());
    }

    #[test]
    fn validation_agrees_with_scoping() {
        // Whatever validate_subscription accepts, scoping must handle. If the
        // two disagree, a document that opens a stream it can never fire
        // reaches the caller as silence.
        for query in [
            "subscription { User { name } }",
            "subscription { ...F } fragment F on Subscription { User { name } }",
            "subscription { ... on Subscription { User { name } } }",
            "subscription { _commits { cid } }",
        ] {
            assert_eq!(
                validate_subscription(query, None).is_ok(),
                subscription_to_scoped_query(query, "bae-1", "cid-1", None).is_ok(),
                "disagreement for: {query}"
            );
        }
    }

    fn parse_scoped_select(
        query: &str,
        variables: Option<&JsonValue>,
        operation_name: Option<&str>,
    ) -> crate::Select {
        match crate::query_parse::parse_request_with_variables(
            query,
            variables_to_map(variables).as_ref(),
            operation_name,
        )
        .expect("scoped query should parse")
        {
            ParsedOperation::Query { mut selects, .. } => {
                assert_eq!(selects.len(), 1);
                selects.remove(0)
            }
            other => panic!("expected scoped query, got {other:?}"),
        }
    }

    #[test]
    fn scopes_collection_subscription_by_event_doc_id_and_cid() {
        let scoped = subscription_to_scoped_query(
            r#"subscription { User(docID: "old-doc", filter: {active: {_eq: true}}) { _docID name } }"#,
            "event-doc",
            "event-cid",
            None,
        )
        .expect("subscription should scope");

        let select = parse_scoped_select(&scoped, None, None);
        assert_eq!(select.collection_name, "User");
        assert_eq!(select.doc_ids, Some(vec!["event-doc".to_string()]));
        assert_eq!(select.cid, Some(vec!["event-cid".to_string()]));
        assert!(select.filter.is_some());
    }

    #[test]
    fn scopes_commits_subscription_by_cid_without_dropping_doc_id() {
        let scoped = subscription_to_scoped_query(
            r#"subscription { _commits(docID: ["target-doc"], cid: ["old-cid"], depth: 3) { cid docID } }"#,
            "ignored-doc",
            "event-cid",
            None,
        )
        .expect("commit subscription should scope");

        let select = parse_scoped_select(&scoped, None, None);
        assert_eq!(select.collection_name, "_commits");
        assert_eq!(select.doc_ids, Some(vec!["target-doc".to_string()]));
        assert_eq!(select.cid, Some(vec!["event-cid".to_string()]));
        assert_eq!(select.depth, Some(3));
    }

    #[test]
    fn resolves_subscription_doc_ids_from_variables() {
        let query = r#"
            subscription WatchUser($id: ID!, $active: Boolean!) {
                User(docID: $id, filter: {active: {_eq: $active}}) {
                    _docID
                }
            }
        "#;
        let variables = json!({"id": "target-doc", "active": true});

        assert!(is_subscription_operation(
            query,
            Some(&variables),
            Some("WatchUser"),
        ));
        assert_eq!(
            subscription_doc_ids(query, Some(&variables), Some("WatchUser")),
            Some(vec!["target-doc".to_string()])
        );

        let scoped =
            subscription_to_scoped_query(query, "event-doc", "event-cid", Some("WatchUser"))
                .expect("subscription should scope");
        let select = parse_scoped_select(&scoped, Some(&variables), Some("WatchUser"));

        assert_eq!(select.doc_ids, Some(vec!["event-doc".to_string()]));
        assert_eq!(select.cid, Some(vec!["event-cid".to_string()]));
        assert!(select.filter.is_some());
    }
}
