//! Shared subscription helpers for converting live subscription events into scoped queries.
//!
//! Used by both FFI and HTTP code paths to process subscription events.

use std::collections::HashMap;

use graphql_parser::query::{
    Definition, Field, OperationDefinition, Query as GqlQuery, Selection, SelectionSet,
    Subscription, Value,
};
use serde_json::Value as JsonValue;

use crate::error::{QueryError, Result};
use crate::query_parse::{parse_request_with_limits, ParsedOperation};
use crate::{QueryLimits, QueryResponse};

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

    let mut fragments = Vec::new();
    let mut scoped_operation = None;

    for definition in document.definitions {
        match definition {
            Definition::Operation(OperationDefinition::Subscription(subscription))
                if operation_matches(&subscription.name, operation_name) =>
            {
                if scoped_operation.is_some() {
                    return Err(QueryError::parse(
                        "operation name is required when multiple subscriptions are present",
                    ));
                }
                scoped_operation = Some(Definition::Operation(OperationDefinition::Query(
                    scoped_subscription_query(subscription, event_doc_id, event_cid)?,
                )));
            }
            Definition::Fragment(fragment) => fragments.push(Definition::Fragment(fragment)),
            _ => {}
        }
    }

    let Some(operation) = scoped_operation else {
        return Err(QueryError::parse("subscription operation not found"));
    };

    let mut definitions = Vec::with_capacity(1 + fragments.len());
    definitions.push(operation);
    definitions.extend(fragments);

    Ok(graphql_parser::query::Document { definitions }.to_string())
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
    event_doc_id: &str,
    event_cid: &str,
) -> Result<GqlQuery<'static, String>> {
    let mut selection_set = subscription.selection_set;
    scope_root_selection(&mut selection_set, event_doc_id, event_cid)?;

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
    event_doc_id: &str,
    event_cid: &str,
) -> Result<()> {
    if selection_set.items.len() != 1 {
        return Err(QueryError::parse(
            "subscription must have exactly one root field",
        ));
    }

    let Selection::Field(field) = &mut selection_set.items[0] else {
        return Err(QueryError::parse(
            "subscription root selection must be a field",
        ));
    };

    scope_root_field(field, event_doc_id, event_cid);
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
