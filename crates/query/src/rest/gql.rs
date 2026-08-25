//! Building the GraphQL documents the REST surface runs.
//!
//! Every REST operation is a GraphQL query or mutation underneath, so the
//! document text is assembled here, in one place, rather than inline at each
//! call site. Keeping it together is also what keeps the trust boundary in one
//! place: `json_to_graphql_input` is where caller-supplied JSON becomes
//! GraphQL syntax, and it is the only thing standing between a request body
//! and the document that gets executed.

use serde_json::Value as JsonValue;

use super::error::{RestError, RestResult};
use super::trait_def::CollectionDocIdsPagination;

/// A GraphQL `Name`: `/[_A-Za-z][_0-9A-Za-z]*/` (GraphQL spec 2.1.9).
fn is_graphql_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

/// Convert a JSON value to GraphQL input syntax.
///
/// Object keys are bare identifiers (GraphQL input objects), not JSON-quoted keys.
/// String leaves use JSON string encoding, which is a valid GraphQL `StringValue`
/// and matches Go DefraDB's test harness (`valueToGQL` → `encoding/json.Marshal`),
/// including `\b`, `\f`, and `\uXXXX` for other controls.
///
/// Keys are the trust boundary. They are written into the mutation unquoted, so
/// a key carrying `)`, `{` or `}` would close the argument list and let the
/// caller append operations of its own. Anything that is not a GraphQL `Name`
/// is refused here rather than escaped: a key that is not a `Name` cannot
/// address a schema field, so nothing legitimate is being turned away.
pub(super) fn json_to_graphql_input(value: &JsonValue) -> RestResult<String> {
    Ok(match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => {
            serde_json::to_string(s).expect("serializing a string to JSON cannot fail")
        }
        JsonValue::Array(arr) => {
            let items = arr
                .iter()
                .map(json_to_graphql_input)
                .collect::<RestResult<Vec<_>>>()?;
            format!("[{}]", items.join(", "))
        }
        JsonValue::Object(obj) => {
            let fields = obj
                .iter()
                .map(|(k, v)| {
                    if !is_graphql_name(k) {
                        return Err(RestError::invalid_input(format!(
                            "'{k}' is not a valid field name"
                        )));
                    }
                    Ok(format!("{}: {}", k, json_to_graphql_input(v)?))
                })
                .collect::<RestResult<Vec<_>>>()?;
            format!("{{{}}}", fields.join(", "))
        }
    })
}

pub(super) fn build_list_ids_query(
    collection: &str,
    pagination: Option<CollectionDocIdsPagination>,
) -> String {
    match pagination {
        Some(pagination) => format!(
            r#"{{ {collection}(limit: {limit}, offset: {offset}) {{ _docID }} }}"#,
            collection = collection,
            limit = pagination.limit,
            offset = pagination.offset
        ),
        None => format!(
            r#"{{ {collection} {{ _docID }} }}"#,
            collection = collection
        ),
    }
}

pub(super) fn build_count_query(collection: &str) -> String {
    format!(
        r#"{{ total: COUNT({collection}: {{}}) }}"#,
        collection = collection
    )
}

pub(super) fn build_create_mutation(collection: &str, data: &JsonValue) -> RestResult<String> {
    let graphql_data = json_to_graphql_input(data)?;
    Ok(format!(
        r#"mutation {{ add_{collection}(input: [{graphql_data}]) {{ _docID }} }}"#,
        collection = collection,
        graphql_data = graphql_data
    ))
}

pub(super) fn build_create_many_mutation(
    collection: &str,
    docs: &[JsonValue],
) -> RestResult<String> {
    let inputs = docs
        .iter()
        .map(json_to_graphql_input)
        .collect::<RestResult<Vec<_>>>()?;
    Ok(format!(
        r#"mutation {{ add_{collection}(input: [{inputs}]) {{ _docID }} }}"#,
        collection = collection,
        inputs = inputs.join(", ")
    ))
}

pub(super) fn build_update_mutation(
    collection: &str,
    doc_id: &str,
    patch: &JsonValue,
) -> RestResult<String> {
    let graphql_patch = json_to_graphql_input(patch)?;
    Ok(format!(
        r#"mutation {{ update_{collection}(docIDs: ["{doc_id}"], input: {graphql_patch}) {{ _docID }} }}"#,
        collection = collection,
        doc_id = doc_id,
        graphql_patch = graphql_patch
    ))
}

pub(super) fn build_delete_mutation(collection: &str, doc_id: &str) -> String {
    format!(
        r#"mutation {{ delete_{collection}(docIDs: ["{doc_id}"]) {{ _docID }} }}"#,
        collection = collection,
        doc_id = doc_id
    )
}

pub(super) fn build_filtered_delete_mutation(
    collection: &str,
    filter: &JsonValue,
) -> RestResult<String> {
    Ok(format!(
        r#"mutation {{ delete_{collection}(filter: {filter}) {{ _docID }} }}"#,
        collection = collection,
        filter = json_to_graphql_input(filter)?
    ))
}
