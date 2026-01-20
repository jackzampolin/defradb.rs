//! GraphQL query parser
//!
//! Parses GraphQL query strings into Select and Mutation operations for execution.

use graphql_parser::query::{
    Definition, Document, Field, OperationDefinition, Selection, SelectionSet, Value,
};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{
    parse_mutation_name, Aggregate, AggregateTarget, AggregateType, Field as SelectField, Filter,
    GroupBy, Limit, Mutation, MutationType, OrderBy, OrderCondition, OrderDirection, Requestable,
    Select,
};

/// Result of parsing a GraphQL request.
#[derive(Debug)]
pub enum ParsedOperation {
    /// Query operations (SELECT)
    Query(Vec<Select>),
    /// Mutation operations (CREATE, UPDATE, DELETE)
    Mutation(Vec<Mutation>),
}

/// Parse a GraphQL query string into Select operations.
///
/// Returns a vector of Select operations, one for each top-level field in the query.
/// For mutations, use `parse_request` instead.
pub fn parse_query(query: &str) -> Result<Vec<Select>> {
    match parse_request(query)? {
        ParsedOperation::Query(selects) => Ok(selects),
        ParsedOperation::Mutation(_) => Err(QueryError::parse(
            "Expected query but got mutation. Use parse_request() for mutations.",
        )),
    }
}

/// Parse a GraphQL mutation string into Mutation operations.
///
/// Returns a vector of Mutation operations, one for each top-level field in the mutation.
pub fn parse_mutations(query: &str) -> Result<Vec<Mutation>> {
    match parse_request(query)? {
        ParsedOperation::Mutation(mutations) => Ok(mutations),
        ParsedOperation::Query(_) => Err(QueryError::parse("Expected mutation but got query")),
    }
}

/// Parse a GraphQL request (query or mutation) into operations.
///
/// This is the main entry point for parsing GraphQL requests.
pub fn parse_request(query: &str) -> Result<ParsedOperation> {
    let doc: Document<'_, String> =
        graphql_parser::parse_query(query).map_err(|e| QueryError::parse(e.to_string()))?;

    let mut selects = Vec::new();
    let mut mutations = Vec::new();
    let mut has_query = false;
    let mut has_mutation = false;

    for def in doc.definitions {
        match def {
            Definition::Operation(op) => {
                match op {
                    OperationDefinition::Query(q) => {
                        has_query = true;
                        for selection in q.selection_set.items {
                            if let Selection::Field(field) = selection {
                                let select = parse_field_to_select(&field)?;
                                selects.push(select);
                            }
                        }
                    }
                    OperationDefinition::SelectionSet(ss) => {
                        // Bare selection set is treated as query
                        has_query = true;
                        for selection in ss.items {
                            if let Selection::Field(field) = selection {
                                let select = parse_field_to_select(&field)?;
                                selects.push(select);
                            }
                        }
                    }
                    OperationDefinition::Mutation(m) => {
                        has_mutation = true;
                        for selection in m.selection_set.items {
                            if let Selection::Field(field) = selection {
                                let mutation = parse_field_to_mutation(&field)?;
                                mutations.push(mutation);
                            }
                        }
                    }
                    OperationDefinition::Subscription(_) => {
                        return Err(QueryError::parse("subscriptions not supported"))
                    }
                };
            }
            Definition::Fragment(_) => {
                return Err(QueryError::parse("fragments not yet supported"))
            }
        }
    }

    // Cannot mix queries and mutations
    if has_query && has_mutation {
        return Err(QueryError::parse(
            "Cannot mix queries and mutations in same request",
        ));
    }

    if has_mutation {
        Ok(ParsedOperation::Mutation(mutations))
    } else {
        Ok(ParsedOperation::Query(selects))
    }
}

/// Parse a single GraphQL field into a Select operation.
fn parse_field_to_select(field: &Field<'_, String>) -> Result<Select> {
    let collection_name = field.name.clone();
    let alias = field.alias.clone();

    let mut select = Select::new(&collection_name);
    if let Some(a) = alias {
        select.field = SelectField::with_alias(&collection_name, a);
    }

    // Parse arguments (filter, limit, offset, order, docIDs, etc.)
    for (arg_name, arg_value) in &field.arguments {
        match arg_name.as_str() {
            "filter" => {
                let filter = parse_filter_value(arg_value)?;
                select.filter = Some(filter);
            }
            "limit" => {
                let limit_val = parse_int_value(arg_value)?;
                if limit_val < 0 {
                    return Err(QueryError::parse("limit must be non-negative"));
                }
                select.limit = Some(Limit::new(
                    Some(limit_val as u64),
                    select.limit.as_ref().map(|l| l.offset).unwrap_or(0),
                ));
            }
            "offset" => {
                let offset_val = parse_int_value(arg_value)?;
                if offset_val < 0 {
                    return Err(QueryError::parse("offset must be non-negative"));
                }
                select.limit = Some(Limit::new(
                    select.limit.as_ref().and_then(|l| l.limit),
                    offset_val as u64,
                ));
            }
            "order" => {
                let order_by = parse_order_value(arg_value)?;
                select.order_by = Some(order_by);
            }
            "groupBy" => {
                let group_by = parse_group_by_value(arg_value)?;
                select.group_by = Some(group_by);
            }
            "docIDs" | "docID" => {
                let doc_ids = parse_doc_ids_value(arg_value)?;
                select.doc_ids = Some(doc_ids);
            }
            "cid" => match arg_value {
                Value::String(s) => select.cid = Some(s.clone()),
                _ => return Err(QueryError::parse("cid argument must be a string")),
            },
            "showDeleted" => match arg_value {
                Value::Boolean(b) => select.show_deleted = *b,
                _ => return Err(QueryError::parse("showDeleted argument must be a boolean")),
            },
            _ => {
                return Err(QueryError::parse(format!(
                    "unknown argument '{}' on collection '{}'. Valid arguments are: filter, limit, offset, order, groupBy, docIDs, docID, cid, showDeleted",
                    arg_name, collection_name
                )));
            }
        }
    }

    // Parse selection set (child fields)
    let (fields, mapping) = parse_selection_set(&field.selection_set, &collection_name)?;
    select.fields = fields;
    select.document_mapping = mapping;

    Ok(select)
}

/// Parse a selection set into fields and document mapping.
fn parse_selection_set(
    selection_set: &SelectionSet<'_, String>,
    _collection_name: &str,
) -> Result<(Vec<Requestable>, DocumentMapping)> {
    let mut fields = Vec::new();
    let mut mapping = DocumentMapping::new();

    for selection in &selection_set.items {
        match selection {
            Selection::Field(field) => {
                let field_name = field.name.clone();
                let alias = field.alias.clone();

                // Check if this is an aggregate field (_count, _sum, _avg, _min, _max)
                if let Some(agg_type) = AggregateType::parse(&field_name) {
                    let mut aggregate = parse_aggregate_field(field, agg_type)?;

                    // Set alias if provided
                    if let Some(ref a) = alias {
                        aggregate = aggregate.with_alias(a.clone());
                    }

                    // Add to document mapping
                    let index = mapping.next_index();
                    mapping.add(index, agg_type.as_str());
                    mapping.add_render_key(index, aggregate.output_name());

                    fields.push(Requestable::Aggregate(aggregate));
                } else if !field.selection_set.items.is_empty() {
                    // This is a nested select (relation)
                    let nested = parse_field_to_select(field)?;

                    // Add nested select to document mapping
                    // Use field name for internal indexing, output_name (alias) for rendering
                    let index = mapping.next_index();
                    mapping.add(index, &field_name);
                    mapping.add_render_key(index, nested.field.output_name());

                    fields.push(Requestable::Select(Box::new(nested)));
                } else {
                    // Simple field
                    let select_field = if let Some(a) = alias {
                        SelectField::with_alias(&field_name, a)
                    } else {
                        SelectField::new(&field_name)
                    };

                    // Add to document mapping
                    let index = mapping.next_index();
                    mapping.add(index, &field_name);
                    mapping.add_render_key(index, select_field.output_name());

                    fields.push(Requestable::Field(select_field));
                }
            }
            Selection::FragmentSpread(_) => {
                return Err(QueryError::parse("fragment spreads not yet supported"))
            }
            Selection::InlineFragment(_) => {
                return Err(QueryError::parse("inline fragments not yet supported"))
            }
        }
    }

    Ok((fields, mapping))
}

/// Parse a filter argument value into a Filter.
fn parse_filter_value(value: &Value<'_, String>) -> Result<Filter> {
    match value {
        Value::Object(obj) => {
            let conditions = parse_filter_object(obj)?;
            Ok(Filter::from_conditions(conditions))
        }
        _ => Err(QueryError::parse("filter must be an object")),
    }
}

/// Parse a filter object into conditions map.
fn parse_filter_object(
    obj: &BTreeMap<String, Value<'_, String>>,
) -> Result<HashMap<String, JsonValue>> {
    let mut conditions = HashMap::new();

    for (key, val) in obj {
        let json_val = graphql_value_to_json(val)?;
        conditions.insert(key.clone(), json_val);
    }

    Ok(conditions)
}

/// Convert GraphQL Value to JSON Value.
fn graphql_value_to_json(value: &Value<'_, String>) -> Result<JsonValue> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Int(n) => n
            .as_i64()
            .map(|i| JsonValue::Number(i.into()))
            .ok_or_else(|| QueryError::parse("integer out of range")),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .ok_or_else(|| QueryError::parse("invalid float value")),
        Value::String(s) => Ok(JsonValue::String(s.clone())),
        Value::Boolean(b) => Ok(JsonValue::Bool(*b)),
        Value::Enum(e) => Ok(JsonValue::String(e.clone())),
        Value::List(items) => {
            let arr: Result<Vec<JsonValue>> = items.iter().map(graphql_value_to_json).collect();
            Ok(JsonValue::Array(arr?))
        }
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                map.insert(k.clone(), graphql_value_to_json(v)?);
            }
            Ok(JsonValue::Object(map))
        }
        Value::Variable(_) => Err(QueryError::parse("variables not yet supported")),
    }
}

/// Parse an integer value from GraphQL Value.
fn parse_int_value(value: &Value<'_, String>) -> Result<i64> {
    match value {
        Value::Int(n) => n
            .as_i64()
            .ok_or_else(|| QueryError::parse("integer out of range")),
        _ => Err(QueryError::parse("expected integer value")),
    }
}

/// Parse order argument into OrderBy.
fn parse_order_value(value: &Value<'_, String>) -> Result<OrderBy> {
    let mut order_by = OrderBy::new();

    match value {
        Value::Object(obj) => {
            for (field_name, direction_val) in obj {
                let direction = match direction_val {
                    Value::Enum(s) | Value::String(s) => {
                        OrderDirection::parse(s).ok_or_else(|| {
                            QueryError::parse(format!(
                                "invalid order direction '{}', expected ASC or DESC",
                                s
                            ))
                        })?
                    }
                    _ => return Err(QueryError::parse("order direction must be ASC or DESC")),
                };
                order_by = order_by.with_condition(OrderCondition::new(field_name, direction));
            }
        }
        _ => return Err(QueryError::parse("order must be an object")),
    }

    Ok(order_by)
}

/// Parse groupBy argument into GroupBy.
fn parse_group_by_value(value: &Value<'_, String>) -> Result<GroupBy> {
    match value {
        Value::List(items) => {
            let fields: Result<Vec<String>> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) | Value::Enum(s) => Ok(s.clone()),
                    _ => Err(QueryError::parse("groupBy items must be strings")),
                })
                .collect();
            Ok(GroupBy::new(fields?))
        }
        _ => Err(QueryError::parse("groupBy must be a list")),
    }
}

/// Parse an aggregate field into an Aggregate.
///
/// Handles aggregate functions like `_count`, `_sum(field: "age")`, etc.
fn parse_aggregate_field(field: &Field<'_, String>, agg_type: AggregateType) -> Result<Aggregate> {
    let mut target_field: Option<String> = None;

    // Parse arguments (e.g., `field: "age"` for _sum)
    for (arg_name, arg_value) in &field.arguments {
        match arg_name.as_str() {
            "field" => {
                target_field = Some(match arg_value {
                    Value::String(s) => s.clone(),
                    Value::Enum(s) => s.clone(),
                    _ => return Err(QueryError::parse("field argument must be a string")),
                });
            }
            _ => {
                return Err(QueryError::parse(format!(
                    "unknown argument '{}' on aggregate '{}'. Valid arguments are: field",
                    arg_name,
                    agg_type.as_str()
                )));
            }
        }
    }

    // Create the appropriate aggregate
    let aggregate = match agg_type {
        AggregateType::Count => {
            // _count can work without a field argument (counts all docs)
            if let Some(field_name) = target_field {
                Aggregate::count().with_target(AggregateTarget::with_field("", field_name))
            } else {
                Aggregate::count()
            }
        }
        AggregateType::Sum => {
            let field_name = target_field
                .ok_or_else(|| QueryError::parse("_sum requires a 'field' argument"))?;
            Aggregate::sum(AggregateTarget::with_field("", field_name))
        }
        AggregateType::Average => {
            let field_name = target_field
                .ok_or_else(|| QueryError::parse("_avg requires a 'field' argument"))?;
            Aggregate::avg(AggregateTarget::with_field("", field_name))
        }
        AggregateType::Min => {
            let field_name = target_field
                .ok_or_else(|| QueryError::parse("_min requires a 'field' argument"))?;
            Aggregate::min(AggregateTarget::with_field("", field_name))
        }
        AggregateType::Max => {
            let field_name = target_field
                .ok_or_else(|| QueryError::parse("_max requires a 'field' argument"))?;
            Aggregate::max(AggregateTarget::with_field("", field_name))
        }
    };

    Ok(aggregate)
}

/// Parse docIDs argument into vector of strings.
fn parse_doc_ids_value(value: &Value<'_, String>) -> Result<Vec<String>> {
    match value {
        Value::List(items) => {
            let ids: Result<Vec<String>> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(s.clone()),
                    _ => Err(QueryError::parse("docIDs items must be strings")),
                })
                .collect();
            ids
        }
        Value::String(s) => Ok(vec![s.clone()]),
        _ => Err(QueryError::parse("docIDs must be a string or list")),
    }
}

// =============================================================================
// Mutation Parsing
// =============================================================================

/// Parse a single GraphQL field into a Mutation operation.
///
/// Mutation field names follow the format: `operation_collection`
/// Examples: `create_Users`, `update_Posts`, `delete_Comments`
fn parse_field_to_mutation(field: &Field<'_, String>) -> Result<Mutation> {
    let field_name = &field.name;

    // Parse mutation name to get operation type and collection
    let (mutation_type, collection_name) =
        parse_mutation_name(field_name).map_err(QueryError::parse)?;

    // Create base mutation
    let mut mutation = match mutation_type {
        MutationType::Create => Mutation::create(&collection_name),
        MutationType::Update => Mutation::update(&collection_name),
        MutationType::Delete => Mutation::delete(&collection_name),
        MutationType::Upsert => Mutation::upsert(&collection_name),
    };

    // Parse arguments based on mutation type
    for (arg_name, arg_value) in &field.arguments {
        match (mutation_type, arg_name.as_str()) {
            // CREATE: input is array of documents
            (MutationType::Create, "input") => {
                let input = parse_create_input(arg_value)?;
                mutation.create_input = input;
            }

            // UPDATE/UPSERT: input is patch object
            (MutationType::Update | MutationType::Upsert, "input") => {
                let input = parse_update_input(arg_value)?;
                mutation.update_input = input;
            }

            // UPDATE/DELETE/UPSERT: docIDs to target
            (
                MutationType::Update | MutationType::Delete | MutationType::Upsert,
                "docIDs" | "_docIDs",
            ) => {
                let doc_ids = parse_doc_ids_value(arg_value)?;
                mutation.doc_ids = Some(doc_ids);
            }

            // UPDATE/DELETE/UPSERT: filter to find documents
            (MutationType::Update | MutationType::Delete | MutationType::Upsert, "filter") => {
                let filter = parse_filter_value(arg_value)?;
                mutation.filter = Some(filter);
            }

            // Unknown argument
            _ => {
                return Err(QueryError::parse(format!(
                    "Unknown argument '{}' for {} mutation on '{}'",
                    arg_name,
                    mutation_type.as_prefix(),
                    collection_name
                )));
            }
        }
    }

    // Validate mutation has required arguments
    match mutation_type {
        MutationType::Create => {
            if mutation.create_input.is_empty() {
                return Err(QueryError::parse(format!(
                    "create_{} mutation requires 'input' argument with array of documents",
                    collection_name
                )));
            }
        }
        MutationType::Update => {
            if mutation.update_input.is_empty() {
                return Err(QueryError::parse(format!(
                    "update_{} mutation requires 'input' argument with fields to update",
                    collection_name
                )));
            }
            if mutation.doc_ids.is_none() && mutation.filter.is_none() {
                return Err(QueryError::parse(format!(
                    "update_{} mutation requires either 'docIDs' or 'filter' argument",
                    collection_name
                )));
            }
        }
        MutationType::Delete => {
            if mutation.doc_ids.is_none() && mutation.filter.is_none() {
                return Err(QueryError::parse(format!(
                    "delete_{} mutation requires either 'docIDs' or 'filter' argument",
                    collection_name
                )));
            }
        }
        MutationType::Upsert => {
            if mutation.update_input.is_empty() {
                return Err(QueryError::parse(format!(
                    "upsert_{} mutation requires 'input' argument with fields to set",
                    collection_name
                )));
            }
            // Note: docIDs/filter are optional for upsert - if not provided, creates a new document
        }
    }

    // Parse selection set (fields to return after mutation)
    let (fields, mapping) = parse_selection_set(&field.selection_set, &collection_name)?;
    mutation.fields = fields;
    mutation.document_mapping = mapping;

    Ok(mutation)
}

/// Parse CREATE mutation input (array of documents).
fn parse_create_input(value: &Value<'_, String>) -> Result<Vec<HashMap<String, JsonValue>>> {
    match value {
        Value::List(items) => {
            let mut docs = Vec::new();
            for item in items {
                match item {
                    Value::Object(obj) => {
                        let doc = parse_document_input(obj)?;
                        docs.push(doc);
                    }
                    _ => return Err(QueryError::parse("CREATE input items must be objects")),
                }
            }
            Ok(docs)
        }
        Value::Object(obj) => {
            // Single document (wrap in array)
            let doc = parse_document_input(obj)?;
            Ok(vec![doc])
        }
        _ => Err(QueryError::parse(
            "CREATE input must be an array of objects or a single object",
        )),
    }
}

/// Parse UPDATE mutation input (patch object).
fn parse_update_input(value: &Value<'_, String>) -> Result<HashMap<String, JsonValue>> {
    match value {
        Value::Object(obj) => parse_document_input(obj),
        _ => Err(QueryError::parse("UPDATE input must be an object")),
    }
}

/// Parse a document input object into field-value map.
fn parse_document_input(
    obj: &BTreeMap<String, Value<'_, String>>,
) -> Result<HashMap<String, JsonValue>> {
    let mut fields = HashMap::new();
    for (key, value) in obj {
        let json_value = graphql_value_to_json(value)?;
        fields.insert(key.clone(), json_value);
    }
    Ok(fields)
}

#[cfg(test)]
mod mutation_tests {
    use super::*;

    #[test]
    fn test_parse_create_mutation() {
        let query = r#"
            mutation {
                create_Users(input: [{name: "Alice", age: 30}]) {
                    _docID
                    name
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        assert_eq!(mutations.len(), 1);

        let m = &mutations[0];
        assert_eq!(m.mutation_type, MutationType::Create);
        assert_eq!(m.collection_name, "Users");
        assert_eq!(m.create_input.len(), 1);
        assert_eq!(
            m.create_input[0].get("name"),
            Some(&JsonValue::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_parse_create_multiple_documents() {
        let query = r#"
            mutation {
                create_Users(input: [
                    {name: "Alice", age: 30},
                    {name: "Bob", age: 25}
                ]) {
                    _docID
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        assert_eq!(mutations[0].create_input.len(), 2);
    }

    #[test]
    fn test_parse_update_mutation() {
        let query = r#"
            mutation {
                update_Users(docIDs: ["bae-123"], input: {email: "new@example.com"}) {
                    _docID
                    email
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        assert_eq!(mutations.len(), 1);

        let m = &mutations[0];
        assert_eq!(m.mutation_type, MutationType::Update);
        assert_eq!(m.collection_name, "Users");
        assert_eq!(m.doc_ids, Some(vec!["bae-123".to_string()]));
        assert_eq!(
            m.update_input.get("email"),
            Some(&JsonValue::String("new@example.com".to_string()))
        );
    }

    #[test]
    fn test_parse_update_with_filter() {
        let query = r#"
            mutation {
                update_Users(filter: {name: {_eq: "Alice"}}, input: {active: false}) {
                    _docID
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        let m = &mutations[0];
        assert!(m.filter.is_some());
        assert!(m.doc_ids.is_none());
    }

    #[test]
    fn test_parse_delete_mutation() {
        let query = r#"
            mutation {
                delete_Users(docIDs: ["bae-123", "bae-456"]) {
                    _docID
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        assert_eq!(mutations.len(), 1);

        let m = &mutations[0];
        assert_eq!(m.mutation_type, MutationType::Delete);
        assert_eq!(m.collection_name, "Users");
        assert_eq!(
            m.doc_ids,
            Some(vec!["bae-123".to_string(), "bae-456".to_string()])
        );
    }

    #[test]
    fn test_parse_delete_with_filter() {
        let query = r#"
            mutation {
                delete_Users(filter: {active: {_eq: false}}) {
                    _docID
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        let m = &mutations[0];
        assert!(m.filter.is_some());
    }

    #[test]
    fn test_parse_multiple_mutations() {
        let query = r#"
            mutation {
                create_Users(input: [{name: "Alice"}]) {
                    _docID
                }
                delete_Posts(docIDs: ["bae-999"]) {
                    _docID
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        assert_eq!(mutations.len(), 2);
        assert_eq!(mutations[0].mutation_type, MutationType::Create);
        assert_eq!(mutations[1].mutation_type, MutationType::Delete);
    }

    #[test]
    fn test_create_missing_input_error() {
        let query = r#"
            mutation {
                create_Users {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires 'input'"));
    }

    #[test]
    fn test_update_missing_target_error() {
        let query = r#"
            mutation {
                update_Users(input: {name: "Bob"}) {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires either 'docIDs' or 'filter'"));
    }

    #[test]
    fn test_delete_missing_target_error() {
        let query = r#"
            mutation {
                delete_Users {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_mutation_name_error() {
        let query = r#"
            mutation {
                Users(input: [{name: "Alice"}]) {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid mutation name"));
    }

    #[test]
    fn test_query_still_works() {
        let query = r#"
            {
                Users {
                    _docID
                    name
                }
            }
        "#;

        let selects = parse_query(query).unwrap();
        assert_eq!(selects.len(), 1);
        assert_eq!(selects[0].collection_name, "Users");
    }

    #[test]
    fn test_cannot_mix_query_and_mutation() {
        // Note: GraphQL parser won't actually allow this syntax,
        // but we handle it anyway
        let query = r#"
            mutation {
                create_Users(input: [{name: "Alice"}]) { _docID }
            }
        "#;

        // This should work as pure mutation
        let result = parse_mutations(query);
        assert!(result.is_ok());

        // parse_query should fail on mutation
        let result = parse_query(query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_upsert_mutation_with_doc_ids() {
        let query = r#"
            mutation {
                upsert_Users(docIDs: ["bae-123"], input: {name: "Alice", age: 30}) {
                    _docID
                    name
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        assert_eq!(mutations.len(), 1);

        let m = &mutations[0];
        assert_eq!(m.mutation_type, MutationType::Upsert);
        assert_eq!(m.collection_name, "Users");
        assert_eq!(m.doc_ids, Some(vec!["bae-123".to_string()]));
        assert_eq!(
            m.update_input.get("name"),
            Some(&JsonValue::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_parse_upsert_mutation_with_filter() {
        let query = r#"
            mutation {
                upsert_Users(filter: {name: {_eq: "Alice"}}, input: {age: 31}) {
                    _docID
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        let m = &mutations[0];
        assert_eq!(m.mutation_type, MutationType::Upsert);
        assert!(m.filter.is_some());
        assert!(m.doc_ids.is_none());
    }

    #[test]
    fn test_parse_upsert_mutation_create_new() {
        // Upsert without docIDs/filter creates a new document
        let query = r#"
            mutation {
                upsert_Users(input: {name: "NewUser", email: "new@example.com"}) {
                    _docID
                    name
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        let m = &mutations[0];
        assert_eq!(m.mutation_type, MutationType::Upsert);
        assert!(m.doc_ids.is_none());
        assert!(m.filter.is_none());
        assert_eq!(
            m.update_input.get("name"),
            Some(&JsonValue::String("NewUser".to_string()))
        );
    }

    #[test]
    fn test_upsert_missing_input_error() {
        let query = r#"
            mutation {
                upsert_Users(docIDs: ["bae-123"]) {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("requires 'input'"));
    }
}
