//! GraphQL query parser
//!
//! Parses GraphQL query strings into Select and Mutation operations for execution.

use graphql_parser::query::{
    Definition, Directive, Document, Field, FragmentDefinition, OperationDefinition, Selection,
    SelectionSet, Value, VariableDefinition,
};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{
    parse_mutation_name, Aggregate, AggregateTarget, AggregateType, Field as SelectField, Filter,
    GroupBy, Limit, Mutation, MutationType, OrderBy, OrderCondition, OrderDirection, Requestable,
    Select,
};

/// Type of explain output requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExplainType {
    /// Simple explanation showing query plan structure without execution.
    #[default]
    Simple,
    /// Execute the query and return both the plan structure and execution metrics.
    Execute,
    /// Debug mode showing all plan nodes including internal ones.
    Debug,
}

impl ExplainType {
    /// Parse explain type from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "simple" => Some(Self::Simple),
            "execute" => Some(Self::Execute),
            "debug" => Some(Self::Debug),
            _ => None,
        }
    }
}

/// Result of parsing a GraphQL request.
#[derive(Debug)]
pub enum ParsedOperation {
    /// Query operations (SELECT)
    Query {
        selects: Vec<Select>,
        /// Whether @explain directive was used and which type
        explain: Option<ExplainType>,
    },
    /// Mutation operations (CREATE, UPDATE, DELETE)
    Mutation(Vec<Mutation>),
    /// Subscription operations (single root field only per GraphQL spec)
    Subscription {
        /// The single select for the subscription.
        select: Select,
    },
}

/// Check if a directive list contains @explain and parse its type.
/// Returns Some(ExplainType) if @explain is present, None otherwise.
fn parse_explain_directive(directives: &[Directive<'_, String>]) -> Option<ExplainType> {
    for directive in directives {
        if directive.name == "explain" {
            // Check for type argument: @explain(type: simple|execute|debug)
            for (name, value) in &directive.arguments {
                if name == "type" {
                    if let Value::Enum(type_str) = value {
                        if let Some(explain_type) = ExplainType::from_str(type_str) {
                            return Some(explain_type);
                        }
                    } else if let Value::String(type_str) = value {
                        if let Some(explain_type) = ExplainType::from_str(type_str) {
                            return Some(explain_type);
                        }
                    }
                }
            }
            // No type argument or unknown type - default to Simple
            return Some(ExplainType::Simple);
        }
    }
    None
}

/// Type alias for fragment definitions map
type FragmentMap<'a> = HashMap<String, &'a FragmentDefinition<'a, String>>;

/// Parse a selection into Select operations, handling fragments.
fn parse_selection_to_selects<'a>(
    selection: &'a Selection<'a, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    fragments: &FragmentMap<'a>,
    selects: &mut Vec<Select>,
    visiting: &mut HashSet<String>,
) -> Result<()> {
    match selection {
        Selection::Field(field) => {
            let select = parse_field_to_select(field, variables, fragments, visiting)?;
            selects.push(select);
        }
        Selection::FragmentSpread(spread) => {
            // Check for circular fragment reference
            if visiting.contains(&spread.fragment_name) {
                return Err(QueryError::parse(format!(
                    "circular fragment reference detected: '{}'",
                    spread.fragment_name
                )));
            }

            // Look up the fragment by name
            let frag = fragments.get(&spread.fragment_name).ok_or_else(|| {
                QueryError::parse(format!("undefined fragment '{}'", spread.fragment_name))
            })?;

            // Mark this fragment as being visited
            visiting.insert(spread.fragment_name.clone());

            // Process each selection in the fragment's selection set
            for frag_selection in &frag.selection_set.items {
                parse_selection_to_selects(
                    frag_selection,
                    variables,
                    fragments,
                    selects,
                    visiting,
                )?;
            }

            // Unmark after processing
            visiting.remove(&spread.fragment_name);
        }
        Selection::InlineFragment(inline) => {
            // Inline fragments: ... on Type { fields }
            // For now, we ignore the type condition and just expand the fields
            // (DefraDB doesn't have interface/union types yet)
            for inline_selection in &inline.selection_set.items {
                parse_selection_to_selects(
                    inline_selection,
                    variables,
                    fragments,
                    selects,
                    visiting,
                )?;
            }
        }
    }
    Ok(())
}

/// Parse a GraphQL query string into Select operations.
///
/// Returns a vector of Select operations, one for each top-level field in the query.
/// For mutations, use `parse_request` instead.
pub fn parse_query(query: &str) -> Result<Vec<Select>> {
    match parse_request(query)? {
        ParsedOperation::Query { selects, .. } => Ok(selects),
        ParsedOperation::Mutation(_) => Err(QueryError::parse(
            "Expected query but got mutation. Use parse_request() for mutations.",
        )),
        ParsedOperation::Subscription { .. } => Err(QueryError::parse(
            "Expected query but got subscription. Use parse_request() for subscriptions.",
        )),
    }
}

/// Parse a GraphQL mutation string into Mutation operations.
///
/// Returns a vector of Mutation operations, one for each top-level field in the mutation.
pub fn parse_mutations(query: &str) -> Result<Vec<Mutation>> {
    match parse_request(query)? {
        ParsedOperation::Mutation(mutations) => Ok(mutations),
        ParsedOperation::Query { .. } => Err(QueryError::parse("Expected mutation but got query")),
        ParsedOperation::Subscription { .. } => {
            Err(QueryError::parse("Expected mutation but got subscription"))
        }
    }
}

/// Parse a GraphQL request (query or mutation) into operations.
///
/// This is the main entry point for parsing GraphQL requests.
/// For queries with variables, use `parse_request_with_variables` instead.
pub fn parse_request(query: &str) -> Result<ParsedOperation> {
    parse_request_with_variables(query, None)
}

/// Parse a GraphQL request with variable substitution.
///
/// Variables in the query (e.g., `$userId`) will be substituted with values
/// from the provided variables map during parsing.
///
/// # Example
/// ```ignore
/// let variables = HashMap::from([
///     ("userId".to_string(), json!("bae-123")),
/// ]);
/// let result = parse_request_with_variables(
///     "query($userId: ID!) { User(docID: $userId) { name } }",
///     Some(&variables)
/// )?;
/// ```
pub fn parse_request_with_variables(
    query: &str,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<ParsedOperation> {
    let doc: Document<'_, String> =
        graphql_parser::parse_query(query).map_err(|e| QueryError::parse(e.to_string()))?;

    // First pass: collect all fragment definitions
    let mut fragments: HashMap<String, &FragmentDefinition<'_, String>> = HashMap::new();
    for def in &doc.definitions {
        if let Definition::Fragment(frag) = def {
            fragments.insert(frag.name.clone(), frag);
        }
    }

    let mut selects = Vec::new();
    let mut mutations = Vec::new();
    let mut subscription_selects = Vec::new();
    let mut has_query = false;
    let mut has_mutation = false;
    let mut has_subscription = false;
    let mut explain: Option<ExplainType> = None;

    // Second pass: parse operations with fragments available
    for def in &doc.definitions {
        match def {
            Definition::Operation(op) => {
                match op {
                    OperationDefinition::Query(q) => {
                        has_query = true;
                        // Check for @explain directive and parse type
                        if let Some(explain_type) = parse_explain_directive(&q.directives) {
                            explain = Some(explain_type);
                        }

                        // Extract default values from variable definitions and merge with provided variables
                        let defaults = extract_variable_defaults(&q.variable_definitions)?;
                        let effective_variables = merge_variables(variables, &defaults);
                        // If variables was provided (even empty) or we have defaults, use the merged map
                        // Otherwise preserve None to get appropriate "no variables provided" error
                        let effective_vars_ref = if variables.is_some() || !defaults.is_empty() {
                            Some(&effective_variables)
                        } else {
                            None
                        };

                        let mut visiting = HashSet::new();
                        for selection in &q.selection_set.items {
                            parse_selection_to_selects(
                                selection,
                                effective_vars_ref,
                                &fragments,
                                &mut selects,
                                &mut visiting,
                            )?;
                        }
                    }
                    OperationDefinition::SelectionSet(ss) => {
                        // Bare selection set is treated as query
                        has_query = true;
                        let mut visiting = HashSet::new();
                        for selection in &ss.items {
                            parse_selection_to_selects(
                                selection,
                                variables,
                                &fragments,
                                &mut selects,
                                &mut visiting,
                            )?;
                        }
                    }
                    OperationDefinition::Mutation(m) => {
                        has_mutation = true;

                        // Extract default values from variable definitions and merge with provided variables
                        let defaults = extract_variable_defaults(&m.variable_definitions)?;
                        let effective_variables = merge_variables(variables, &defaults);
                        // If variables was provided (even empty) or we have defaults, use the merged map
                        // Otherwise preserve None to get appropriate "no variables provided" error
                        let effective_vars_ref = if variables.is_some() || !defaults.is_empty() {
                            Some(&effective_variables)
                        } else {
                            None
                        };

                        for selection in &m.selection_set.items {
                            if let Selection::Field(field) = selection {
                                let mutation = parse_field_to_mutation(field, effective_vars_ref)?;
                                mutations.push(mutation);
                            }
                        }
                    }
                    OperationDefinition::Subscription(s) => {
                        has_subscription = true;

                        // Extract default values from variable definitions and merge with provided variables
                        let defaults = extract_variable_defaults(&s.variable_definitions)?;
                        let effective_variables = merge_variables(variables, &defaults);
                        let effective_vars_ref = if variables.is_some() || !defaults.is_empty() {
                            Some(&effective_variables)
                        } else {
                            None
                        };

                        // Parse selections (same as Query)
                        let mut visiting = HashSet::new();
                        for selection in &s.selection_set.items {
                            parse_selection_to_selects(
                                selection,
                                effective_vars_ref,
                                &fragments,
                                &mut subscription_selects,
                                &mut visiting,
                            )?;
                        }

                        // Validate single root field (GraphQL spec requirement)
                        if subscription_selects.len() != 1 {
                            return Err(QueryError::parse(
                                "subscription must have exactly one root field",
                            ));
                        }
                    }
                };
            }
            Definition::Fragment(_) => {
                // Already processed in first pass
            }
        }
    }

    // Cannot mix operation types
    let op_count = [has_query, has_mutation, has_subscription]
        .iter()
        .filter(|&&x| x)
        .count();
    if op_count > 1 {
        return Err(QueryError::parse(
            "Cannot mix queries, mutations, and subscriptions in same request",
        ));
    }

    if has_subscription {
        // subscription_selects is guaranteed to have exactly one element due to earlier validation
        Ok(ParsedOperation::Subscription {
            select: subscription_selects.into_iter().next().unwrap(),
        })
    } else if has_mutation {
        Ok(ParsedOperation::Mutation(mutations))
    } else {
        Ok(ParsedOperation::Query { selects, explain })
    }
}

/// Parse a single GraphQL field into a Select operation.
fn parse_field_to_select(
    field: &Field<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    fragments: &FragmentMap<'_>,
    visiting: &mut HashSet<String>,
) -> Result<Select> {
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
                let filter = parse_filter_value(arg_value, variables)?;
                select.filter = Some(filter);
            }
            "limit" => {
                let limit_val = parse_int_value(arg_value, variables)?;
                if limit_val < 0 {
                    return Err(QueryError::parse("limit must be non-negative"));
                }
                select.limit = Some(Limit::new(
                    Some(limit_val as u64),
                    select.limit.as_ref().map(|l| l.offset).unwrap_or(0),
                ));
            }
            "offset" => {
                let offset_val = parse_int_value(arg_value, variables)?;
                if offset_val < 0 {
                    return Err(QueryError::parse("offset must be non-negative"));
                }
                select.limit = Some(Limit::new(
                    select.limit.as_ref().and_then(|l| l.limit),
                    offset_val as u64,
                ));
            }
            "order" => {
                let order_by = parse_order_value(arg_value, variables)?;
                select.order_by = Some(order_by);
            }
            "groupBy" => {
                let group_by = parse_group_by_value(arg_value, variables)?;
                select.group_by = Some(group_by);
            }
            "docIDs" | "docID" => {
                let doc_ids = parse_doc_ids_value(arg_value, variables)?;
                select.doc_ids = Some(doc_ids);
            }
            "cid" => {
                let cid_val = resolve_string_value(arg_value, variables, "cid")?;
                select.cid = Some(cid_val);
            }
            "showDeleted" => {
                let show_deleted = resolve_bool_value(arg_value, variables, "showDeleted")?;
                select.show_deleted = show_deleted;
            }
            _ => {
                return Err(QueryError::parse(format!(
                    "unknown argument '{}' on collection '{}'. Valid arguments are: filter, limit, offset, order, groupBy, docIDs, docID, cid, showDeleted",
                    arg_name, collection_name
                )));
            }
        }
    }

    // Parse selection set (child fields)
    let (fields, mapping) = parse_selection_set(
        &field.selection_set,
        &collection_name,
        variables,
        fragments,
        visiting,
    )?;
    select.fields = fields;
    select.document_mapping = mapping;

    Ok(select)
}

/// Parse a selection set into fields and document mapping.
fn parse_selection_set(
    selection_set: &SelectionSet<'_, String>,
    _collection_name: &str,
    variables: Option<&HashMap<String, JsonValue>>,
    fragments: &FragmentMap<'_>,
    visiting: &mut HashSet<String>,
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
                    let mut aggregate = parse_aggregate_field(field, agg_type, variables)?;

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
                    let nested = parse_field_to_select(field, variables, fragments, visiting)?;

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
            Selection::FragmentSpread(spread) => {
                // Check for circular fragment reference
                if visiting.contains(&spread.fragment_name) {
                    return Err(QueryError::parse(format!(
                        "circular fragment reference detected: '{}'",
                        spread.fragment_name
                    )));
                }

                // Look up the fragment by name
                let frag = fragments.get(&spread.fragment_name).ok_or_else(|| {
                    QueryError::parse(format!("undefined fragment '{}'", spread.fragment_name))
                })?;

                // Mark this fragment as being visited
                visiting.insert(spread.fragment_name.clone());

                // Recursively parse the fragment's selection set
                let (frag_fields, _frag_mapping) = parse_selection_set(
                    &frag.selection_set,
                    _collection_name,
                    variables,
                    fragments,
                    visiting,
                )?;

                // Unmark after processing
                visiting.remove(&spread.fragment_name);

                // Merge fragment fields and mapping into our current sets
                for frag_field in frag_fields {
                    // Update mapping indices for the merged fields
                    let index = mapping.next_index();
                    match &frag_field {
                        Requestable::Field(f) => {
                            mapping.add(index, &f.name);
                            mapping.add_render_key(index, f.output_name());
                        }
                        Requestable::Aggregate(a) => {
                            mapping.add(index, a.aggregate_type.as_str());
                            mapping.add_render_key(index, a.output_name());
                        }
                        Requestable::Select(s) => {
                            mapping.add(index, &s.field.name);
                            mapping.add_render_key(index, s.field.output_name());
                        }
                    }
                    fields.push(frag_field);
                }
            }
            Selection::InlineFragment(inline) => {
                // Inline fragments: ... on Type { fields }
                // For now, we ignore the type condition and just expand the fields
                // (DefraDB doesn't have interface/union types yet)
                let (inline_fields, _inline_mapping) = parse_selection_set(
                    &inline.selection_set,
                    _collection_name,
                    variables,
                    fragments,
                    visiting,
                )?;

                // Merge inline fragment fields into our current sets
                for inline_field in inline_fields {
                    let index = mapping.next_index();
                    match &inline_field {
                        Requestable::Field(f) => {
                            mapping.add(index, &f.name);
                            mapping.add_render_key(index, f.output_name());
                        }
                        Requestable::Aggregate(a) => {
                            mapping.add(index, a.aggregate_type.as_str());
                            mapping.add_render_key(index, a.output_name());
                        }
                        Requestable::Select(s) => {
                            mapping.add(index, &s.field.name);
                            mapping.add_render_key(index, s.field.output_name());
                        }
                    }
                    fields.push(inline_field);
                }
            }
        }
    }

    Ok((fields, mapping))
}

/// Parse a filter argument value into a Filter.
fn parse_filter_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Filter> {
    match value {
        Value::Object(obj) => {
            let conditions = parse_filter_object(obj, variables)?;
            Ok(Filter::from_conditions(conditions))
        }
        _ => Err(QueryError::parse("filter must be an object")),
    }
}

/// Parse a filter object into conditions map.
fn parse_filter_object(
    obj: &BTreeMap<String, Value<'_, String>>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<HashMap<String, JsonValue>> {
    let mut conditions = HashMap::new();

    for (key, val) in obj {
        let json_val = graphql_value_to_json(val, variables)?;
        conditions.insert(key.clone(), json_val);
    }

    Ok(conditions)
}

/// Merge provided variables with default values.
///
/// Provided variables take precedence over defaults.
fn merge_variables(
    provided: Option<&HashMap<String, JsonValue>>,
    defaults: &HashMap<String, JsonValue>,
) -> HashMap<String, JsonValue> {
    let mut merged = defaults.clone();
    if let Some(vars) = provided {
        for (k, v) in vars {
            merged.insert(k.clone(), v.clone());
        }
    }
    merged
}

/// Extract default values from variable definitions.
///
/// Returns a HashMap of variable name -> default value for all variables
/// that have a default value defined.
fn extract_variable_defaults(
    var_defs: &[VariableDefinition<'_, String>],
) -> Result<HashMap<String, JsonValue>> {
    let mut defaults = HashMap::new();
    for var_def in var_defs {
        if let Some(default_value) = &var_def.default_value {
            // Convert the default value without variable resolution (defaults can't reference other variables)
            let json_val = graphql_value_to_json_no_vars(default_value)?;
            defaults.insert(var_def.name.clone(), json_val);
        }
    }
    Ok(defaults)
}

/// Convert GraphQL Value to JSON Value without variable resolution.
///
/// Used for converting default values where variable references are not allowed.
fn graphql_value_to_json_no_vars(value: &Value<'_, String>) -> Result<JsonValue> {
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
            let arr: Result<Vec<JsonValue>> =
                items.iter().map(graphql_value_to_json_no_vars).collect();
            Ok(JsonValue::Array(arr?))
        }
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                map.insert(k.clone(), graphql_value_to_json_no_vars(v)?);
            }
            Ok(JsonValue::Object(map))
        }
        Value::Variable(name) => Err(QueryError::parse(format!(
            "variable '{}' cannot be used in default value",
            name
        ))),
    }
}

/// Convert GraphQL Value to JSON Value, resolving variables if present.
fn graphql_value_to_json(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<JsonValue> {
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
            let arr: Result<Vec<JsonValue>> = items
                .iter()
                .map(|v| graphql_value_to_json(v, variables))
                .collect();
            Ok(JsonValue::Array(arr?))
        }
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                map.insert(k.clone(), graphql_value_to_json(v, variables)?);
            }
            Ok(JsonValue::Object(map))
        }
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            vars.get(name).cloned().ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })
        }
    }
}

/// Parse an integer value from GraphQL Value, resolving variables if present.
fn parse_int_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<i64> {
    match value {
        Value::Int(n) => n
            .as_i64()
            .ok_or_else(|| QueryError::parse("integer out of range")),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            json_val.as_i64().ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" must be of type Int", name))
            })
        }
        _ => Err(QueryError::parse("expected integer value")),
    }
}

/// Parse order argument into OrderBy.
fn parse_order_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<OrderBy> {
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
                    Value::Variable(name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!(
                                "variable '{}' used but no variables provided",
                                name
                            ))
                        })?;
                        let json_val = vars.get(name).ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided", name))
                        })?;
                        let s = json_val.as_str().ok_or_else(|| {
                            QueryError::parse(format!(
                                "Variable \"${}\" must be of type Ordering (ASC or DESC)",
                                name
                            ))
                        })?;
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
fn parse_group_by_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<GroupBy> {
    match value {
        Value::List(items) => {
            let fields: Result<Vec<String>> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) | Value::Enum(s) => Ok(s.clone()),
                    Value::Variable(name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!(
                                "variable '{}' used but no variables provided",
                                name
                            ))
                        })?;
                        let json_val = vars.get(name).ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided", name))
                        })?;
                        json_val.as_str().map(|s| s.to_string()).ok_or_else(|| {
                            QueryError::parse(format!(
                                "Variable \"${}\" must be of type String",
                                name
                            ))
                        })
                    }
                    _ => Err(QueryError::parse("groupBy items must be strings")),
                })
                .collect();
            Ok(GroupBy::new(fields?))
        }
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            let arr = json_val.as_array().ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" must be of type [String]", name))
            })?;
            let fields: Result<Vec<String>> = arr
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| QueryError::parse("groupBy items must be strings"))
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
fn parse_aggregate_field(
    field: &Field<'_, String>,
    agg_type: AggregateType,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Aggregate> {
    let mut target_field: Option<String> = None;

    // Parse arguments (e.g., `field: "age"` for _sum)
    for (arg_name, arg_value) in &field.arguments {
        match arg_name.as_str() {
            "field" => {
                target_field = Some(match arg_value {
                    Value::String(s) => s.clone(),
                    Value::Enum(s) => s.clone(),
                    Value::Variable(name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!(
                                "variable '{}' used but no variables provided",
                                name
                            ))
                        })?;
                        let json_val = vars.get(name).ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided", name))
                        })?;
                        json_val
                            .as_str()
                            .ok_or_else(|| {
                                QueryError::parse(format!(
                                    "Variable \"${}\" must be of type String",
                                    name
                                ))
                            })?
                            .to_string()
                    }
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
fn parse_doc_ids_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Vec<String>> {
    match value {
        Value::List(items) => {
            let ids: Result<Vec<String>> = items
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(s.clone()),
                    Value::Variable(name) => {
                        let vars = variables.ok_or_else(|| {
                            QueryError::parse(format!(
                                "variable '{}' used but no variables provided",
                                name
                            ))
                        })?;
                        let json_val = vars.get(name).ok_or_else(|| {
                            QueryError::parse(format!("Variable \"${}\" was not provided", name))
                        })?;
                        json_val.as_str().map(|s| s.to_string()).ok_or_else(|| {
                            QueryError::parse(format!(
                                "Variable \"${}\" must be of type String",
                                name
                            ))
                        })
                    }
                    _ => Err(QueryError::parse("docIDs items must be strings")),
                })
                .collect();
            ids
        }
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            // Variable can be a string (single ID) or array of strings
            if let Some(s) = json_val.as_str() {
                Ok(vec![s.to_string()])
            } else if let Some(arr) = json_val.as_array() {
                arr.iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| QueryError::parse("docIDs items must be strings"))
                    })
                    .collect()
            } else {
                Err(QueryError::parse(format!(
                    "Variable \"${}\" must be of type String or [String]",
                    name
                )))
            }
        }
        _ => Err(QueryError::parse("docIDs must be a string or list")),
    }
}

/// Resolve a string value, handling variables.
fn resolve_string_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    arg_name: &str,
) -> Result<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            json_val.as_str().map(|s| s.to_string()).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" must be of type String", name))
            })
        }
        _ => Err(QueryError::parse(format!(
            "{} argument must be a string",
            arg_name
        ))),
    }
}

/// Resolve a boolean value, handling variables.
fn resolve_bool_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
    arg_name: &str,
) -> Result<bool> {
    match value {
        Value::Boolean(b) => Ok(*b),
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            json_val.as_bool().ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" must be of type Boolean", name))
            })
        }
        _ => Err(QueryError::parse(format!(
            "{} argument must be a boolean",
            arg_name
        ))),
    }
}

// =============================================================================
// Mutation Parsing
// =============================================================================

/// Parse a single GraphQL field into a Mutation operation.
///
/// Mutation field names follow the format: `operation_collection`
/// Examples: `create_Users`, `update_Posts`, `delete_Comments`
fn parse_field_to_mutation(
    field: &Field<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Mutation> {
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
                let input = parse_create_input(arg_value, variables)?;
                mutation.create_input = input;
            }

            // UPDATE/UPSERT: input is patch object
            (MutationType::Update | MutationType::Upsert, "input") => {
                let input = parse_update_input(arg_value, variables)?;
                mutation.update_input = input;
            }

            // UPDATE/DELETE/UPSERT: docID or docIDs to target (Go uses singular docID)
            (
                MutationType::Update | MutationType::Delete | MutationType::Upsert,
                "docID" | "docIDs" | "_docIDs",
            ) => {
                let doc_ids = parse_doc_ids_value(arg_value, variables)?;
                mutation.doc_ids = Some(doc_ids);
            }

            // UPDATE/DELETE/UPSERT: filter to find documents
            (MutationType::Update | MutationType::Delete | MutationType::Upsert, "filter") => {
                let filter = parse_filter_value(arg_value, variables)?;
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
    // For mutations, we don't support fragments in return fields
    // For mutations, we don't support fragments in return fields
    let empty_fragments: FragmentMap<'_> = HashMap::new();
    let mut empty_visiting = HashSet::new();
    let (fields, mapping) = parse_selection_set(
        &field.selection_set,
        &collection_name,
        variables,
        &empty_fragments,
        &mut empty_visiting,
    )?;
    mutation.fields = fields;
    mutation.document_mapping = mapping;

    Ok(mutation)
}

/// Parse CREATE mutation input (array of documents).
fn parse_create_input(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Vec<HashMap<String, JsonValue>>> {
    match value {
        Value::List(items) => {
            let mut docs = Vec::new();
            for item in items {
                match item {
                    Value::Object(obj) => {
                        let doc = parse_document_input(obj, variables)?;
                        docs.push(doc);
                    }
                    _ => return Err(QueryError::parse("CREATE input items must be objects")),
                }
            }
            Ok(docs)
        }
        Value::Object(obj) => {
            // Single document (wrap in array)
            let doc = parse_document_input(obj, variables)?;
            Ok(vec![doc])
        }
        _ => Err(QueryError::parse(
            "CREATE input must be an array of objects or a single object",
        )),
    }
}

/// Parse UPDATE mutation input (patch object).
fn parse_update_input(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<HashMap<String, JsonValue>> {
    match value {
        Value::Object(obj) => parse_document_input(obj, variables),
        _ => Err(QueryError::parse("UPDATE input must be an object")),
    }
}

/// Parse a document input object into field-value map.
fn parse_document_input(
    obj: &BTreeMap<String, Value<'_, String>>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<HashMap<String, JsonValue>> {
    let mut fields = HashMap::new();
    for (key, value) in obj {
        let json_value = graphql_value_to_json(value, variables)?;
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

#[cfg(test)]
mod variable_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_variable_in_filter() {
        let query = r#"
            query($name: String!) {
                Users(filter: {name: {_eq: $name}}) {
                    _docID
                    name
                }
            }
        "#;

        let variables = HashMap::from([("name".to_string(), json!("Alice"))]);

        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                assert_eq!(selects.len(), 1);
                let filter = selects[0].filter.as_ref().unwrap();
                let conditions = filter.conditions();
                let name_cond = conditions.get("name").unwrap();
                assert_eq!(name_cond.get("_eq"), Some(&json!("Alice")));
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_in_limit() {
        let query = r#"
            query($lim: Int!) {
                Users(limit: $lim) {
                    _docID
                }
            }
        "#;

        let variables = HashMap::from([("lim".to_string(), json!(10))]);

        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                assert_eq!(selects[0].limit.as_ref().unwrap().limit, Some(10));
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_in_doc_ids() {
        let query = r#"
            query($ids: [String!]!) {
                Users(docIDs: $ids) {
                    _docID
                    name
                }
            }
        "#;

        let variables = HashMap::from([("ids".to_string(), json!(["bae-123", "bae-456"]))]);

        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                assert_eq!(
                    selects[0].doc_ids,
                    Some(vec!["bae-123".to_string(), "bae-456".to_string()])
                );
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_in_mutation_input() {
        let query = r#"
            mutation($userName: String!, $userAge: Int!) {
                create_Users(input: [{name: $userName, age: $userAge}]) {
                    _docID
                }
            }
        "#;

        let variables = HashMap::from([
            ("userName".to_string(), json!("Bob")),
            ("userAge".to_string(), json!(25)),
        ]);

        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Mutation(mutations) => {
                assert_eq!(mutations.len(), 1);
                let input = &mutations[0].create_input[0];
                assert_eq!(input.get("name"), Some(&json!("Bob")));
                assert_eq!(input.get("age"), Some(&json!(25)));
            }
            _ => panic!("Expected mutation"),
        }
    }

    #[test]
    fn test_variable_in_mutation_doc_ids() {
        let query = r#"
            mutation($id: String!) {
                delete_Users(docIDs: [$id]) {
                    _docID
                }
            }
        "#;

        let variables = HashMap::from([("id".to_string(), json!("bae-999"))]);

        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Mutation(mutations) => {
                assert_eq!(mutations[0].doc_ids, Some(vec!["bae-999".to_string()]));
            }
            _ => panic!("Expected mutation"),
        }
    }

    #[test]
    fn test_undefined_variable_error() {
        let query = r#"
            query {
                Users(filter: {name: {_eq: $undefined}}) {
                    _docID
                }
            }
        "#;

        let variables = HashMap::new();
        let result = parse_request_with_variables(query, Some(&variables));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("was not provided"));
    }

    #[test]
    fn test_no_variables_provided_error() {
        let query = r#"
            query {
                Users(filter: {name: {_eq: $name}}) {
                    _docID
                }
            }
        "#;

        let result = parse_request_with_variables(query, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no variables provided"));
    }

    #[test]
    fn test_query_without_variables_still_works() {
        let query = r#"
            {
                Users(filter: {name: {_eq: "Alice"}}) {
                    _docID
                    name
                }
            }
        "#;

        // No variables provided
        let result = parse_request_with_variables(query, None).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                assert_eq!(selects.len(), 1);
                let filter = selects[0].filter.as_ref().unwrap();
                let conditions = filter.conditions();
                let name_cond = conditions.get("name").unwrap();
                assert_eq!(name_cond.get("_eq"), Some(&json!("Alice")));
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_type_mismatch_int() {
        let query = r#"
            query($lim: Int!) {
                Users(limit: $lim) {
                    _docID
                }
            }
        "#;

        // Provide string instead of int
        let variables = HashMap::from([("lim".to_string(), json!("not an int"))]);
        let result = parse_request_with_variables(query, Some(&variables));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be of type Int"));
    }

    #[test]
    fn test_multiple_variables() {
        let query = r#"
            query($name: String!, $minAge: Int!, $lim: Int!) {
                Users(filter: {name: {_eq: $name}, age: {_gte: $minAge}}, limit: $lim) {
                    _docID
                    name
                    age
                }
            }
        "#;

        let variables = HashMap::from([
            ("name".to_string(), json!("Alice")),
            ("minAge".to_string(), json!(18)),
            ("lim".to_string(), json!(5)),
        ]);

        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                assert_eq!(selects[0].limit.as_ref().unwrap().limit, Some(5));
                let filter = selects[0].filter.as_ref().unwrap();
                let conditions = filter.conditions();
                assert_eq!(
                    conditions.get("name").unwrap().get("_eq"),
                    Some(&json!("Alice"))
                );
                assert_eq!(conditions.get("age").unwrap().get("_gte"), Some(&json!(18)));
            }
            _ => panic!("Expected query"),
        }
    }

    // =========================================================================
    // Variable type mismatch tests
    // =========================================================================

    #[test]
    fn test_variable_type_mismatch_bool() {
        let query = r#"
            query($deleted: Boolean!) {
                Users(showDeleted: $deleted) {
                    _docID
                }
            }
        "#;

        // Provide string instead of bool
        let variables = HashMap::from([("deleted".to_string(), json!("true"))]);
        let result = parse_request_with_variables(query, Some(&variables));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be of type Boolean"));
    }

    #[test]
    fn test_variable_type_mismatch_string() {
        let query = r#"
            query($c: String!) {
                Users(cid: $c) {
                    _docID
                }
            }
        "#;

        // Provide integer instead of string
        let variables = HashMap::from([("c".to_string(), json!(12345))]);
        let result = parse_request_with_variables(query, Some(&variables));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be of type String"));
    }

    #[test]
    fn test_variable_in_order_direction() {
        let query = r#"
            query($dir: String!) {
                Users(order: {name: $dir}) {
                    _docID
                    name
                }
            }
        "#;

        let variables = HashMap::from([("dir".to_string(), json!("DESC"))]);
        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                let order = selects[0].order_by.as_ref().unwrap();
                assert_eq!(
                    order.conditions[0].direction,
                    crate::mapper::OrderDirection::Desc
                );
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_invalid_order_direction() {
        let query = r#"
            query($dir: String!) {
                Users(order: {name: $dir}) {
                    _docID
                }
            }
        "#;

        let variables = HashMap::from([("dir".to_string(), json!("INVALID"))]);
        let result = parse_request_with_variables(query, Some(&variables));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid order direction"));
    }

    // =========================================================================
    // Variable default value tests
    // =========================================================================

    #[test]
    fn test_variable_default_value_used_when_not_provided() {
        let query = r#"
            query($name: String = "DefaultName") {
                Users(filter: {name: {_eq: $name}}) {
                    _docID
                    name
                }
            }
        "#;

        // Don't provide the variable - should use default
        let result = parse_request_with_variables(query, None).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                let filter = selects[0].filter.as_ref().unwrap();
                let conditions = filter.conditions();
                assert_eq!(
                    conditions.get("name").unwrap().get("_eq"),
                    Some(&json!("DefaultName"))
                );
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_provided_value_overrides_default() {
        let query = r#"
            query($name: String = "DefaultName") {
                Users(filter: {name: {_eq: $name}}) {
                    _docID
                    name
                }
            }
        "#;

        // Provide a value - should override default
        let variables = HashMap::from([("name".to_string(), json!("ProvidedName"))]);
        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                let filter = selects[0].filter.as_ref().unwrap();
                let conditions = filter.conditions();
                assert_eq!(
                    conditions.get("name").unwrap().get("_eq"),
                    Some(&json!("ProvidedName"))
                );
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_default_int_value() {
        let query = r#"
            query($lim: Int = 50) {
                Users(limit: $lim) {
                    _docID
                }
            }
        "#;

        // Don't provide the variable - should use default 50
        let result = parse_request_with_variables(query, None).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                assert_eq!(selects[0].limit.as_ref().unwrap().limit, Some(50));
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_default_boolean_value() {
        let query = r#"
            query($deleted: Boolean = true) {
                Users(showDeleted: $deleted) {
                    _docID
                }
            }
        "#;

        // Don't provide the variable - should use default true
        let result = parse_request_with_variables(query, None).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                assert!(selects[0].show_deleted);
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_multiple_variables_with_some_defaults() {
        let query = r#"
            query($name: String!, $minAge: Int = 18, $lim: Int = 10) {
                Users(filter: {name: {_eq: $name}, age: {_gte: $minAge}}, limit: $lim) {
                    _docID
                    name
                }
            }
        "#;

        // Only provide $name, use defaults for $minAge and $lim
        let variables = HashMap::from([("name".to_string(), json!("Alice"))]);
        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                assert_eq!(selects[0].limit.as_ref().unwrap().limit, Some(10));
                let filter = selects[0].filter.as_ref().unwrap();
                let conditions = filter.conditions();
                assert_eq!(
                    conditions.get("name").unwrap().get("_eq"),
                    Some(&json!("Alice"))
                );
                assert_eq!(conditions.get("age").unwrap().get("_gte"), Some(&json!(18)));
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_mutation_variable_default_value() {
        let query = r#"
            mutation($name: String = "DefaultUser") {
                create_Users(input: [{name: $name}]) {
                    _docID
                }
            }
        "#;

        // Don't provide the variable - should use default
        let result = parse_request_with_variables(query, None).unwrap();
        match result {
            ParsedOperation::Mutation(mutations) => {
                let input = &mutations[0].create_input;
                assert_eq!(input[0].get("name"), Some(&json!("DefaultUser")));
            }
            _ => panic!("Expected mutation"),
        }
    }

    #[test]
    fn test_variable_default_array_value() {
        let query = r#"
            query($ids: [String!] = ["id1", "id2"]) {
                Users(docIDs: $ids) {
                    _docID
                }
            }
        "#;

        // Don't provide the variable - should use default array
        let result = parse_request_with_variables(query, None).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                let doc_ids = selects[0].doc_ids.as_ref().unwrap();
                assert_eq!(doc_ids.len(), 2);
                assert_eq!(doc_ids[0], "id1");
                assert_eq!(doc_ids[1], "id2");
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_default_null_value() {
        let query = r#"
            query($name: String = null) {
                Users(filter: {name: {_eq: $name}}) {
                    _docID
                }
            }
        "#;

        // Don't provide the variable - should use default null
        let result = parse_request_with_variables(query, None).unwrap();
        match result {
            ParsedOperation::Query { selects, .. } => {
                let filter = selects[0].filter.as_ref().unwrap();
                let conditions = filter.conditions();
                assert_eq!(
                    conditions.get("name").unwrap().get("_eq"),
                    Some(&JsonValue::Null)
                );
            }
            _ => panic!("Expected query"),
        }
    }

    #[test]
    fn test_variable_default_cannot_reference_other_variable() {
        // Note: GraphQL spec doesn't allow variable references in default values
        // graphql-parser rejects this at parse time with a parse error
        let query = r#"
            query($a: String = $b, $b: String = "test") {
                Users(filter: {name: {_eq: $a}}) {
                    _docID
                }
            }
        "#;

        let result = parse_request_with_variables(query, None);
        // graphql-parser rejects this at the parse level since variable references
        // aren't allowed in default value position per the GraphQL spec
        assert!(
            result.is_err(),
            "Expected error for variable reference in default value, but got: {:?}",
            result
        );
    }
}

#[cfg(test)]
mod subscription_tests {
    use super::*;

    #[test]
    fn test_parse_subscription_basic() {
        let query = r#"
            subscription {
                User {
                    _docID
                    name
                }
            }
        "#;

        let result = parse_request(query).unwrap();
        match result {
            ParsedOperation::Subscription { select } => {
                assert_eq!(select.collection_name, "User");
                assert_eq!(select.fields.len(), 2);
            }
            _ => panic!("Expected subscription"),
        }
    }

    #[test]
    fn test_parse_subscription_with_filter() {
        let query = r#"
            subscription {
                User(filter: {active: {_eq: true}}) {
                    _docID
                    name
                    email
                }
            }
        "#;

        let result = parse_request(query).unwrap();
        match result {
            ParsedOperation::Subscription { select } => {
                assert_eq!(select.collection_name, "User");
                assert!(select.filter.is_some());
            }
            _ => panic!("Expected subscription"),
        }
    }

    #[test]
    fn test_parse_subscription_with_variables() {
        let query = r#"
            subscription($active: Boolean!) {
                User(filter: {active: {_eq: $active}}) {
                    _docID
                    name
                }
            }
        "#;

        let variables = HashMap::from([("active".to_string(), serde_json::json!(true))]);
        let result = parse_request_with_variables(query, Some(&variables)).unwrap();
        match result {
            ParsedOperation::Subscription { select } => {
                assert_eq!(select.collection_name, "User");
                let filter = select.filter.as_ref().unwrap();
                let conditions = filter.conditions();
                assert_eq!(
                    conditions.get("active").unwrap().get("_eq"),
                    Some(&serde_json::json!(true))
                );
            }
            _ => panic!("Expected subscription"),
        }
    }

    #[test]
    fn test_parse_subscription_multiple_root_fields_error() {
        let query = r#"
            subscription {
                User {
                    name
                }
                Post {
                    title
                }
            }
        "#;

        let result = parse_request(query);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exactly one root field"));
    }

    #[test]
    fn test_parse_subscription_with_nested_fields() {
        let query = r#"
            subscription {
                User {
                    _docID
                    name
                    posts {
                        _docID
                        title
                    }
                }
            }
        "#;

        let result = parse_request(query).unwrap();
        match result {
            ParsedOperation::Subscription { select } => {
                assert_eq!(select.collection_name, "User");
                // Should have 3 fields: _docID, name, and posts (nested)
                assert_eq!(select.fields.len(), 3);
            }
            _ => panic!("Expected subscription"),
        }
    }

    #[test]
    fn test_cannot_mix_subscription_and_query() {
        // GraphQL doesn't allow mixing operation types in a single document
        // But we can test that our parser handles it correctly
        let query = r#"
            subscription {
                User { name }
            }
        "#;

        let query_op = r#"
            query {
                User { name }
            }
        "#;

        // Each should parse independently
        assert!(matches!(
            parse_request(query).unwrap(),
            ParsedOperation::Subscription { .. }
        ));
        assert!(matches!(
            parse_request(query_op).unwrap(),
            ParsedOperation::Query { .. }
        ));
    }

    #[test]
    fn test_subscription_with_doc_id() {
        let query = r#"
            subscription {
                User(docID: "bae-123") {
                    _docID
                    name
                }
            }
        "#;

        let result = parse_request(query).unwrap();
        match result {
            ParsedOperation::Subscription { select } => {
                assert_eq!(select.doc_ids, Some(vec!["bae-123".to_string()]));
            }
            _ => panic!("Expected subscription"),
        }
    }

    #[test]
    fn test_subscription_with_default_variable() {
        let query = r#"
            subscription($limit: Int = 10) {
                User(limit: $limit) {
                    _docID
                    name
                }
            }
        "#;

        // Don't provide the variable - should use default
        let result = parse_request_with_variables(query, None).unwrap();
        match result {
            ParsedOperation::Subscription { select } => {
                assert_eq!(select.limit.as_ref().unwrap().limit, Some(10));
            }
            _ => panic!("Expected subscription"),
        }
    }
}
