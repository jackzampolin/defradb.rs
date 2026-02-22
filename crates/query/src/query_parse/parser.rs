//! GraphQL query parser
//!
//! Parses GraphQL query strings into Select and Mutation operations for execution.

use graphql_parser::query::{
    Definition, Directive, Document, Field, FragmentDefinition, OperationDefinition, Selection,
    SelectionSet, Value, VariableDefinition,
};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use tracing::instrument;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{AggregateType, Field as SelectField, Limit, Mutation, Requestable, Select};

use super::aggregates::{parse_aggregate_field, parse_group_by_value, parse_top_level_aggregate};
use super::filters::parse_filter_value;
use super::limits::validate_select_limits;
use super::mutations::{parse_field_to_mutation, parse_similarity_field};
use super::ordering::parse_order_value;

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
    pub fn parse_str(s: &str) -> Option<Self> {
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
    Mutation {
        mutations: Vec<Mutation>,
        /// Whether @explain directive was used and which type
        explain: Option<ExplainType>,
    },
    /// Subscription operations (single root field only per GraphQL spec)
    Subscription {
        /// The single select for the subscription.
        select: Box<Select>,
    },
    /// Introspection query (__schema or __type)
    ///
    /// Introspection queries are handled separately using the GraphQL schema
    /// rather than the document storage.
    Introspection {
        /// The original query string to be executed against the schema
        query: String,
    },
}

/// Check if a directive list contains @explain and parse its type.
/// Returns Ok(Some(ExplainType)) if @explain is present, Ok(None) if not,
/// or Err if @explain has an invalid type argument.
fn parse_explain_directive(directives: &[Directive<'_, String>]) -> Result<Option<ExplainType>> {
    for directive in directives {
        if directive.name == "explain" {
            // Check for type argument: @explain(type: simple|execute|debug)
            for (name, value) in &directive.arguments {
                if name == "type" {
                    let type_str = match value {
                        Value::Enum(s) => s.as_str(),
                        Value::String(s) => s.as_str(),
                        _ => {
                            return Err(QueryError::parse(
                                "Argument \"type\" has invalid value.\nExpected type \"ExplainType\".",
                            ));
                        }
                    };
                    if let Some(explain_type) = ExplainType::parse_str(type_str) {
                        return Ok(Some(explain_type));
                    }
                    return Err(QueryError::parse(format!(
                        "Argument \"type\" has invalid value {}.\nExpected type \"ExplainType\", found {}.",
                        type_str, type_str
                    )));
                }
            }
            // No type argument - default to Simple
            return Ok(Some(ExplainType::Simple));
        }
    }
    Ok(None)
}

/// Check if a field's directive list contains @explain (which is invalid on fields).
/// Returns an error if @explain is found on a field selection.
fn check_field_explain_directive(directives: &[Directive<'_, String>]) -> Result<()> {
    for directive in directives {
        if directive.name == "explain" {
            return Err(QueryError::parse(
                "Directive \"explain\" may not be used on FIELD.".to_string(),
            ));
        }
    }
    Ok(())
}

/// Type alias for fragment definitions map
pub(super) type FragmentMap<'a> = HashMap<String, &'a FragmentDefinition<'a, String>>;

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
            // Check for @explain directive on field (invalid - must be on operation)
            check_field_explain_directive(&field.directives)?;
            // Check if this is a top-level aggregate (e.g., _avg(Users: {field: Age}))
            if let Some(agg_type) = AggregateType::parse(&field.name) {
                let select = parse_top_level_aggregate(field, agg_type, variables)?;
                selects.push(select);
            } else {
                let select = parse_field_to_select(field, variables, fragments, visiting)?;
                selects.push(select);
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
/// For introspection queries, use `parse_request` and handle the Introspection variant.
pub fn parse_query(query: &str) -> Result<Vec<Select>> {
    match parse_request(query)? {
        ParsedOperation::Query { selects, .. } => Ok(selects),
        ParsedOperation::Mutation { .. } => Err(QueryError::parse(
            "Expected query but got mutation. Use parse_request() for mutations.",
        )),
        ParsedOperation::Subscription { .. } => Err(QueryError::parse(
            "Expected query but got subscription. Use parse_request() for subscriptions.",
        )),
        ParsedOperation::Introspection { .. } => Err(QueryError::parse(
            "Expected data query but got introspection query. Use parse_request() for introspection.",
        )),
    }
}

/// Parse a GraphQL query string with variable substitution.
///
/// Returns a vector of Select operations, one for each top-level field in the query.
/// For mutations, use `parse_mutations_with_variables` instead.
pub fn parse_query_with_variables(
    query: &str,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Vec<Select>> {
    match parse_request_with_variables(query, variables, None)? {
        ParsedOperation::Query { selects, .. } => Ok(selects),
        ParsedOperation::Mutation { .. } => Err(QueryError::parse(
            "Expected query but got mutation. Use parse_mutations_with_variables() for mutations.",
        )),
        ParsedOperation::Subscription { .. } => {
            Err(QueryError::parse("Expected query but got subscription."))
        }
        ParsedOperation::Introspection { .. } => {
            Err(QueryError::parse("Expected query but got introspection."))
        }
    }
}

/// Parse a GraphQL mutation string into Mutation operations.
///
/// Returns a vector of Mutation operations, one for each top-level field in the mutation.
pub fn parse_mutations(query: &str) -> Result<Vec<Mutation>> {
    parse_mutations_with_variables(query, None)
}

/// Parse a GraphQL mutation string with variable substitution.
///
/// Returns a vector of Mutation operations, one for each top-level field in the mutation.
pub fn parse_mutations_with_variables(
    query: &str,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Vec<Mutation>> {
    match parse_request_with_variables(query, variables, None)? {
        ParsedOperation::Mutation { mutations, .. } => Ok(mutations),
        ParsedOperation::Query { .. } => Err(QueryError::parse("Expected mutation but got query")),
        ParsedOperation::Subscription { .. } => {
            Err(QueryError::parse("Expected mutation but got subscription"))
        }
        ParsedOperation::Introspection { .. } => Err(QueryError::parse(
            "Expected mutation but got introspection query",
        )),
    }
}

/// Parse a GraphQL request (query or mutation) into operations.
///
/// This is the main entry point for parsing GraphQL requests.
/// For queries with variables, use `parse_request_with_variables` instead.
pub fn parse_request(query: &str) -> Result<ParsedOperation> {
    parse_request_with_variables(query, None, None)
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
/// Check if a document is an introspection query.
///
/// Returns true if any root-level field is `__schema` or `__type`.
fn is_introspection_query(doc: &Document<'_, String>) -> bool {
    for def in &doc.definitions {
        if let Definition::Operation(op) = def {
            let selections = match op {
                OperationDefinition::Query(q) => &q.selection_set.items,
                OperationDefinition::SelectionSet(ss) => &ss.items,
                _ => continue,
            };

            for selection in selections {
                if let Selection::Field(field) = selection {
                    if field.name == "__schema" || field.name == "__type" {
                        return true;
                    }
                }
            }
        }
    }
    false
}

#[instrument(name = "query.parse", skip(query, variables), fields(query_len = query.len()))]
pub fn parse_request_with_variables(
    query: &str,
    variables: Option<&HashMap<String, JsonValue>>,
    operation_name: Option<&str>,
) -> Result<ParsedOperation> {
    let doc: Document<'_, String> =
        graphql_parser::parse_query(query).map_err(|e| QueryError::parse(e.to_string()))?;

    // Check for introspection queries (__schema, __type) before normal parsing
    // These are handled separately by executing against the GraphQL schema
    if is_introspection_query(&doc) {
        return Ok(ParsedOperation::Introspection {
            query: query.to_string(),
        });
    }

    // First pass: collect all fragment definitions
    let mut fragments: HashMap<String, &FragmentDefinition<'_, String>> = HashMap::new();
    for def in &doc.definitions {
        if let Definition::Fragment(frag) = def {
            fragments.insert(frag.name.clone(), frag);
        }
    }

    // Count operations (not fragments) to validate operation_name
    let operation_count = doc
        .definitions
        .iter()
        .filter(|d| matches!(d, Definition::Operation(_)))
        .count();
    if operation_count > 1 && operation_name.is_none() {
        return Err(QueryError::parse(
            "Must provide operation name if query contains multiple operations.",
        ));
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
                // If operation_name is specified, skip operations that don't match
                if let Some(target_name) = operation_name {
                    let op_name = match op {
                        OperationDefinition::Query(q) => q.name.as_deref(),
                        OperationDefinition::Mutation(m) => m.name.as_deref(),
                        OperationDefinition::Subscription(s) => s.name.as_deref(),
                        OperationDefinition::SelectionSet(_) => None,
                    };
                    if op_name != Some(target_name) {
                        continue;
                    }
                }

                match op {
                    OperationDefinition::Query(q) => {
                        has_query = true;
                        // Check for @explain directive and parse type
                        if let Some(explain_type) = parse_explain_directive(&q.directives)? {
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
                        // Check for @explain directive and parse type
                        if let Some(explain_type) = parse_explain_directive(&m.directives)? {
                            explain = Some(explain_type);
                        }

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
        let select = subscription_selects.into_iter().next().unwrap();
        validate_select_limits(&select)?;
        Ok(ParsedOperation::Subscription {
            select: Box::new(select),
        })
    } else if has_mutation {
        Ok(ParsedOperation::Mutation { mutations, explain })
    } else {
        for select in &selects {
            validate_select_limits(select)?;
        }
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
    let (collection_name, is_encrypted) = if field.name.starts_with("encrypted_") {
        (field.name["encrypted_".len()..].to_string(), true)
    } else {
        (field.name.clone(), false)
    };
    let alias = field.alias.clone();

    let mut select = Select::new(&collection_name);
    select.is_encrypted = is_encrypted;
    // Preserve original field name (e.g. "encrypted_User") as the response key
    if is_encrypted {
        select.field = SelectField::with_alias(&collection_name, field.name.clone());
    }
    if let Some(a) = alias {
        select.field = SelectField::with_alias(&collection_name, a);
    }

    // Parse arguments (filter, limit, offset, order, docIDs, etc.)
    for (arg_name, arg_value) in &field.arguments {
        match arg_name.as_str() {
            "filter" => {
                // Null filter is valid and means "no filter" (operate on all docs)
                if !matches!(arg_value, Value::Null) {
                    // Validate _and/_or arrays don't contain null elements
                    if let Value::Object(obj) = arg_value {
                        for (key, val) in obj {
                            if key == "_and" || key == "_or" {
                                if let Value::List(items) = val {
                                    for item in items {
                                        if matches!(item, Value::Null) {
                                            return Err(QueryError::parse(format!(
                                                "Expected \"{}FilterArg!\", found null",
                                                collection_name
                                            )));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let filter = parse_filter_value(arg_value, variables)?;
                    select.filter = Some(filter);
                }
            }
            "limit" => {
                // null means "no limit" (skip setting it)
                if let Some(limit_val) = parse_optional_int_value(arg_value, variables)? {
                    if limit_val < 0 {
                        return Err(QueryError::parse("limit must be non-negative"));
                    }
                    select.limit = Some(Limit::new(
                        Some(limit_val as u64),
                        select.limit.as_ref().map(|l| l.offset).unwrap_or(0),
                    ));
                }
            }
            "offset" => {
                // null means "no offset" (skip setting it)
                if let Some(offset_val) = parse_optional_int_value(arg_value, variables)? {
                    if offset_val < 0 {
                        return Err(QueryError::parse("offset must be non-negative"));
                    }
                    select.limit = Some(Limit::new(
                        select.limit.as_ref().and_then(|l| l.limit),
                        offset_val as u64,
                    ));
                }
            }
            "order" => {
                // null means "no ordering" (skip setting it)
                if !matches!(arg_value, Value::Null) {
                    let order_by = parse_order_value(arg_value, variables)?;
                    select.order_by = Some(order_by);
                }
            }
            "groupBy" => {
                // null means "no grouping" (skip setting it)
                if !matches!(arg_value, Value::Null) {
                    let group_by = parse_group_by_value(arg_value, variables)?;
                    select.group_by = Some(group_by);
                }
            }
            "docIDs" | "docID" => {
                // Null docIDs is valid and means "no specific docIDs" (use filter or all)
                if !matches!(arg_value, Value::Null) {
                    let doc_ids = parse_doc_ids_value(arg_value, variables)?;
                    select.doc_ids = Some(doc_ids);
                }
            }
            "cid" => {
                // null means "no cid filter" (skip setting it)
                if !matches!(arg_value, Value::Null) {
                    let cid_val = resolve_string_value(arg_value, variables, "cid")?;
                    select.cid = Some(cid_val);
                }
            }
            "showDeleted" => {
                // null means "don't show deleted" (false) - skip setting it
                if !matches!(arg_value, Value::Null) {
                    let show_deleted = resolve_bool_value(arg_value, variables, "showDeleted")?;
                    select.show_deleted = show_deleted;
                }
            }
            "depth" => {
                // depth is only valid for _commits queries
                if collection_name == "_commits" {
                    // null means "no limit" (None), not an error
                    if let Some(depth_val) = parse_optional_int_value(arg_value, variables)? {
                        if depth_val < 0 {
                            return Err(QueryError::parse("depth must be non-negative"));
                        }
                        select.depth = Some(depth_val as u64);
                    }
                    // else depth remains None (unlimited)
                } else {
                    return Err(QueryError::parse(format!(
                        "argument 'depth' is only valid for _commits queries, not '{}'",
                        collection_name
                    )));
                }
            }
            _ => {
                let valid_args = if collection_name == "_commits" {
                    "filter, limit, offset, order, groupBy, docIDs, docID, cid, showDeleted, depth"
                } else {
                    "filter, limit, offset, order, groupBy, docIDs, docID, cid, showDeleted"
                };
                return Err(QueryError::parse(format!(
                    "unknown argument '{}' on collection '{}'. Valid arguments are: {}",
                    arg_name, collection_name, valid_args
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

    // Validate groupBy field selection: when groupBy is specified, only group-by fields,
    // their FK counterparts (e.g. _authorID for groupBy [author]), and aggregate fields
    // are allowed at the group level (nested selects like _group are fine).
    if let Some(ref group_by) = select.group_by {
        for field in &select.fields {
            if let Requestable::Field(f) = field {
                // Allow special meta-fields at group level
                if f.name == "_docID" || f.name == "GROUP" || f.name == "__typename" {
                    continue;
                }
                if group_by.fields.contains(&f.name) {
                    continue;
                }
                // Allow FK fields for relation groupBy fields (e.g. _authorID for author)
                let is_fk_for_group = group_by
                    .fields
                    .iter()
                    .any(|gb_field| f.name == format!("_{}ID", gb_field));
                if is_fk_for_group {
                    continue;
                }
                return Err(QueryError::parse(
                    "cannot select a non-group-by field at group-level",
                ));
            }
        }
    }

    Ok(select)
}

/// Parse a selection set into fields and document mapping.
pub(super) fn parse_selection_set(
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

                // Check if this is a _similarity field
                if field_name == "SIMILARITY" {
                    let similarity = parse_similarity_field(field, variables)?;
                    let sim = if let Some(ref a) = alias {
                        similarity.with_alias(a.clone())
                    } else {
                        similarity
                    };

                    let index = mapping.next_index();
                    mapping.add(index, "SIMILARITY");
                    mapping.add_render_key(index, sim.output_name());

                    fields.push(Requestable::Similarity(sim));
                    continue;
                }

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
                        Requestable::Similarity(sim) => {
                            mapping.add(index, "SIMILARITY");
                            mapping.add_render_key(index, sim.output_name());
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
                        Requestable::Similarity(sim) => {
                            mapping.add(index, "SIMILARITY");
                            mapping.add_render_key(index, sim.output_name());
                        }
                    }
                    fields.push(inline_field);
                }
            }
        }
    }

    Ok((fields, mapping))
}

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
pub(super) fn graphql_value_to_json(
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
pub(super) fn parse_int_value(
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

/// Parse an optional integer value (returns None for null).
/// This matches Go DefraDB's behavior where null is treated as "not provided".
fn parse_optional_int_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Option<i64>> {
    match value {
        Value::Null => Ok(None),
        Value::Int(n) => n
            .as_i64()
            .map(Some)
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
            if json_val.is_null() {
                Ok(None)
            } else {
                json_val.as_i64().map(Some).ok_or_else(|| {
                    QueryError::parse(format!("Variable \"${}\" must be of type Int", name))
                })
            }
        }
        _ => Err(QueryError::parse("expected integer value")),
    }
}

/// Parse docIDs argument into vector of strings.
pub(super) fn parse_doc_ids_value(
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

#[cfg(test)]
#[path = "mutation_tests.rs"]
mod mutation_tests;

#[cfg(test)]
#[path = "variable_tests.rs"]
mod variable_tests;

#[cfg(test)]
#[path = "subscription_tests.rs"]
mod subscription_tests;
