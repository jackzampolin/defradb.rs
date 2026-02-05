//! GraphQL query parser
//!
//! Parses GraphQL query strings into Select and Mutation operations for execution.

use graphql_parser::query::{
    Definition, Directive, Document, Field, FragmentDefinition, OperationDefinition, Selection,
    SelectionSet, Value, VariableDefinition,
};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use tracing::instrument;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{
    parse_mutation_name, Aggregate, AggregateTarget, AggregateType, Field as SelectField, Filter,
    GroupBy, Limit, Mutation, MutationType, OrderBy, OrderCondition, OrderDirection, Requestable,
    Select, Similarity,
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
    Mutation {
        mutations: Vec<Mutation>,
        /// Whether @explain directive was used and which type
        explain: Option<ExplainType>,
    },
    /// Subscription operations (single root field only per GraphQL spec)
    Subscription {
        /// The single select for the subscription.
        select: Select,
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
                            return Err(QueryError::parse(format!(
                                "Argument \"type\" has invalid value.\nExpected type \"ExplainType\"."
                            )));
                        }
                    };
                    if let Some(explain_type) = ExplainType::from_str(type_str) {
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
        Ok(ParsedOperation::Subscription {
            select: subscription_selects.into_iter().next().unwrap(),
        })
    } else if has_mutation {
        Ok(ParsedOperation::Mutation { mutations, explain })
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
                if f.name == "_docID" || f.name == "_group" || f.name == "__typename" {
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

                // Check if this is a _similarity field
                if field_name == "_similarity" {
                    let similarity = parse_similarity_field(field, variables)?;
                    let sim = if let Some(ref a) = alias {
                        similarity.with_alias(a.clone())
                    } else {
                        similarity
                    };

                    let index = mapping.next_index();
                    mapping.add(index, "_similarity");
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
                            mapping.add(index, "_similarity");
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
                            mapping.add(index, "_similarity");
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
        Value::Variable(name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    name
                ))
            })?;
            let json_val = vars.get(name.as_str()).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", name))
            })?;
            if let JsonValue::Object(obj) = json_val {
                let conditions: HashMap<String, JsonValue> =
                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                Ok(Filter::from_conditions(conditions))
            } else {
                Err(QueryError::parse("filter must be an object"))
            }
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

/// Parse order argument into OrderBy.
/// Supports both single object `{field: ASC}` and array `[{field: ASC}, {other: DESC}]` formats.
fn parse_order_value(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<OrderBy> {
    let mut order_by = OrderBy::new();

    match value {
        Value::Object(obj) => {
            // Go DefraDB requires each order argument to only define one field
            if obj.len() > 1 {
                return Err(QueryError::parse(
                    "each order argument can only define one field",
                ));
            }
            for (field_name, direction_val) in obj {
                if let Some(condition) =
                    parse_order_condition(field_name.clone(), direction_val, variables)?
                {
                    order_by = order_by.with_condition(condition);
                }
            }
        }
        Value::List(items) => {
            // Array of order objects: [{rating: ASC}, {publisher: {yearOpened: DESC}}]
            for item in items {
                if let Value::Object(obj) = item {
                    if obj.len() > 1 {
                        return Err(QueryError::parse(
                            "each order argument can only define one field",
                        ));
                    }
                    for (field_name, direction_val) in obj {
                        if let Some(condition) =
                            parse_order_condition(field_name.clone(), direction_val, variables)?
                        {
                            order_by = order_by.with_condition(condition);
                        }
                    }
                } else {
                    return Err(QueryError::parse(
                        "each order item in array must be an object",
                    ));
                }
            }
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
            return parse_order_from_json(json_val);
        }
        _ => return Err(QueryError::parse("order must be an object or array")),
    }

    Ok(order_by)
}

/// Parse order from a resolved JSON variable value.
fn parse_order_from_json(json: &JsonValue) -> Result<OrderBy> {
    let mut order_by = OrderBy::new();
    match json {
        JsonValue::Object(obj) => {
            for (field_name, dir_val) in obj {
                if let Some(dir_str) = dir_val.as_str() {
                    if let Some(direction) = OrderDirection::parse(dir_str) {
                        order_by =
                            order_by.with_condition(OrderCondition::new(field_name, direction));
                    }
                }
            }
        }
        JsonValue::Array(items) => {
            for item in items {
                if let JsonValue::Object(obj) = item {
                    for (field_name, dir_val) in obj {
                        if let Some(dir_str) = dir_val.as_str() {
                            if let Some(direction) = OrderDirection::parse(dir_str) {
                                order_by = order_by
                                    .with_condition(OrderCondition::new(field_name, direction));
                            }
                        }
                    }
                }
            }
        }
        _ => {
            return Err(QueryError::parse(
                "order variable must be an object or array",
            ))
        }
    }
    Ok(order_by)
}

/// Parse a single order condition, handling nested relation ordering.
/// Supports both simple `{field: ASC}` and nested `{relation: {field: DESC}}`.
/// Returns None for null values (order field is ignored).
fn parse_order_condition(
    field_name: String,
    direction_val: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Option<OrderCondition>> {
    match direction_val {
        // Null order direction means skip this field (Go compatibility)
        Value::Null => Ok(None),
        Value::Enum(s) | Value::String(s) => {
            let direction = OrderDirection::parse(s).ok_or_else(|| {
                // Match Go DefraDB error format
                QueryError::parse(format!(
                    "Argument \"order\" has invalid value {{{}: {}}}",
                    field_name, s
                ))
            })?;
            Ok(Some(OrderCondition::new(field_name, direction)))
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
            let direction = OrderDirection::parse(s).ok_or_else(|| {
                // Match Go DefraDB error format
                QueryError::parse(format!(
                    "Argument \"order\" has invalid value {{{}: {}}}",
                    field_name, s
                ))
            })?;
            Ok(Some(OrderCondition::new(field_name, direction)))
        }
        Value::Object(nested_obj) => {
            // Nested ordering: {relation: {field: ASC}} or {_alias: {aliasName: ASC}}
            // Empty nested order is a no-op (Go compatibility)
            if nested_obj.is_empty() {
                return Ok(None);
            }
            // Recursively parse the nested object
            if nested_obj.len() != 1 {
                return Err(QueryError::parse(
                    "nested order must have exactly one field",
                ));
            }
            let (nested_field, nested_direction) = nested_obj.iter().next().unwrap();
            let nested_condition =
                parse_order_condition(nested_field.clone(), nested_direction, variables)?;
            // If nested condition is None (null value), propagate the None
            match nested_condition {
                Some(mut cond) => {
                    // Handle _alias directive: don't prepend "_alias", just use the nested field name.
                    // This allows ordering by aliased fields like: order: {_alias: {MyAge: ASC}}
                    // where MyAge is an alias for the Age field.
                    if field_name != "_alias" {
                        // For regular nested ordering (relations), prepend the parent field to the path
                        cond.fields.insert(0, field_name);
                    }
                    Ok(Some(cond))
                }
                None => Ok(None),
            }
        }
        _ => Err(QueryError::parse("order direction must be ASC or DESC")),
    }
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

/// Parse an aggregate target from a GraphQL Object value.
fn parse_aggregate_target_obj(
    arg_name: &str,
    obj: &BTreeMap<String, Value<'_, String>>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<AggregateTarget> {
    let mut target = AggregateTarget::new(arg_name.to_string());
    for (key, val) in obj {
        match key.as_str() {
            "field" => {
                let field_name = match val {
                    Value::String(s) => s.clone(),
                    Value::Enum(s) => s.clone(),
                    _ => {
                        return Err(QueryError::parse(
                            "field in relation aggregate must be a string",
                        ))
                    }
                };
                target.field_name = Some(field_name);
            }
            "filter" => {
                let filter = parse_filter_value(val, variables)?;
                target.filter = Some(filter);
            }
            "limit" => {
                let limit_val = parse_int_value(val, variables)?;
                target.limit = Some(Limit::new(Some(limit_val as u64), 0));
            }
            "offset" => {
                let offset_val = parse_int_value(val, variables)?;
                if let Some(ref mut limit) = target.limit {
                    limit.offset = offset_val as u64;
                } else {
                    target.limit = Some(Limit::new(None, offset_val as u64));
                }
            }
            "order" => {
                let order = match val {
                    Value::Enum(s) | Value::String(s) => {
                        let direction = OrderDirection::parse(s).ok_or_else(|| {
                            QueryError::parse(format!("invalid order direction: {}", s))
                        })?;
                        OrderBy::new().with_condition(OrderCondition::new("", direction))
                    }
                    _ => parse_order_value(val, variables)?,
                };
                target.order = Some(order);
            }
            _ => {}
        }
    }
    Ok(target)
}

/// Parse an aggregate target from a resolved JSON variable value.
fn parse_aggregate_target_from_json(
    arg_name: &str,
    json: &JsonValue,
    _variables: Option<&HashMap<String, JsonValue>>,
) -> Result<AggregateTarget> {
    let mut target = AggregateTarget::new(arg_name.to_string());
    if let JsonValue::Object(obj) = json {
        for (key, val) in obj {
            match key.as_str() {
                "field" => {
                    if let Some(s) = val.as_str() {
                        target.field_name = Some(s.to_string());
                    }
                }
                "filter" => {
                    // Convert JSON filter to a Filter
                    if let JsonValue::Object(filter_obj) = val {
                        let conditions: HashMap<String, JsonValue> = filter_obj
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        target.filter = Some(Filter::from_conditions(conditions));
                    }
                }
                "limit" => {
                    if let Some(n) = val.as_i64() {
                        target.limit = Some(Limit::new(Some(n as u64), 0));
                    }
                }
                "offset" => {
                    if let Some(n) = val.as_i64() {
                        if let Some(ref mut limit) = target.limit {
                            limit.offset = n as u64;
                        } else {
                            target.limit = Some(Limit::new(None, n as u64));
                        }
                    }
                }
                "order" => {
                    target.order = Some(parse_order_from_json(val)?);
                }
                _ => {}
            }
        }
    }
    Ok(target)
}

/// Parse an aggregate field into an Aggregate.
///
/// Handles aggregate functions like `_count`, `_sum(field: "age")`, etc.
/// Also supports relation aggregates like `_count(books: {})`, `_sum(books: {field: score}, articles: {field: rating})`.
fn parse_aggregate_field(
    field: &Field<'_, String>,
    agg_type: AggregateType,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Aggregate> {
    let mut target_field: Option<String> = None;
    let mut relation_targets: Vec<AggregateTarget> = Vec::new();

    // Parse arguments (e.g., `field: "age"` for _sum, or `books: {}` for relation aggregates)
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
                // This might be a relation name argument like `books: {field: score}`
                // Relation arguments take an object value with optional field, filter, limit, order.
                // Also handle variables that resolve to objects.
                match arg_value {
                    Value::Object(obj) => {
                        let target = parse_aggregate_target_obj(arg_name, obj, variables)?;
                        relation_targets.push(target);
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
                        let target =
                            parse_aggregate_target_from_json(arg_name, json_val, variables)?;
                        relation_targets.push(target);
                    }
                    _ => {
                        return Err(QueryError::parse(format!(
                            "unknown argument '{}' on aggregate '{}', expected an object value for relation aggregates",
                            arg_name,
                            agg_type.as_str()
                        )));
                    }
                }
            }
        }
    }

    // Create the appropriate aggregate
    let aggregate = if !relation_targets.is_empty() {
        // Relation aggregate mode - create aggregate with targets directly (no empty initial target)
        Aggregate {
            aggregate_type: agg_type,
            targets: relation_targets,
            filter: None,
            alias: None,
        }
    } else {
        // Simple field aggregate mode
        match agg_type {
            AggregateType::Count => {
                // _count can work without a field argument (counts all docs)
                if let Some(field_name) = target_field {
                    Aggregate::count().with_target(AggregateTarget::with_field("", field_name))
                } else {
                    Aggregate::count()
                }
            }
            AggregateType::Sum => {
                let field_name = target_field.ok_or_else(|| {
                    QueryError::parse("_sum requires a 'field' argument or relation targets")
                })?;
                Aggregate::sum(AggregateTarget::with_field("", field_name))
            }
            AggregateType::Average => {
                let field_name = target_field.ok_or_else(|| {
                    QueryError::parse("_avg requires a 'field' argument or relation targets")
                })?;
                Aggregate::avg(AggregateTarget::with_field("", field_name))
            }
            AggregateType::Min => {
                let field_name = target_field.ok_or_else(|| {
                    QueryError::parse("_min requires a 'field' argument or relation targets")
                })?;
                Aggregate::min(AggregateTarget::with_field("", field_name))
            }
            AggregateType::Max => {
                let field_name = target_field.ok_or_else(|| {
                    QueryError::parse("_max requires a 'field' argument or relation targets")
                })?;
                Aggregate::max(AggregateTarget::with_field("", field_name))
            }
        }
    };

    Ok(aggregate)
}

/// Parse a top-level aggregate query (e.g., `{ _avg(Users: {field: Age}) }`).
///
/// Top-level aggregates are different from nested aggregates in that:
/// - The aggregate function name is the top-level field
/// - Arguments are collection names with their aggregate configuration
/// - The result is wrapped in a Select with the collection as the target
fn parse_top_level_aggregate(
    field: &Field<'_, String>,
    agg_type: AggregateType,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Select> {
    let mut aggregate = parse_aggregate_field(field, agg_type, variables)?;

    // Top-level numeric aggregates require a field argument on each target.
    // This matches Go's GraphQL schema validation where collection args require
    // the "field" key (e.g., _avg(Users: {field: Age}) not _avg(Users: {})).
    if agg_type != AggregateType::Count {
        for target in &aggregate.targets {
            if target.field_name.is_none() {
                let type_name = format!("{}NumericFieldsArg!", capitalize_first(&target.host_name));
                return Err(QueryError::parse(format!(
                    "Argument \"{}\" has invalid value {{}}.\nIn field \"field\": Expected \"{}\", found null.",
                    target.host_name,
                    type_name
                )));
            }
        }
    }

    // Set alias if provided
    if let Some(ref a) = field.alias {
        aggregate = aggregate.with_alias(a.clone());
    }

    // Get the collection name from the first target
    let collection_name = aggregate
        .targets
        .first()
        .map(|t| t.host_name.clone())
        .unwrap_or_else(|| String::new());

    // Create a Select that wraps this aggregate
    // The field name should be the aggregate name (e.g., "_avg") so the response
    // key is correct (e.g., {"_avg": 29} not {"Users": 29})
    let mut select = Select::new(&collection_name);
    let agg_name = agg_type.as_str();
    if let Some(ref a) = field.alias {
        // If aliased, use alias as the output name: { average: _avg(...) } -> {"average": ...}
        select.field = SelectField::with_alias(agg_name, a.clone());
    } else {
        // Otherwise use the aggregate name: { _avg(...) } -> {"_avg": ...}
        select.field = SelectField::new(agg_name);
    }

    // Add to document mapping
    let index = select.document_mapping.next_index();
    select.document_mapping.add(index, agg_name);
    select
        .document_mapping
        .add_render_key(index, aggregate.output_name());

    select.fields.push(Requestable::Aggregate(aggregate));

    Ok(select)
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

    // Capture GraphQL alias if present (e.g., "john: update_Users(...)")
    if let Some(ref alias) = field.alias {
        mutation = mutation.with_alias(alias.clone());
    }

    // Track if input argument was present (even if null)
    let mut has_input_arg = false;

    // Parse arguments based on mutation type
    for (arg_name, arg_value) in &field.arguments {
        match (mutation_type, arg_name.as_str()) {
            // CREATE: input is array of documents (null means empty operation)
            (MutationType::Create, "input") => {
                has_input_arg = true;
                if !matches!(arg_value, Value::Null) {
                    let input = parse_create_input(arg_value, variables)?;
                    mutation.create_input = input;
                }
                // null input is valid - leaves create_input empty for empty result
            }

            // UPDATE: input is patch object (null means empty operation)
            (MutationType::Update, "input") => {
                has_input_arg = true;
                if !matches!(arg_value, Value::Null) {
                    let input = parse_update_input(arg_value, variables)?;
                    mutation.update_input = input;
                }
            }

            // UPSERT: create is the document to create if no match (single object, not array)
            (MutationType::Upsert, "create") => {
                if matches!(arg_value, Value::Null) {
                    return Err(QueryError::parse(
                        "Argument \"create\" has invalid value <nil>.".to_string(),
                    ));
                }
                let input = parse_update_input(arg_value, variables)?;
                // Store create input as single-element array for consistency
                mutation.create_input = vec![input];
            }

            // UPSERT: update is the fields to update if match found
            (MutationType::Upsert, "update") => {
                if matches!(arg_value, Value::Null) {
                    return Err(QueryError::parse(
                        "Argument \"update\" has invalid value <nil>.".to_string(),
                    ));
                }
                let input = parse_update_input(arg_value, variables)?;
                mutation.update_input = input;
            }

            // UPDATE/DELETE: docID or docIDs to target (Go uses singular docID)
            (MutationType::Update | MutationType::Delete, "docID" | "docIDs" | "_docIDs") => {
                // Null docIDs is valid and means "no specific docIDs" (use filter or all)
                if !matches!(arg_value, Value::Null) {
                    let doc_ids = parse_doc_ids_value(arg_value, variables)?;
                    mutation.doc_ids = Some(doc_ids);
                }
            }

            // UPDATE/DELETE/UPSERT: filter to find documents
            (MutationType::Update | MutationType::Delete | MutationType::Upsert, "filter") => {
                // Null filter is valid and means "no filter" (operate on all docs)
                if !matches!(arg_value, Value::Null) {
                    let filter = parse_filter_value(arg_value, variables)?;
                    mutation.filter = Some(filter);
                }
            }

            // Encryption: encrypt entire document
            (_, "encrypt") => {
                if let Value::Boolean(b) = arg_value {
                    mutation.encrypt_doc = *b;
                }
            }

            // Encryption: encrypt specific fields
            (_, "encryptFields") => {
                if let Value::List(fields) = arg_value {
                    mutation.encrypt_fields = fields
                        .iter()
                        .filter_map(|v| match v {
                            Value::Enum(name) => Some(name.clone()),
                            Value::String(name) => Some(name.clone()),
                            _ => None,
                        })
                        .collect();
                }
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
            if mutation.create_input.is_empty() && !has_input_arg {
                return Err(QueryError::parse(format!(
                    "create_{} mutation requires 'input' argument with array of documents",
                    collection_name
                )));
            }
        }
        MutationType::Update => {
            if mutation.update_input.is_empty() && !has_input_arg {
                return Err(QueryError::parse(format!(
                    "update_{} mutation requires 'input' argument with fields to update",
                    collection_name
                )));
            }
            // Note: Go DefraDB allows update without docIDs or filter
            // (meaning update all documents in the collection)
        }
        MutationType::Delete => {
            // Note: Go DefraDB allows delete without docIDs or filter
            // (meaning delete all documents in the collection)
        }
        MutationType::Upsert => {
            // Go DefraDB requires all three: filter, create, update
            if mutation.filter.is_none() {
                return Err(QueryError::parse(format!(
                    "upsert_{} mutation requires 'filter' argument",
                    collection_name
                )));
            }
            if mutation.create_input.is_empty() {
                return Err(QueryError::parse(format!(
                    "upsert_{} mutation requires 'create' argument with document to create if no match",
                    collection_name
                )));
            }
            if mutation.update_input.is_empty() {
                return Err(QueryError::parse(format!(
                    "upsert_{} mutation requires 'update' argument with fields to update if match found",
                    collection_name
                )));
            }
        }
    }

    // Parse selection set (fields to return after mutation)
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

    // All mutations return [TypeName], which is an object type requiring a sub selection.
    if fields.is_empty() {
        return Err(QueryError::parse(format!(
            "Field \"{}\" of type \"[{}]\" must have a sub selection.",
            field_name, collection_name
        )));
    }

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
        // Null input is valid and means "no documents to create"
        Value::Null => Ok(vec![]),
        // Variable reference - resolve from variables map
        Value::Variable(var_name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    var_name
                ))
            })?;
            let json_val = vars.get(var_name).ok_or_else(|| {
                QueryError::parse(format!("variable '{}' not found in variables", var_name))
            })?;
            // Convert JSON value to documents
            match json_val {
                JsonValue::Array(items) => {
                    let mut docs = Vec::new();
                    for item in items {
                        if let JsonValue::Object(obj) = item {
                            let doc: HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            docs.push(doc);
                        } else {
                            return Err(QueryError::parse("CREATE input items must be objects"));
                        }
                    }
                    Ok(docs)
                }
                JsonValue::Object(obj) => {
                    let doc: HashMap<String, JsonValue> =
                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    Ok(vec![doc])
                }
                JsonValue::Null => Ok(vec![]),
                _ => Err(QueryError::parse(
                    "CREATE input variable must be an array of objects or a single object",
                )),
            }
        }
        _ => Err(QueryError::parse(
            "CREATE input must be an array of objects or a single object",
        )),
    }
}

/// Parse UPDATE mutation input (patch object).
/// Non-object input (e.g., array "patch") is treated as empty/no-op (Go compatibility).
fn parse_update_input(
    value: &Value<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<HashMap<String, JsonValue>> {
    match value {
        Value::Object(obj) => parse_document_input(obj, variables),
        _ => Ok(HashMap::new()),
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

/// Capitalize the first character of a string.
fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

/// Parse a _similarity field from a GraphQL query.
///
/// Format: `_similarity(fieldName: {vector: [1, 2, 3]})`
/// The argument name is the target field containing the document's vector.
/// The value is an object with a `vector` key containing the query vector.
fn parse_similarity_field(
    field: &Field<'_, String>,
    variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Similarity> {
    if field.arguments.is_empty() {
        return Err(QueryError::parse("_similarity requires a field argument"));
    }

    let (target_field, value) = &field.arguments[0];

    // Parse the value: {vector: [1, 2, 3]}
    let vector = match value {
        Value::Object(obj) => {
            let vec_value = obj.get("vector").ok_or_else(|| {
                QueryError::parse("_similarity argument must contain a 'vector' key")
            })?;
            parse_vector_value(vec_value, variables)?
        }
        Value::Variable(var_name) => {
            let vars = variables.ok_or_else(|| {
                QueryError::parse(format!(
                    "variable '{}' used but no variables provided",
                    var_name
                ))
            })?;
            let json_val = vars.get(var_name.as_str()).ok_or_else(|| {
                QueryError::parse(format!("Variable \"${}\" was not provided", var_name))
            })?;
            if let JsonValue::Object(obj) = json_val {
                let vec_val = obj.get("vector").ok_or_else(|| {
                    QueryError::parse("_similarity variable must contain a 'vector' key")
                })?;
                parse_json_vector(vec_val)?
            } else {
                return Err(QueryError::parse("_similarity variable must be an object"));
            }
        }
        _ => {
            return Err(QueryError::parse(
                "_similarity argument must be an object with 'vector' key",
            ));
        }
    };

    Ok(Similarity::new(target_field.clone(), vector))
}

/// Parse a vector value from a GraphQL list literal.
fn parse_vector_value(
    value: &Value<'_, String>,
    _variables: Option<&HashMap<String, JsonValue>>,
) -> Result<Vec<f64>> {
    match value {
        Value::List(items) => {
            let mut vec = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Int(n) => {
                        vec.push(
                            n.as_i64()
                                .ok_or_else(|| QueryError::parse("integer out of range"))?
                                as f64,
                        );
                    }
                    Value::Float(f) => {
                        vec.push(*f);
                    }
                    _ => {
                        return Err(QueryError::parse("vector values must be numeric"));
                    }
                }
            }
            Ok(vec)
        }
        _ => Err(QueryError::parse("vector must be an array")),
    }
}

/// Parse a vector from a JSON array value.
fn parse_json_vector(value: &JsonValue) -> Result<Vec<f64>> {
    match value {
        JsonValue::Array(items) => {
            let mut vec = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    JsonValue::Number(n) => {
                        vec.push(
                            n.as_f64()
                                .ok_or_else(|| QueryError::parse("invalid number in vector"))?,
                        );
                    }
                    _ => {
                        return Err(QueryError::parse("vector values must be numeric"));
                    }
                }
            }
            Ok(vec)
        }
        _ => Err(QueryError::parse("vector must be an array")),
    }
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
    fn test_update_without_target_succeeds() {
        // Go DefraDB allows update without filter or docIDs (meaning update all)
        let query = r#"
            mutation {
                update_Users(input: {name: "Bob"}) {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(
            result.is_ok(),
            "update without target should succeed: {:?}",
            result
        );
        let mutations = result.unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(mutations[0].doc_ids.is_none());
        assert!(mutations[0].filter.is_none());
    }

    #[test]
    fn test_delete_without_target_succeeds() {
        // Go DefraDB allows delete without filter or docIDs (meaning delete all)
        let query = r#"
            mutation {
                delete_Users {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(
            result.is_ok(),
            "delete without target should succeed: {:?}",
            result
        );
        let mutations = result.unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(mutations[0].doc_ids.is_none());
        assert!(mutations[0].filter.is_none());
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
    fn test_parse_upsert_mutation_go_style() {
        // Go DefraDB upsert syntax: filter, create, update (all required)
        let query = r#"
            mutation {
                upsert_Users(
                    filter: {name: {_eq: "Bob"}},
                    create: {name: "Bob", age: 40},
                    update: {age: 40}
                ) {
                    _docID
                    name
                    age
                }
            }
        "#;

        let mutations = parse_mutations(query).unwrap();
        assert_eq!(mutations.len(), 1);

        let m = &mutations[0];
        assert_eq!(m.mutation_type, MutationType::Upsert);
        assert_eq!(m.collection_name, "Users");
        assert!(m.filter.is_some());
        // create_input is stored as a single-element vec
        assert_eq!(m.create_input.len(), 1);
        assert_eq!(
            m.create_input[0].get("name"),
            Some(&JsonValue::String("Bob".to_string()))
        );
        // update_input is the fields to update
        assert_eq!(
            m.update_input.get("age"),
            Some(&JsonValue::Number(40.into()))
        );
    }

    #[test]
    fn test_upsert_missing_filter_error() {
        // Go style requires filter
        let query = r#"
            mutation {
                upsert_Users(
                    create: {name: "Bob", age: 40},
                    update: {age: 40}
                ) {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("filter"));
    }

    #[test]
    fn test_upsert_missing_create_error() {
        // Go style requires create
        let query = r#"
            mutation {
                upsert_Users(
                    filter: {name: {_eq: "Bob"}},
                    update: {age: 40}
                ) {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("create"));
    }

    #[test]
    fn test_upsert_missing_update_error() {
        // Go style requires update
        let query = r#"
            mutation {
                upsert_Users(
                    filter: {name: {_eq: "Bob"}},
                    create: {name: "Bob", age: 40}
                ) {
                    _docID
                }
            }
        "#;

        let result = parse_mutations(query);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("update"));
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

        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
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

        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
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

        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
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

        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
        match result {
            ParsedOperation::Mutation { mutations, .. } => {
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

        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
        match result {
            ParsedOperation::Mutation { mutations, .. } => {
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
        let result = parse_request_with_variables(query, Some(&variables), None);
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

        let result = parse_request_with_variables(query, None, None);
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
        let result = parse_request_with_variables(query, None, None).unwrap();
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
        let result = parse_request_with_variables(query, Some(&variables), None);
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

        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
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
        let result = parse_request_with_variables(query, Some(&variables), None);
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
        let result = parse_request_with_variables(query, Some(&variables), None);
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
        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
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
        let result = parse_request_with_variables(query, Some(&variables), None);
        assert!(result.is_err());
        // Error format matches Go DefraDB
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Argument \"order\" has invalid value"));
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
        let result = parse_request_with_variables(query, None, None).unwrap();
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
        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
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
        let result = parse_request_with_variables(query, None, None).unwrap();
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
        let result = parse_request_with_variables(query, None, None).unwrap();
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
        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
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
        let result = parse_request_with_variables(query, None, None).unwrap();
        match result {
            ParsedOperation::Mutation { mutations, .. } => {
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
        let result = parse_request_with_variables(query, None, None).unwrap();
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
        let result = parse_request_with_variables(query, None, None).unwrap();
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

        let result = parse_request_with_variables(query, None, None);
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
        let result = parse_request_with_variables(query, Some(&variables), None).unwrap();
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
        let result = parse_request_with_variables(query, None, None).unwrap();
        match result {
            ParsedOperation::Subscription { select } => {
                assert_eq!(select.limit.as_ref().unwrap().limit, Some(10));
            }
            _ => panic!("Expected subscription"),
        }
    }
}
