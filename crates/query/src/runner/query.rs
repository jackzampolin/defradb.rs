//! Query execution methods for QueryRunner.

use acp::{DocumentPermission, Identity};
use identity::Did;
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, instrument, warn};

use crate::document::{documents_to_plan_docs, documents_with_status_to_plan_docs};
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::planner::index_selection::{
    can_be_ordered_by_index, filter_to_index_scan, select_best_index,
};
use crate::planner::Planner;
use crate::query_parse::parse_query_with_variables;
use crate::txn::TransactionRegistry;

use super::fetcher::FetcherWrapper;
use super::plan;
use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a GraphQL query and return JSON results.
    pub async fn execute_query(&self, query: &str) -> Result<JsonValue> {
        self.execute_query_internal(query, self.fetcher.as_ref(), None)
            .await
    }

    /// Execute a GraphQL query with identity for ACP permission checks.
    pub async fn execute_query_with_identity(
        &self,
        query: &str,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        self.execute_query_internal(query, self.fetcher.as_ref(), caller_identity)
            .await
    }

    /// Execute a GraphQL query with identity and variables.
    pub async fn execute_query_with_identity_and_vars(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        self.execute_query_internal_with_vars(
            query,
            self.fetcher.as_ref(),
            caller_identity,
            variables,
        )
        .await
    }

    /// Execute a GraphQL query with a specific fetcher and identity.
    ///
    /// This is used internally for both regular queries (using the default fetcher)
    /// and transactional queries (using a transaction-scoped fetcher).
    pub(crate) async fn execute_query_internal(
        &self,
        query: &str,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        self.execute_query_internal_with_vars(query, fetcher, caller_identity, None)
            .await
    }

    /// Execute a GraphQL query with a specific fetcher, identity, and variables.
    pub(crate) async fn execute_query_internal_with_vars(
        &self,
        query: &str,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        let selects = parse_query_with_variables(query, variables)?;

        let mut results = Map::new();

        for select in selects {
            let result = self
                .execute_select_internal(&select, fetcher, caller_identity.clone())
                .await?;
            let key = select.field.output_name();
            results.insert(key.to_string(), result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute already-parsed Select operations with a specific fetcher and identity.
    #[instrument(
        name = "query.execute",
        skip(self, selects, fetcher, caller_identity),
        fields(select_count = selects.len())
    )]
    pub(crate) async fn execute_selects_internal(
        &self,
        selects: Vec<Select>,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        let mut results = Map::new();

        for select in selects {
            let result = self
                .execute_select_internal(&select, fetcher, caller_identity.clone())
                .await?;
            let key = select.field.output_name();
            results.insert(key.to_string(), result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute a single Select operation with a specific fetcher and identity.
    async fn execute_select_internal(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Handle encrypted search queries (encrypted_<Collection>)
        if select.is_encrypted {
            return self.execute_encrypted_select(select, fetcher).await;
        }

        // Handle _commits system collection specially
        if select.collection_name == "_commits" {
            return self.execute_commits_query(select).await;
        }

        // Check if _version is selected - it needs special handling since it's commit data
        // not a schema field. Extract the _version Select for later use.
        let version_selection: Option<&Select> = select.fields.iter().find_map(|f| {
            if let Requestable::Select(s) = f {
                if s.field.name == "_version" {
                    return Some(s.as_ref());
                }
            }
            None
        });

        // Handle CID-based time-travel queries
        if select.cid.is_some() {
            return self
                .execute_cid_query_with_version(select, fetcher, caller_identity, version_selection)
                .await;
        }

        // For queries with _version, execute documents first then add version data
        if version_selection.is_some() {
            return self
                .execute_query_with_version(select, fetcher, caller_identity, version_selection)
                .await;
        }

        // Get collection schema on-demand from provider
        let collection = self.get_collection(&select.collection_name).await?;

        // Embedded-only types (interface types from view SDL) are not root-queryable
        if collection.is_embedded_only {
            return Err(QueryError::parse(format!(
                "Cannot query field \"{}\" on type \"Query\".",
                select.collection_name
            )));
        }

        // Validate unsupported features and field references
        plan::validate_select(select, &collection)?;

        // Check if this query has nested selections (relations)
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        // Check if the filter references relation fields (e.g., {author: {verified: true}})
        // If so, we need to use the Planner to join the relation for filtering even if
        // the relation field is not in the selection set.
        let filter_has_relations = select
            .filter
            .as_ref()
            .map(|f| f.has_relation_filters())
            .unwrap_or(false);

        // Check if the order references relation fields (e.g., {author: {age: DESC}})
        // If so, we need to use the Planner to join the relation for ordering.
        let order_has_relations = select
            .order_by
            .as_ref()
            .map(|o| o.has_relation_order())
            .unwrap_or(false);

        // Check if this is a top-level aggregate query (e.g., { _avg(Users: {field: Age}) })
        // Top-level aggregates have: only aggregate fields, and all targets have host_name == collection_name
        let is_top_level_aggregate = !select.fields.is_empty()
            && select
                .fields
                .iter()
                .all(|f| matches!(f, Requestable::Aggregate(_)))
            && select.fields.iter().all(|f| {
                if let Requestable::Aggregate(agg) = f {
                    agg.targets
                        .iter()
                        .all(|t| t.host_name == select.collection_name)
                } else {
                    true
                }
            });

        // Check if any aggregates reference relations (e.g., _count(books: {}))
        // Relation-based aggregates have a non-empty host_name that differs from collection_name.
        // Exclude top-level aggregates where host_name == collection_name.
        let aggregates_have_relations = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets
                    .iter()
                    .any(|t| !t.host_name.is_empty() && t.host_name != select.collection_name)
            } else {
                false
            }
        });

        // Check if any aggregate targets have filters with relation conditions.
        // If so, we need the planner to join relation data before filtering.
        let aggregate_filter_has_relations = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets.iter().any(|t| {
                    t.filter
                        .as_ref()
                        .map(|f| f.has_relation_filters())
                        .unwrap_or(false)
                })
            } else {
                false
            }
        });

        // Handle top-level aggregates specially - return single value, not array
        if is_top_level_aggregate {
            if aggregate_filter_has_relations {
                // Use planner path to join relation data before filtering
                return self
                    .execute_top_level_aggregate_with_planner(select, fetcher, caller_identity)
                    .await;
            } else {
                return self
                    .execute_top_level_aggregate(select, fetcher, &collection, caller_identity)
                    .await;
            }
        }

        // Check if any secondary relation ID fields are selected (e.g., `_authorID` for a secondary `author` relation).
        // These require a TypeJoin to compute the ID via reverse lookup.
        let has_secondary_relation_id = select.fields.iter().any(|f| {
            if let Requestable::Field(field) = f {
                let field_name = &field.name;
                // Check pattern: _<relationName>ID
                if field_name.starts_with('_') && field_name.ends_with("ID") && field_name.len() > 3
                {
                    let relation_name = &field_name[1..field_name.len() - 2];
                    if let Some(relation_field) = collection.field_by_name(relation_name) {
                        // Only secondary relations need a join to compute the ID
                        return relation_field.kind.is_relation() && !relation_field.is_primary;
                    }
                }
            }
            false
        });

        // Check if an ordering-only index can be used (planner needed for IndexScanNode)
        let has_ordering_index = select.order_by.is_some()
            && fetcher.supports_index_queries()
            && !collection.indexes.is_empty()
            && select
                .order_by
                .as_ref()
                .map(|o| {
                    collection
                        .indexes
                        .iter()
                        .any(|idx| can_be_ordered_by_index(o, idx).0)
                })
                .unwrap_or(false);

        // Check if any similarity fields are present (require SimilarityNode in planner)
        let has_similarity = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Similarity(_)));

        // Validate similarity fields against the collection schema
        if has_similarity {
            for field in &select.fields {
                if let Requestable::Similarity(sim) = field {
                    let target = &sim.target_field;
                    let schema_field = collection.field_by_name(target);
                    // Check that the target field exists and is a numeric array
                    let element_kind = schema_field.and_then(|f| {
                        if let schema::FieldKind::ScalarArray(arr) = &f.kind {
                            let ek = arr.element_kind();
                            match ek {
                                schema::ScalarKind::Int
                                | schema::ScalarKind::Float32
                                | schema::ScalarKind::Float64 => Some(ek),
                                _ => None,
                            }
                        } else {
                            None
                        }
                    });

                    let element_kind = match element_kind {
                        Some(ek) => ek,
                        None => {
                            return Err(QueryError::execution(format!(
                                "Unknown argument \"{}\" on field \"_similarity\" of type \"{}\".",
                                target, collection.name
                            )));
                        }
                    };

                    // For Int fields, validate that vector values are integers
                    if element_kind == schema::ScalarKind::Int {
                        let non_int_values: Vec<String> = sim
                            .vector
                            .iter()
                            .filter(|v| v.fract() != 0.0)
                            .map(|v| format!("{}", v))
                            .collect();
                        if !non_int_values.is_empty() {
                            let vector_repr = format!(
                                "[{}]",
                                sim.vector
                                    .iter()
                                    .map(|v| format!("{}", v))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            );
                            let mut msg = format!(
                                "Argument \"{}\" has invalid value {{vector: {}}}.",
                                target, vector_repr
                            );
                            for v in &non_int_values {
                                msg.push_str(&format!(
                                    "\nIn field \"vector\": In element #1: Expected type \"Int\", found {}.",
                                    v
                                ));
                            }
                            return Err(QueryError::execution(msg));
                        }
                    }
                }
            }
        }

        // Views must always use the planner because they don't store data directly -
        // the planner creates a ViewNode that executes the underlying query.
        let is_view = collection.query.is_some();

        // Use Planner if there are nested selections, filter through relations,
        // order through relations, aggregates on relations, aggregate filters with relations,
        // secondary relation ID fields, similarity computations, when an index can provide
        // ordering, or when querying a view
        let needs_planner = is_view
            || has_nested
            || filter_has_relations
            || order_has_relations
            || aggregates_have_relations
            || aggregate_filter_has_relations
            || has_secondary_relation_id
            || has_ordering_index
            || has_similarity;

        if needs_planner {
            // Use the Planner for queries with nested selections (joins) or relation filters.
            self.execute_nested_select_with_planner(select, fetcher, caller_identity)
                .await
        } else {
            // Use the optimized path for simple queries
            self.execute_simple_select(select, fetcher, &collection, caller_identity)
                .await
        }
    }

    /// Execute an encrypted search query (`encrypted_<Collection>`).
    ///
    /// Validates encrypted index exists, then fetches documents, applies _eq filter
    /// conditions, and returns Go-compatible `[{"docIDs": [...]}]` format.
    async fn execute_encrypted_select(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
    ) -> Result<JsonValue> {
        // Get collection to check for encrypted indexes
        let collection = self.get_collection(&select.collection_name).await?;

        // Validate collection has encrypted indexes (Go-compatible error)
        if collection.encrypted_indexes.is_empty() {
            return Err(QueryError::internal("collection has no encrypted indexes"));
        }

        // Extract filtered field names and validate they have encrypted indexes
        if let Some(ref filter) = select.filter {
            let filtered_fields = filter.referenced_fields();
            for field_name in &filtered_fields {
                let has_index = collection
                    .encrypted_indexes
                    .iter()
                    .any(|idx| idx.field_name == *field_name);
                if !has_index {
                    return Err(QueryError::internal(format!(
                        "no encrypted index found for field: {}",
                        field_name
                    )));
                }
            }
        }

        let docs = fetcher.get_all(&select.collection_name).await?;

        let matching_ids: Vec<String> = if let Some(ref filter) = select.filter {
            let mut ids = Vec::new();
            for doc in &docs {
                let json_map = doc
                    .to_map()
                    .map_err(|e| QueryError::internal(e.to_string()))?;
                let json_obj =
                    JsonValue::Object(json_map.into_iter().collect::<Map<String, JsonValue>>());
                if filter.matches_json_object(&json_obj)? {
                    if let Some(id) = doc.id() {
                        ids.push(id.to_string());
                    }
                }
            }
            ids
        } else {
            docs.iter()
                .filter_map(|doc| doc.id().map(|id| id.to_string()))
                .collect()
        };

        Ok(serde_json::json!([{"docIDs": matching_ids}]))
    }

    /// Execute a query with nested selections using the Planner.
    ///
    /// The Planner builds a proper join plan with TypeJoinOne/TypeJoinMany nodes.
    /// ScanNodes fetch their own data via the attached fetcher.
    /// ACP permission filtering is applied per-collection via PermissionFilterNode in the plan.
    pub(crate) async fn execute_nested_select_with_planner(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Create a fetcher wrapper that can be shared across plan nodes
        // We need to wrap the reference in an Arc-compatible struct
        let fetcher_arc = FetcherWrapper::new(fetcher);

        // Build the plan using the Planner with fetcher support
        // Get all collections from provider for join planning
        let collections_map = self.collections_map().await?;
        let collections: Vec<CollectionVersion> =
            collections_map.values().map(|c| (**c).clone()).collect();

        let mut planner = Planner::new(collections).with_fetcher(Arc::new(fetcher_arc));
        if let Some(ref acp) = self.acp {
            planner = planner.with_acp(acp.clone(), identity);
        }
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(select)?;
        let mut plan = plan_result.plan;
        let ordering_only_fields = plan_result.ordering_only_fields;
        let aggregate_internal_keys = plan_result.aggregate_internal_keys;

        // Get the mapping from the plan
        let mapping = plan.document_map().clone();

        // Execute the plan and collect results
        plan.init().await?;
        plan.start().await?;

        let mut results = Vec::new();

        while plan.next().await? {
            let doc = plan.value();
            let mut json = self.doc_to_json(doc, &mapping)?;

            // Strip ordering-only fields from nested objects.
            // These fields were added for ORDER BY but shouldn't appear in output.
            for (relation_field, nested_field) in &ordering_only_fields {
                if let Some(obj) = json.as_object_mut() {
                    if let Some(relation_value) = obj.get_mut(relation_field) {
                        if let Some(nested_obj) = relation_value.as_object_mut() {
                            nested_obj.remove(nested_field);
                        }
                    }
                }
            }

            results.push(json);
        }

        plan.close().await?;

        // Post-process relation-based aggregates
        // For aggregates like _count(books: {}), compute the value from joined data
        let results =
            self.compute_relation_aggregates(results, select, &aggregate_internal_keys)?;

        // Strip fields from relation data that were added for filter evaluation
        // but not explicitly requested in the selection set.
        let results = Self::clean_filter_only_relation_fields(results, select);

        // Apply deferred limit/offset to relation fields.
        // TypeJoinMany stores ALL children (for aggregates to count), so we apply
        // the select's limit/offset here after aggregates have been computed.
        let results = Self::apply_relation_limits(results, select);

        Ok(JsonValue::Array(results))
    }

    /// Compute aggregate values from joined relation data.
    ///
    /// For each relation-based aggregate (e.g., _count(books: {})), this function:
    /// 1. Finds the joined relation data (stored under the relation field name)
    /// 2. Computes the aggregate (count, sum, avg, etc.)
    /// 3. Stores the result under the aggregate's output name
    fn compute_relation_aggregates(
        &self,
        mut results: Vec<JsonValue>,
        select: &Select,
        aggregate_internal_keys: &std::collections::HashMap<String, (String, String)>,
    ) -> Result<Vec<JsonValue>> {
        use crate::mapper::AggregateType;

        // Collect info about relation aggregates with full target references
        let mut aggregates_info: Vec<(
            String,
            AggregateType,
            Vec<&crate::mapper::AggregateTarget>,
        )> = Vec::new();

        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                let mut relation_targets = Vec::new();
                for target in &agg.targets {
                    // Skip _group targets - they're handled by GroupByNode and aggregate nodes
                    if !target.host_name.is_empty() && target.host_name != "_group" {
                        relation_targets.push(target);
                    }
                }
                if !relation_targets.is_empty() {
                    aggregates_info.push((
                        agg.output_name().to_string(),
                        agg.aggregate_type,
                        relation_targets,
                    ));
                }
            }
        }

        if aggregates_info.is_empty() {
            return Ok(results);
        }

        // Collect which relation fields are explicitly selected and their requested fields (for cleanup later)
        let _selected_relations: std::collections::HashSet<String> = select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    Some(s.field.name.clone())
                } else {
                    None
                }
            })
            .collect();

        // For each selected relation, collect the fields that were explicitly requested.
        // Any fields NOT in this set were added for aggregate filter evaluation and should be cleaned up.
        let selected_relation_fields: std::collections::HashMap<
            String,
            std::collections::HashSet<String>,
        > = select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    let mut fields = std::collections::HashSet::new();
                    // Always include _docID as it's implicit
                    fields.insert("_docID".to_string());
                    for requestable in &s.fields {
                        match requestable {
                            Requestable::Field(f) => {
                                fields.insert(f.output_name().to_string());
                            }
                            Requestable::Select(nested) => {
                                fields.insert(nested.field.output_name().to_string());
                            }
                            Requestable::Aggregate(agg) => {
                                fields.insert(agg.output_name().to_string());
                            }
                            Requestable::Similarity(sim) => {
                                fields.insert(sim.output_name().to_string());
                            }
                        }
                    }
                    Some((s.field.output_name().to_string(), fields))
                } else {
                    None
                }
            })
            .collect();

        // Collect all relation names used by aggregates (for deferred cleanup)
        let mut aggregate_relation_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for (_, _, targets) in &aggregates_info {
            for target in targets {
                aggregate_relation_names.insert(target.host_name.clone());
            }
        }

        // Build a mapping from relation field name → output name for aliased relation selections.
        // When a query uses `NewestPublishersBook: book(...)`, the JSON key is "NewestPublishersBook"
        // but the aggregate target references "book". We need to resolve these aliases.
        let relation_alias_map: std::collections::HashMap<&str, &str> = select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    let name = s.field.name.as_str();
                    let output = s.field.output_name();
                    if name != output {
                        Some((name, output))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // Process each result
        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (output_name, agg_type, targets) in &aggregates_info {
                    let mut total_value: f64 = 0.0;
                    let mut total_count: i64 = 0;

                    for target in targets {
                        let relation_name = &target.host_name;
                        let field_name = target.field_name.as_deref();

                        // Try internal key first (when selection and aggregate use same relation),
                        // then direct relation name, then fall back to alias
                        let relation_data = aggregate_internal_keys
                            .get(output_name)
                            .and_then(|(_, internal_key)| obj.get(internal_key.as_str()))
                            .or_else(|| obj.get(relation_name.as_str()))
                            .or_else(|| {
                                relation_alias_map
                                    .get(relation_name.as_str())
                                    .and_then(|alias| obj.get(*alias))
                            });
                        if let Some(relation_data) = relation_data {
                            if let JsonValue::Array(items) = relation_data {
                                // Array data: relation or inline array aggregate
                                // Step 1: Apply filter to array elements
                                let filtered_items: Vec<&JsonValue> = if let Some(ref filter) =
                                    target.filter
                                {
                                    // Check if the filter has field-based conditions
                                    // (keys that are not operators like _gt, _eq, etc.)
                                    let has_field_conditions = filter.has_field_conditions();

                                    items
                                        .iter()
                                        .filter(|item| {
                                            if has_field_conditions {
                                                // Field-based filter like {rating: {_gt: 4.8}}
                                                // Match against the entire item object
                                                filter.matches_json_object(item).unwrap_or(false)
                                            } else {
                                                // Operator-only filter like {_gt: 4.8}
                                                // Extract the field value and match against it
                                                let val = match field_name {
                                                    Some(f) => item
                                                        .as_object()
                                                        .and_then(|o| o.get(f))
                                                        .unwrap_or(&JsonValue::Null),
                                                    None => *item,
                                                };
                                                filter.matches_scalar_value(val).unwrap_or(false)
                                            }
                                        })
                                        .collect()
                                } else {
                                    items.iter().collect()
                                };

                                // Step 2: Apply order (sort array elements before limit/offset)
                                // The order field may differ from the aggregate field (e.g., order by "name", sum "rating")
                                // Supports nested paths (e.g., order: {publisher: {yearOpened: ASC}})
                                let mut ordered_items = filtered_items;
                                if let Some(ref order) = target.order {
                                    if let Some(condition) = order.conditions.first() {
                                        let fields = &condition.fields;
                                        let desc = matches!(
                                            condition.direction,
                                            crate::mapper::OrderDirection::Desc
                                        );
                                        ordered_items.sort_by(|a, b| {
                                            let resolve_value =
                                                |item: &&JsonValue| -> Option<JsonValue> {
                                                    // For scalar inline arrays, order: ASC/DESC has no field path
                                                    // (fields is either empty or contains a single empty string)
                                                    if fields.is_empty()
                                                        || (fields.len() == 1
                                                            && fields[0].is_empty())
                                                    {
                                                        return Some((*item).clone());
                                                    }
                                                    // Start with the first field
                                                    let first = &fields[0];
                                                    let mut current = item
                                                        .as_object()
                                                        .and_then(|o| o.get(first.as_str()))
                                                        .cloned()?;
                                                    // Resolve remaining nested fields
                                                    for key in &fields[1..] {
                                                        current = match current {
                                                            JsonValue::Object(ref obj) => {
                                                                obj.get(key.as_str())?.clone()
                                                            }
                                                            _ => return None,
                                                        };
                                                    }
                                                    Some(current)
                                                };
                                            let a_val = resolve_value(a);
                                            let b_val = resolve_value(b);
                                            let cmp = crate::plan::compare_json_values(
                                                a_val.as_ref(),
                                                b_val.as_ref(),
                                            );
                                            if desc {
                                                cmp.reverse()
                                            } else {
                                                cmp
                                            }
                                        });
                                    }
                                }

                                // Step 3: Apply limit/offset
                                let final_items: Vec<&JsonValue> =
                                    if let Some(ref limit) = target.limit {
                                        let offset = limit.offset as usize;
                                        let sliced = if offset < ordered_items.len() {
                                            &ordered_items[offset..]
                                        } else {
                                            &[][..]
                                        };
                                        match limit.limit {
                                            Some(l) => {
                                                sliced.iter().take(l as usize).copied().collect()
                                            }
                                            None => sliced.to_vec(),
                                        }
                                    } else {
                                        ordered_items
                                    };

                                // Step 4: Compute aggregate over final items
                                match agg_type {
                                    AggregateType::Count => {
                                        total_count += final_items.len() as i64;
                                    }
                                    AggregateType::Sum | AggregateType::Average => {
                                        for item in &final_items {
                                            if let Some(n) = extract_numeric(item, field_name) {
                                                total_value += n;
                                                total_count += 1;
                                            }
                                        }
                                    }
                                    AggregateType::Min => {
                                        for item in &final_items {
                                            if let Some(n) = extract_numeric(item, field_name) {
                                                if total_count == 0 || n < total_value {
                                                    total_value = n;
                                                }
                                                total_count += 1;
                                            }
                                        }
                                    }
                                    AggregateType::Max => {
                                        for item in &final_items {
                                            if let Some(n) = extract_numeric(item, field_name) {
                                                if total_count == 0 || n > total_value {
                                                    total_value = n;
                                                }
                                                total_count += 1;
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Scalar data: multi-field per-document aggregate
                                // e.g., _avg(HeightM: {}, Age: {}) where HeightM is a scalar
                                if let Some(n) = relation_data.as_f64() {
                                    match agg_type {
                                        AggregateType::Count => {
                                            total_count += 1;
                                        }
                                        AggregateType::Sum | AggregateType::Average => {
                                            total_value += n;
                                            total_count += 1;
                                        }
                                        AggregateType::Min => {
                                            if total_count == 0 || n < total_value {
                                                total_value = n;
                                            }
                                            total_count += 1;
                                        }
                                        AggregateType::Max => {
                                            if total_count == 0 || n > total_value {
                                                total_value = n;
                                            }
                                            total_count += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Store the computed aggregate value
                    let computed_value = match agg_type {
                        AggregateType::Count => JsonValue::Number(total_count.into()),
                        AggregateType::Sum => {
                            if total_value == total_value.floor()
                                && total_value.abs() < i64::MAX as f64
                            {
                                JsonValue::Number((total_value as i64).into())
                            } else {
                                JsonValue::Number(
                                    serde_json::Number::from_f64(total_value)
                                        .unwrap_or_else(|| 0.into()),
                                )
                            }
                        }
                        AggregateType::Average => {
                            if total_count > 0 {
                                let avg = total_value / total_count as f64;
                                if avg == avg.floor() && avg.abs() < i64::MAX as f64 {
                                    JsonValue::Number((avg as i64).into())
                                } else {
                                    JsonValue::Number(
                                        serde_json::Number::from_f64(avg)
                                            .unwrap_or_else(|| 0.into()),
                                    )
                                }
                            } else {
                                // Go DefraDB returns 0 for average of empty/null arrays
                                JsonValue::Number(0.into())
                            }
                        }
                        AggregateType::Min | AggregateType::Max => {
                            if total_count > 0 {
                                if total_value == total_value.floor()
                                    && total_value.abs() < i64::MAX as f64
                                {
                                    JsonValue::Number((total_value as i64).into())
                                } else {
                                    JsonValue::Number(
                                        serde_json::Number::from_f64(total_value)
                                            .unwrap_or_else(|| 0.into()),
                                    )
                                }
                            } else {
                                JsonValue::Null
                            }
                        }
                    };

                    obj.insert(output_name.clone(), computed_value);
                }

                // Deferred cleanup: remove relation data only used for aggregation.
                // When a selection uses an alias (e.g., `books2020: book(...)`), the
                // aggregate's raw relation data ("book") must also be removed since
                // the display data is at the alias key ("books2020").
                for relation_name in &aggregate_relation_names {
                    let selected_with_same_key = select.fields.iter().any(|f| {
                        if let Requestable::Select(s) = f {
                            s.field.name == *relation_name && s.field.output_name() == relation_name
                        } else {
                            false
                        }
                    });
                    if !selected_with_same_key {
                        obj.remove(relation_name.as_str());
                    }
                }

                // Clean up extra fields from relation data that were added for aggregate filter evaluation
                // but weren't in the original selection. For example, if the selection was
                // `published { name }` but the aggregate filter needed `rating`, we need to remove
                // `rating` from each item in `published` after aggregate computation.
                for (relation_name, allowed_fields) in &selected_relation_fields {
                    if let Some(JsonValue::Array(items)) = obj.get_mut(relation_name) {
                        for item in items.iter_mut() {
                            if let JsonValue::Object(item_obj) = item {
                                // Remove fields that weren't in the original selection
                                item_obj.retain(|k, _| allowed_fields.contains(k));
                            }
                        }
                    }
                }
            }
        }

        // Apply post-aggregate filtering if needed
        // When filter uses _alias to reference computed aggregates, the SelectNode can't
        // filter during plan execution since aggregate values don't exist yet.
        // Example: filter: {_alias: {publishedCount: {_gt: 0}}}
        if let Some(ref filter) = select.filter {
            let aggregate_output_names: std::collections::HashSet<&str> = aggregates_info
                .iter()
                .map(|(name, _, _)| name.as_str())
                .collect();

            // Check if filter has _alias conditions referencing aggregate names
            if let Some(alias_conditions) = filter.conditions().get("_alias") {
                if let Some(alias_obj) = alias_conditions.as_object() {
                    let needs_post_filter = alias_obj
                        .keys()
                        .any(|k| aggregate_output_names.contains(k.as_str()));

                    if needs_post_filter {
                        results.retain(|result| {
                            if let Some(obj) = result.as_object() {
                                // Evaluate each alias condition
                                for (alias_name, condition) in alias_obj {
                                    if let Some(value) = obj.get(alias_name) {
                                        // Parse and evaluate the operator conditions
                                        if let Some(cond_obj) = condition.as_object() {
                                            for (op_str, expected) in cond_obj {
                                                if let Some(op) =
                                                    crate::mapper::FilterOp::parse(op_str)
                                                {
                                                    match op {
                                                        crate::mapper::FilterOp::Eq => {
                                                            if value != expected {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Ne => {
                                                            if value == expected {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Gt => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v <= e {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Gte => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v < e {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Lt => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v >= e {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Lte => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v > e {
                                                                return false;
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        // Alias field not found in result, filter it out
                                        return false;
                                    }
                                }
                            }
                            true
                        });
                    }
                }
            }
        }

        // Apply post-aggregate ordering if needed
        // When order references aggregate aliases (e.g., order: {_alias: {total: DESC}}),
        // the OrderByNode can't sort during plan execution since values don't exist yet.
        if let Some(ref order_by) = select.order_by {
            let aggregate_output_names: std::collections::HashSet<&str> = aggregates_info
                .iter()
                .map(|(name, _, _)| name.as_str())
                .collect();

            let needs_post_sort = order_by.conditions.iter().any(|c| {
                c.fields.len() == 1 && aggregate_output_names.contains(c.fields[0].as_str())
            });

            if needs_post_sort {
                results.sort_by(|a, b| {
                    for condition in &order_by.conditions {
                        if condition.fields.len() != 1 {
                            continue;
                        }
                        let field = &condition.fields[0];
                        let a_val = a.as_object().and_then(|o| o.get(field));
                        let b_val = b.as_object().and_then(|o| o.get(field));
                        let a_f = a_val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let b_f = b_val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let ord = a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal);
                        let ord =
                            if matches!(condition.direction, crate::mapper::OrderDirection::Desc) {
                                ord.reverse()
                            } else {
                                ord
                            };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        // Clean up internal aggregate keys from output (keys like "__agg_published__count")
        // These are only used for looking up relation data when there's a collision with
        // a relation selection.
        if !aggregate_internal_keys.is_empty() {
            for result in &mut results {
                if let JsonValue::Object(ref mut obj) = result {
                    obj.retain(|k, _| !k.starts_with("__agg_"));
                }
            }
        }

        Ok(results)
    }

    /// Strip filter-only fields from relation data in query results.
    ///
    /// When the planner adds relation joins for filter evaluation (e.g., filtering
    /// Author by book.publisher.yearOpened), those relations get render_keys so
    /// the filter can evaluate on rendered JSON. This causes the relation field to
    /// appear in output even though the user didn't request it. This function
    /// retains only the fields explicitly listed in each nested Select.
    fn clean_filter_only_relation_fields(
        mut results: Vec<JsonValue>,
        select: &Select,
    ) -> Vec<JsonValue> {
        // Build map of relation output_name → allowed sub-field names
        let mut relation_allowed_fields: Vec<(String, HashSet<String>)> = Vec::new();

        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                if nested_select.field.name == "_group" {
                    continue;
                }
                let mut allowed = HashSet::new();
                // _docID is always implicit
                allowed.insert("_docID".to_string());
                for sub_field in &nested_select.fields {
                    match sub_field {
                        Requestable::Field(f) => {
                            allowed.insert(f.output_name().to_string());
                        }
                        Requestable::Select(s) => {
                            allowed.insert(s.field.output_name().to_string());
                        }
                        Requestable::Aggregate(a) => {
                            allowed.insert(a.output_name().to_string());
                        }
                        Requestable::Similarity(s) => {
                            allowed.insert(s.output_name().to_string());
                        }
                    }
                }
                relation_allowed_fields
                    .push((nested_select.field.output_name().to_string(), allowed));
            }
        }

        if relation_allowed_fields.is_empty() {
            return results;
        }

        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (relation_name, allowed_fields) in &relation_allowed_fields {
                    if let Some(relation_data) = obj.get_mut(relation_name.as_str()) {
                        match relation_data {
                            JsonValue::Array(items) => {
                                for item in items.iter_mut() {
                                    if let JsonValue::Object(item_obj) = item {
                                        item_obj.retain(|k, _| allowed_fields.contains(k));
                                    }
                                }
                            }
                            JsonValue::Object(item_obj) => {
                                item_obj.retain(|k, _| allowed_fields.contains(k));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        results
    }

    /// Apply deferred limit/offset to relation fields in query results.
    ///
    /// TypeJoinMany stores ALL children so that relation aggregates (e.g., _count)
    /// can see the full set. This function applies the limit/offset from the select's
    /// nested relation fields after aggregates have been computed.
    fn apply_relation_limits(mut results: Vec<JsonValue>, select: &Select) -> Vec<JsonValue> {
        // Collect relation fields with limits
        let mut relation_limits: Vec<(String, u64, u64)> = Vec::new(); // (field_name, limit, offset)
        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                if nested_select.field.name == "_group" {
                    continue; // _group is handled by GroupByNode
                }
                if let Some(ref limit) = nested_select.limit {
                    let limit_val = limit.limit.unwrap_or(0); // 0 means no limit
                    let offset_val = limit.offset;
                    if limit_val > 0 || offset_val > 0 {
                        relation_limits.push((
                            nested_select.field.output_name().to_string(),
                            limit_val,
                            offset_val,
                        ));
                    }
                }
            }
        }

        if relation_limits.is_empty() {
            return results;
        }

        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (field_name, limit, offset) in &relation_limits {
                    if let Some(JsonValue::Array(items)) = obj.get_mut(field_name) {
                        let offset = *offset as usize;
                        let total = items.len();
                        if offset >= total {
                            *items = Vec::new();
                        } else {
                            let remaining: Vec<JsonValue> = items.drain(offset..).collect();
                            *items = if *limit > 0 {
                                remaining.into_iter().take(*limit as usize).collect()
                            } else {
                                remaining
                            };
                        }
                    }
                }
            }
        }

        results
    }

    /// Execute a simple query without nested selections.
    ///
    /// This is the optimized path that supports aggregations and grouping.
    pub(crate) async fn execute_simple_select(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        collection: &Arc<CollectionVersion>,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Build document mapping first (needed for both paths)
        let mapping = plan::build_mapping(select, collection)?;

        // When show_deleted is true, we need to use get_all_with_deleted to include
        // logically deleted documents. The doc_ids filter will be applied by the plan.
        let plan_docs = if select.show_deleted {
            let docs_with_status = fetcher
                .get_all_with_deleted(&select.collection_name, true)
                .await?;
            documents_with_status_to_plan_docs(&docs_with_status, &mapping)?
        } else {
            // Fetch documents from storage
            let docs = if let Some(ref doc_ids) = select.doc_ids {
                // Deduplicate doc_ids while preserving order (Go compatibility)
                let mut seen = HashSet::new();
                let unique_ids: Vec<String> = doc_ids
                    .iter()
                    .filter(|id| seen.insert((*id).clone()))
                    .cloned()
                    .collect();
                let result = fetcher
                    .get_by_ids(&select.collection_name, &unique_ids)
                    .await?;
                let missing = result.missing_ids();
                if !missing.is_empty() {
                    warn!(
                        collection = %select.collection_name,
                        missing_ids = ?missing,
                        requested_count = unique_ids.len(),
                        found_count = result.docs().len(),
                        "Some requested documents were not found"
                    );
                }
                result.into_docs()
            } else if let Some(ref filter) = select.filter {
                // Try to use an index if available
                if fetcher.supports_index_queries() && !collection.indexes.is_empty() {
                    if let Some(best_index) = select_best_index(filter, &collection.indexes) {
                        // Extract limit/offset for index optimization
                        let limit = select.limit.as_ref().and_then(|l| l.limit);
                        let offset = select.limit.as_ref().map(|l| l.offset).unwrap_or(0);
                        if let Some(params) = filter_to_index_scan(
                            filter,
                            best_index,
                            select.order_by.as_ref(),
                            &collection.fields,
                            limit,
                            offset,
                        ) {
                            debug!(
                                collection = %select.collection_name,
                                index = %params.index_name,
                                "Using index for query"
                            );
                            // Get doc IDs from index
                            let scan_result = fetcher
                                .get_by_index_scan(&select.collection_name, &params)
                                .await?;
                            // Fetch the actual documents by ID
                            let result = fetcher
                                .get_by_ids(&select.collection_name, scan_result.doc_ids())
                                .await?;
                            result.into_docs()
                        } else {
                            // Filter doesn't translate to index scan, fallback to full scan
                            fetcher.get_all(&select.collection_name).await?
                        }
                    } else {
                        // No suitable index found, fallback to full scan
                        fetcher.get_all(&select.collection_name).await?
                    }
                } else {
                    // Fetcher doesn't support index queries or no indexes, fallback to full scan
                    fetcher.get_all(&select.collection_name).await?
                }
            } else {
                fetcher.get_all(&select.collection_name).await?
            };
            documents_to_plan_docs(&docs, &mapping)?
        };

        // Build ACP filter config if collection has policy and ACP is configured
        let acp_filter = if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy)
        {
            Some(plan::AcpFilter {
                acp: acp.clone(),
                identity: Identity::from(identity),
                policy_id: policy.id.clone(),
                resource_name: policy.resource_name.clone(),
            })
        } else {
            if collection.policy.is_some() && self.acp.is_none() {
                tracing::warn!(
                    collection = %collection.name,
                    "Collection has ACP policy but QueryRunner has no ACP configured - ACP enforcement is DISABLED"
                );
            }
            None
        };

        // Build and execute the plan (ACP filter is inserted inside, after Select but before aggregates)
        let mut plan =
            plan::build_plan(select, plan_docs, mapping.clone(), collection, acp_filter)?;

        // Execute the plan and collect results
        plan.init().await?;
        plan.start().await?;

        let mut results = Vec::new();

        while plan.next().await? {
            let doc = plan.value();
            let json = self.doc_to_json(doc, &mapping)?;
            results.push(json);
        }

        plan.close().await?;

        Ok(JsonValue::Array(results))
    }

    /// Top-level aggregates compute a single value over all documents in a collection.
    /// Unlike regular collection queries that return an array, top-level aggregates
    /// return a single value (the computed aggregate).
    ///
    /// Returns 0 for empty collections (Go DefraDB semantics).
    async fn execute_top_level_aggregate(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        collection: &Arc<CollectionVersion>,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Fetch all documents from the collection
        let docs = fetcher.get_all(&select.collection_name).await?;

        // Build document mapping for field access
        let mapping = plan::build_mapping(select, collection)?;

        // Convert storage documents to values for aggregation
        let mut plan_docs = documents_to_plan_docs(&docs, &mapping)?;

        // Apply ACP filtering if collection has a policy and ACP is configured
        if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
            let acp_identity = Identity::from(identity);
            let mut filtered = Vec::with_capacity(plan_docs.len());
            for doc in plan_docs {
                if let Some(doc_id_val) = doc.get(0) {
                    if let Some(doc_id) = doc_id_val.as_str() {
                        let has_access = acp
                            .check_doc_access(
                                &acp_identity,
                                DocumentPermission::Read,
                                &policy.id,
                                &policy.resource_name,
                                doc_id,
                            )
                            .await
                            .unwrap_or(false);
                        if has_access {
                            filtered.push(doc);
                        }
                    }
                }
            }
            plan_docs = filtered;
        }

        // For each aggregate in the select, compute its value
        // For top-level aggregates, we return a single object with aggregate results
        let mut result = serde_json::Map::new();

        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                let output_name = agg.output_name().to_string();

                // Get the target info
                let target = agg.targets.first();
                let field_name = target.and_then(|t| t.field_name.as_ref());
                let target_filter = target.and_then(|t| t.filter.as_ref());

                // Find field index in mapping
                let field_index = field_name.and_then(|name| mapping.first_index_of_name(name));

                // Apply filter if present
                let filtered_docs: Vec<&crate::planner::Doc> = if let Some(filter) = target_filter {
                    plan_docs
                        .iter()
                        .filter(|doc| {
                            // Convert Doc to fields Vec for filter evaluation
                            let fields: Vec<Option<JsonValue>> = (0..mapping.next_index())
                                .map(|i| doc.get(i).cloned())
                                .collect();
                            filter.matches(&fields, &mapping).unwrap_or(false)
                        })
                        .collect()
                } else {
                    plan_docs.iter().collect()
                };

                // Compute the aggregate value
                let value = match agg.aggregate_type {
                    crate::mapper::AggregateType::Count => {
                        // Count documents (optionally filtered)
                        let count = filtered_docs.len() as i64;
                        JsonValue::Number(count.into())
                    }
                    crate::mapper::AggregateType::Sum => {
                        if let Some(idx) = field_index {
                            let sum: f64 = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .sum();
                            if sum == sum.floor() {
                                JsonValue::Number((sum as i64).into())
                            } else {
                                JsonValue::Number(
                                    serde_json::Number::from_f64(sum).unwrap_or_else(|| 0.into()),
                                )
                            }
                        } else {
                            JsonValue::Number(0.into())
                        }
                    }
                    crate::mapper::AggregateType::Average => {
                        if let Some(idx) = field_index {
                            let values: Vec<f64> = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .collect();
                            if values.is_empty() {
                                // Go DefraDB returns 0 for AVG of empty set
                                JsonValue::Number(0.into())
                            } else {
                                let avg = values.iter().sum::<f64>() / values.len() as f64;
                                JsonValue::Number(
                                    serde_json::Number::from_f64(avg).unwrap_or_else(|| 0.into()),
                                )
                            }
                        } else {
                            JsonValue::Number(0.into())
                        }
                    }
                    crate::mapper::AggregateType::Min => {
                        if let Some(idx) = field_index {
                            let min: Option<f64> = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .min_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                });
                            match min {
                                Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                                Some(v) => JsonValue::Number(
                                    serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                                ),
                                None => JsonValue::Null,
                            }
                        } else {
                            JsonValue::Null
                        }
                    }
                    crate::mapper::AggregateType::Max => {
                        if let Some(idx) = field_index {
                            let max: Option<f64> = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .max_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                });
                            match max {
                                Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                                Some(v) => JsonValue::Number(
                                    serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                                ),
                                None => JsonValue::Null,
                            }
                        } else {
                            JsonValue::Null
                        }
                    }
                };

                result.insert(output_name, value);
            }
        }

        // Return the result object directly (not wrapped in an array)
        // The caller will insert this into the response with the aggregate name as key
        // But for top-level aggregates, we need the caller to extract the value
        // Actually, looking at execute_query_internal, it inserts result with select.field.output_name()
        // For { _avg(...) }, output_name is "_avg", and we're returning {"_avg": value}
        // So we'd get {"_avg": {"_avg": value}} which is wrong.
        // We need to return just the value.

        // For top-level aggregates, return the single aggregate value
        // (assumes single aggregate in top-level query)
        if let Some((_, value)) = result.into_iter().next() {
            Ok(value)
        } else {
            Ok(JsonValue::Null)
        }
    }

    /// Execute a top-level aggregate with filters that reference relations.
    ///
    /// This function uses the planner to build a join plan so relation data is
    /// available for filter evaluation, then counts the filtered results.
    /// Unlike `execute_top_level_aggregate`, this handles filters like:
    /// `_count(Book: {filter: {author: {age: {_gt: 30}}}})`
    async fn execute_top_level_aggregate_with_planner(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        use crate::mapper::AggregateType;

        // Get the aggregate info
        let agg = match select.fields.first() {
            Some(Requestable::Aggregate(a)) => a,
            _ => return Ok(JsonValue::Null),
        };
        let output_name = agg.output_name().to_string();
        let target = agg.targets.first();
        let target_filter = target.and_then(|t| t.filter.as_ref());
        let field_name = target.and_then(|t| t.field_name.as_ref());

        // Create a modified select that fetches the collection with the filter applied.
        // This select returns documents (not aggregates), so the planner will build
        // a proper join plan with the relation filter.
        let collection_name = target
            .map(|t| t.host_name.clone())
            .unwrap_or_else(|| select.collection_name.clone());
        let filter_select = Select {
            collection_name: collection_name.clone(),
            field: crate::mapper::Field::new(collection_name.clone()),
            fields: if let Some(fname) = field_name {
                // For sum/avg/etc., we need the field value
                vec![Requestable::Field(crate::mapper::Field::new(fname.clone()))]
            } else {
                // For count, we just need any field to count docs
                vec![Requestable::Field(crate::mapper::Field::new(
                    "_docID".to_string(),
                ))]
            },
            filter: target_filter.cloned(),
            order_by: None,
            limit: None,
            group_by: None,
            doc_ids: None,
            cid: None,
            depth: None,
            show_deleted: false,
            is_encrypted: false,
            selection_type: crate::mapper::SelectionType::Object,
            document_mapping: crate::document::DocumentMapping::default(),
        };

        // Execute with the planner to get filtered documents
        let fetcher_arc = FetcherWrapper::new(fetcher);
        let collections_map = self.collections_map().await?;
        let collections: Vec<CollectionVersion> =
            collections_map.values().map(|c| (**c).clone()).collect();

        let mut planner = Planner::new(collections).with_fetcher(Arc::new(fetcher_arc));
        if let Some(ref acp) = self.acp {
            planner = planner.with_acp(acp.clone(), identity);
        }
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(&filter_select)?;
        let mut plan = plan_result.plan;
        let mapping = plan.document_map().clone();

        // Execute the plan and collect results
        plan.init().await?;
        plan.start().await?;

        let mut docs = Vec::new();
        while plan.next().await? {
            let doc = plan.value().deep_clone();
            docs.push(doc);
        }
        plan.close().await?;

        // Compute the aggregate based on type
        let value = match agg.aggregate_type {
            AggregateType::Count => {
                let count = docs.len() as i64;
                JsonValue::Number(count.into())
            }
            AggregateType::Sum => {
                if let Some(fname) = field_name {
                    if let Some(field_idx) = mapping.first_index_of_name(fname) {
                        let sum: f64 = docs
                            .iter()
                            .filter_map(|doc| doc.get(field_idx))
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .sum();
                        if sum == sum.floor() {
                            JsonValue::Number((sum as i64).into())
                        } else {
                            JsonValue::Number(
                                serde_json::Number::from_f64(sum).unwrap_or_else(|| 0.into()),
                            )
                        }
                    } else {
                        JsonValue::Number(0.into())
                    }
                } else {
                    JsonValue::Number(0.into())
                }
            }
            AggregateType::Average => {
                if let Some(fname) = field_name {
                    if let Some(field_idx) = mapping.first_index_of_name(fname) {
                        let values: Vec<f64> = docs
                            .iter()
                            .filter_map(|doc| doc.get(field_idx))
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if values.is_empty() {
                            JsonValue::Number(0.into())
                        } else {
                            let avg = values.iter().sum::<f64>() / values.len() as f64;
                            JsonValue::Number(
                                serde_json::Number::from_f64(avg).unwrap_or_else(|| 0.into()),
                            )
                        }
                    } else {
                        JsonValue::Number(0.into())
                    }
                } else {
                    JsonValue::Number(0.into())
                }
            }
            AggregateType::Min => {
                if let Some(fname) = field_name {
                    if let Some(field_idx) = mapping.first_index_of_name(fname) {
                        let min: Option<f64> = docs
                            .iter()
                            .filter_map(|doc| doc.get(field_idx))
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        match min {
                            Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                            Some(v) => JsonValue::Number(
                                serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                            ),
                            None => JsonValue::Null,
                        }
                    } else {
                        JsonValue::Null
                    }
                } else {
                    JsonValue::Null
                }
            }
            AggregateType::Max => {
                if let Some(fname) = field_name {
                    if let Some(field_idx) = mapping.first_index_of_name(fname) {
                        let max: Option<f64> = docs
                            .iter()
                            .filter_map(|doc| doc.get(field_idx))
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        match max {
                            Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                            Some(v) => JsonValue::Number(
                                serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                            ),
                            None => JsonValue::Null,
                        }
                    } else {
                        JsonValue::Null
                    }
                } else {
                    JsonValue::Null
                }
            }
        };

        // Return just the value (not wrapped in an object with output_name)
        // The caller will insert this with the correct key
        let _ = output_name; // suppress unused warning
        Ok(value)
    }
}

/// Extract a numeric value from a JSON item for aggregate computation.
///
/// Handles two cases:
/// - Inline array items: raw values like `JsonValue::Number(5)` — field_name is None
/// - Relation items: objects like `{"score": 5}` — field_name specifies which key
fn extract_numeric(item: &JsonValue, field_name: Option<&str>) -> Option<f64> {
    let val = match field_name {
        Some(field) => {
            // Relation aggregate: extract field from object
            item.as_object()?.get(field)?
        }
        None => {
            // Inline array aggregate: item is the value itself
            item
        }
    };
    val.as_f64().or_else(|| val.as_i64().map(|n| n as f64))
}
