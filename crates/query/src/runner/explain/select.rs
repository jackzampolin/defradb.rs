use schema::CollectionVersion;
use serde_json::Value as JsonValue;

use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::planner::index_selection::{can_be_ordered_by_index, select_best_index};
use crate::planner::Planner;
use crate::query_parse::ExplainType;
use crate::txn::TransactionRegistry;

use super::super::plan;
use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Generate an explanation of a single Select operation.
    pub(crate) async fn explain_select(
        &self,
        select: &Select,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        // Handle encrypted search queries - return a simple seScanNode explanation
        if select.is_encrypted {
            return Ok(serde_json::json!({
                "selectNode": {
                    "seScanNode": {
                        "collection": select.collection_name,
                        "filter": select.filter.as_ref().map(|f| f.conditions())
                    }
                }
            }));
        }

        // Handle _commits system collection specially
        if select.collection_name == "_commits" {
            return self.explain_commits_select(select, explain_type);
        }

        // Get collection schema
        let collection = self
            .collection_provider
            .get_collection(&select.collection_name)
            .await?
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Check if this query has nested selections (relations)
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        // Check if an ordering-only index can be used (planner needed for IndexScanNode)
        let has_ordering_index = !has_nested
            && select.order_by.is_some()
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

        // Check if a filter-based index can be used
        let has_filter_index = !has_nested
            && select.filter.is_some()
            && !collection.indexes.is_empty()
            && select
                .filter
                .as_ref()
                .map(|f| select_best_index(f, &collection.indexes).is_some())
                .unwrap_or(false);

        // Check if any aggregates reference relations (e.g., _sum(articles: {field: pages}))
        let has_relation_aggregates = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets
                    .iter()
                    .any(|t| !t.host_name.is_empty() && t.host_name != select.collection_name)
            } else {
                false
            }
        });

        // Check if the filter references relation fields (e.g., {author: {verified: true}})
        let filter_has_relations = select
            .filter
            .as_ref()
            .map(|f| f.has_relation_filters())
            .unwrap_or(false);

        // Check if the order references relation fields (e.g., {author: {age: DESC}})
        let order_has_relations = select
            .order_by
            .as_ref()
            .map(|o| o.has_relation_order())
            .unwrap_or(false);

        // Check if any similarity fields are present (require SimilarityNode in planner)
        let has_similarity = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Similarity(_)));

        // Check if any secondary relation ID fields are selected (e.g., `_authorID`)
        let has_secondary_relation_id = select.fields.iter().any(|f| {
            if let Requestable::Field(field) = f {
                let field_name = &field.name;
                if field_name.starts_with('_') && field_name.ends_with("ID") && field_name.len() > 3
                {
                    let relation_name = &field_name[1..field_name.len() - 2];
                    if let Some(relation_field) = collection.field_by_name(relation_name) {
                        return relation_field.kind.is_relation() && !relation_field.is_primary;
                    }
                }
            }
            false
        });

        let is_view = collection.query.is_some();

        if is_view
            || has_nested
            || has_ordering_index
            || has_filter_index
            || has_relation_aggregates
            || filter_has_relations
            || order_has_relations
            || has_similarity
            || has_secondary_relation_id
        {
            // Use the Planner for views, nested selections, index usage, relation aggregates,
            // relation filters/ordering, similarity, or secondary relation IDs
            self.explain_nested_select(select, explain_type).await
        } else {
            // Explain simple query plan
            self.explain_simple_select(select, &collection, explain_type)
        }
    }

    /// Generate an explanation for a query with nested selections.
    pub(crate) async fn explain_nested_select(
        &self,
        select: &Select,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        // Build the plan using the Planner
        let collection_names = self.collection_provider.list_collections().await?;
        let mut collections = Vec::new();
        for name in collection_names {
            if let Some(coll) = self.collection_provider.get_collection(&name).await? {
                collections.push((*coll).clone());
            }
        }

        let mut planner = Planner::new(collections);
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(select)?;
        let plan = plan_result.plan;

        // Get the plan explanation based on type
        let explain = match explain_type {
            ExplainType::Debug => plan.explain_debug(),
            _ => plan.explain(),
        };

        // Ensure result is wrapped in selectNode (Go format)
        Ok(Self::ensure_select_node_wrapper(
            explain,
            select,
            explain_type,
        ))
    }

    /// Generate an explanation for a simple query without nested selections.
    pub(crate) fn explain_simple_select(
        &self,
        select: &Select,
        collection: &CollectionVersion,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        // Build document mapping and plan
        let mapping = plan::build_mapping(select, collection)?;

        // Create an empty plan with no documents for explanation purposes
        let plan = plan::build_plan(select, vec![], mapping, collection, None)?;

        // Get the plan explanation based on type
        let explain = match explain_type {
            ExplainType::Debug => plan.explain_debug(),
            _ => plan.explain(),
        };

        // Ensure result is wrapped in selectNode (Go format)
        Ok(Self::ensure_select_node_wrapper(
            explain,
            select,
            explain_type,
        ))
    }

    /// Process explain output for Go format compatibility.
    ///
    /// Since we now always create SelectNode in the plan, this function handles:
    /// - For Simple mode: ensures docID and filter attributes are in selectNode
    /// - For Debug mode: returns as-is (no additional attributes)
    /// - For Execute mode: returns as-is (attributes added during execution)
    pub(crate) fn ensure_select_node_wrapper(
        explain: JsonValue,
        _select: &Select,
        explain_type: ExplainType,
    ) -> JsonValue {
        // For Debug mode, return as-is (Go debug doesn't add attributes)
        if matches!(explain_type, ExplainType::Debug) {
            return explain;
        }

        // For Simple/Execute mode, the SelectNode already has the attributes
        // from its explain_inner method, so return as-is
        explain
    }

    /// Generate an explanation for a _commits system collection query.
    ///
    /// Returns a dagScanNode structure matching Go's explain output for commits queries.
    pub(crate) fn explain_commits_select(
        &self,
        select: &Select,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        // For Debug mode, return empty inner objects
        if matches!(explain_type, ExplainType::Debug) {
            return Ok(serde_json::json!({
                "selectNode": {
                    "dagScanNode": {}
                }
            }));
        }

        // Build the dagScanNode attributes for Simple/Execute mode
        let mut dag_scan_attrs = serde_json::Map::new();

        // cid: the specific commit CID if provided, else null
        if let Some(ref cids) = select.cid {
            dag_scan_attrs.insert("cid".to_string(), serde_json::json!(cids));
        } else {
            dag_scan_attrs.insert("cid".to_string(), serde_json::Value::Null);
        }

        // prefixes: array of storage prefixes being scanned
        // Format: /d/<docID> for document-specific commits
        let prefixes: Vec<String> = if let Some(ref doc_ids) = select.doc_ids {
            doc_ids.iter().map(|id| format!("/d/{}", id)).collect()
        } else {
            vec![]
        };
        dag_scan_attrs.insert("prefixes".to_string(), serde_json::json!(prefixes));

        // Build the selectNode wrapper (Go structure: selectNode -> dagScanNode)
        let dag_scan_node = serde_json::json!({ "dagScanNode": dag_scan_attrs });

        Ok(serde_json::json!({
            "selectNode": dag_scan_node
        }))
    }
}
