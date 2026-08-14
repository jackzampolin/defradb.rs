//! Non-planner execution path for simple queries.

use acp::Identity;
use identity::Did;
use schema::CollectionVersion;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::document::documents_to_plan_docs;
use crate::error::Result;
use crate::mapper::Select;
use crate::planner::index_selection::{filter_to_index_scan, select_best_index};
use crate::txn::TransactionRegistry;

use super::super::fetcher::FetcherWrapper;
use super::super::plan::{self, ScanSource};
use super::super::plan_drive;
use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
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

        // A fetcher-backed scan streams documents one at a time, so it honours
        // show_deleted, the scan filter, and a downstream limit without ever
        // materializing the collection. doc_ids and index-scan lookups fetch
        // specific documents rather than scanning, so they stay materialized.
        let source = if let Some(narrowed) = try_vector_narrow(select, collection, fetcher).await? {
            narrowed
        } else if select.show_deleted {
            ScanSource::Fetcher(Arc::new(FetcherWrapper::new(fetcher)))
        } else if let Some(ref doc_ids) = select.doc_ids {
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
            ScanSource::Docs(documents_to_plan_docs(&result.into_docs(), &mapping)?)
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
                        ScanSource::Docs(documents_to_plan_docs(&result.into_docs(), &mapping)?)
                    } else {
                        // Filter doesn't translate to index scan, stream the collection
                        ScanSource::Fetcher(Arc::new(FetcherWrapper::new(fetcher)))
                    }
                } else {
                    // No suitable index found, stream the collection
                    ScanSource::Fetcher(Arc::new(FetcherWrapper::new(fetcher)))
                }
            } else {
                // Fetcher doesn't support index queries or no indexes, stream the collection
                ScanSource::Fetcher(Arc::new(FetcherWrapper::new(fetcher)))
            }
        } else {
            ScanSource::Fetcher(Arc::new(FetcherWrapper::new(fetcher)))
        };

        // Build ACP filter config when the collection is policy-backed.
        let acp_filter = collection.policy.as_ref().map(|policy| plan::AcpFilter {
            acp: self.acp.clone(),
            identity: Identity::from(identity),
            policy_id: policy.id.clone(),
            resource_name: policy.resource_name.clone(),
        });

        // Build and execute the plan (ACP filter is inserted inside, after Select but before aggregates)
        let mut plan = plan::build_plan(
            select,
            source,
            mapping.clone(),
            collection,
            acp_filter,
            self.query_limits,
        )?;

        let outcome = async {
            plan.init().await?;
            plan.start().await?;

            let mut results = Vec::new();

            while plan.next().await? {
                let doc = plan.value();
                let json = self.doc_to_json(doc, &mapping)?;
                results.push(json);
            }

            Ok(results)
        }
        .await;

        let results = plan_drive::close_after(plan.as_mut(), outcome).await?;

        Ok(JsonValue::Array(results))
    }
}

/// Narrows a scan to the nearest documents when a vector index can answer the
/// query. `None` leaves the existing paths untouched.
async fn try_vector_narrow(
    select: &query_types::mapper::Select,
    collection: &schema::CollectionVersion,
    fetcher: &dyn query_plan::fetcher::DocFetcher,
) -> Result<Option<ScanSource>> {
    use query_plan::planner::vector_routing::{route, similarity_query};

    if !fetcher.supports_vector_search() {
        return Ok(None);
    }

    let Ok(route) = route(&similarity_query(select), &collection.indexes) else {
        return Ok(None);
    };

    let doc_short_ids = fetcher
        .vector_search(
            &select.collection_name,
            route.index_id,
            &route.query_vector,
            route.k,
            None,
        )
        .await?;

    debug!(
        collection = %select.collection_name,
        index_id = route.index_id,
        k = route.k,
        found = doc_short_ids.len(),
        "narrowed scan with a vector index"
    );

    Ok(Some(ScanSource::VectorNarrowed {
        fetcher: Arc::new(FetcherWrapper::new(fetcher)),
        doc_short_ids,
    }))
}
