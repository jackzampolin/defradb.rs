//! Non-planner execution path for simple queries.

use acp::Identity;
use identity::Did;
use schema::CollectionVersion;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::document::{documents_to_plan_docs, documents_with_status_to_plan_docs};
use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::planner::index_selection::{filter_to_index_scan, select_best_index};
use crate::txn::TransactionRegistry;

use super::super::plan;
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
        let can_page_at_fetch = select.limit.is_some()
            && !select.show_deleted
            && select.filter.is_none()
            && select.doc_ids.is_none()
            && select.order_by.is_none()
            && select.group_by.is_none()
            && collection.policy.is_none()
            && select
                .fields
                .iter()
                .all(|field| !matches!(field, Requestable::Aggregate(_)));
        let select_without_limit;
        let select_for_plan = if can_page_at_fetch {
            select_without_limit = {
                let mut select = select.clone();
                select.limit = None;
                select
            };
            &select_without_limit
        } else {
            select
        };

        // Build document mapping first (needed for both paths)
        let mapping = plan::build_mapping(select_for_plan, collection)?;

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
                if can_page_at_fetch {
                    let limit = select.limit.as_ref().and_then(|limit| match limit.limit {
                        Some(0) => None,
                        other => other,
                    });
                    let offset = select.limit.as_ref().map(|limit| limit.offset).unwrap_or(0);
                    fetcher
                        .get_all_page(&select.collection_name, limit, offset)
                        .await?
                } else {
                    fetcher.get_all(&select.collection_name).await?
                }
            };
            documents_to_plan_docs(&docs, &mapping)?
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
            select_for_plan,
            plan_docs,
            mapping.clone(),
            collection,
            acp_filter,
        )?;

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
}
