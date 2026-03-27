use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::time::Instant;
use tracing::{debug, warn};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::planner::{Doc, ExecInfo, PlanNode};

use super::node::TypeJoinMany;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for TypeJoinMany {
    async fn init(&mut self) -> Result<()> {
        let init_start = Instant::now();
        self.reset_init_state();

        let parent_scope_start = Instant::now();
        let parent_scope = if (self.parent_scoped_child_cache || self.indexed_child_fetch.is_some())
            && !self.per_parent_child_scan
        {
            Some(self.collect_parent_doc_ids().await?)
        } else {
            None
        };
        let parent_scope_elapsed = parent_scope_start.elapsed();

        // Build filter child cache if present (indexed relation filter evaluation)
        let filter_child_cache_start = Instant::now();
        let filter_index_fetches = self.build_filter_child_cache(parent_scope.as_ref()).await?;
        let filter_child_cache_elapsed = filter_child_cache_start.elapsed();

        if self.per_parent_child_scan {
            // Per-parent mode: don't cache, we'll re-scan per parent in next()
            let parent_plan_init_start = Instant::now();
            self.parent_plan.init().await?;
            let parent_plan_init_elapsed = parent_plan_init_start.elapsed();

            debug!(
                parent_collection = %self.parent_side.collection().name,
                child_collection = %self.child_side.collection().name,
                relation_field = %self.parent_side.relation_field().name,
                parent_scope_size = parent_scope.as_ref().map(|scope| scope.len()).unwrap_or(0),
                parent_scope = ?parent_scope_elapsed,
                filter_child_cache = ?filter_child_cache_elapsed,
                filter_child_keys = self.filter_child_cache.len(),
                filter_child_docs = self.filter_child_doc_count(),
                parent_plan_init = ?parent_plan_init_elapsed,
                total = ?init_start.elapsed(),
                per_parent_child_scan = self.per_parent_child_scan,
                parent_scoped_child_cache = self.parent_scoped_child_cache,
                indexed_child_fetch = self.indexed_child_fetch.is_some(),
                "TypeJoinMany init profile"
            );
        } else {
            // Build child cache first (scans child_plan once)
            let child_cache_start = Instant::now();
            self.build_child_cache(parent_scope.as_ref()).await?;
            let child_cache_elapsed = child_cache_start.elapsed();
            // Then init parent plan
            let parent_plan_init_start = Instant::now();
            self.parent_plan.init().await?;
            let parent_plan_init_elapsed = parent_plan_init_start.elapsed();

            debug!(
                parent_collection = %self.parent_side.collection().name,
                child_collection = %self.child_side.collection().name,
                relation_field = %self.parent_side.relation_field().name,
                parent_scope_size = parent_scope.as_ref().map(|scope| scope.len()).unwrap_or(0),
                parent_scope = ?parent_scope_elapsed,
                filter_child_cache = ?filter_child_cache_elapsed,
                filter_child_keys = self.filter_child_cache.len(),
                filter_child_docs = self.filter_child_doc_count(),
                child_cache = ?child_cache_elapsed,
                child_cache_keys = self.child_cache.len(),
                child_cache_docs = self.total_children_in_cache,
                child_cache_fields = self.total_fields_per_scan,
                child_cache_indexes = self.child_exec_info.indexes_fetched,
                parent_plan_init = ?parent_plan_init_elapsed,
                total = ?init_start.elapsed(),
                per_parent_child_scan = self.per_parent_child_scan,
                parent_scoped_child_cache = self.parent_scoped_child_cache,
                indexed_child_fetch = self.indexed_child_fetch.is_some(),
                "TypeJoinMany init profile"
            );
        }

        // Add filter child plan's index_fetches to the display child's
        if let Some(fetches) = filter_index_fetches {
            self.go_child_metrics.index_fetches += fetches;
        }

        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        self.parent_plan.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "TypeJoinMany.next() called before init()",
            ));
        }

        if self.per_parent_child_scan {
            return self.next_per_parent().await;
        }

        // Loop to skip parents that don't pass relation filter
        loop {
            // Track iterations (Go counts each call to next, including final false)
            self.exec_info.iterations += 1;

            if !self.parent_plan.next().await? {
                return Ok(false);
            }

            let mut parent_doc = self.parent_plan.value().deep_clone();

            // Get parent's _docID for the lookup (O(1) cache lookup)
            let parent_doc_id = match parent_doc.doc_id() {
                Some(id) => id.to_string(),
                None => {
                    warn!(
                        parent_collection = %self.parent_side.collection().name,
                        relation_field = %self.parent_side.relation_field().name,
                        "Parent document missing _docID - returning empty children array. \
                         This may indicate data corruption or a schema mismatch."
                    );
                    // No docID means no children can match - skip if filter is present
                    if self.relation_filter.is_some() {
                        continue;
                    }
                    // No filter, return with empty children
                    self.merge_children(&mut parent_doc, Vec::new());
                    self.current_doc = parent_doc;
                    return Ok(true);
                }
            };

            // Apply relation filter if present (check against ALL children, not just limited)
            if let Some(ref rel_filter) = self.relation_filter {
                // Use filter_child_cache (from indexed filter plan) when available,
                // otherwise fall back to child_cache (display plan).
                let use_filter = self.filter_child_plan.is_some();
                let filter_children = if use_filter {
                    self.filter_child_cache
                        .get(&parent_doc_id)
                        .map(|docs| docs.iter().map(|d| d.deep_clone()).collect())
                        .unwrap_or_default()
                } else {
                    self.get_all_children(&parent_doc_id)
                };
                if !self.check_relation_filter(&filter_children, rel_filter, use_filter)? {
                    // No children pass the filter - skip this parent
                    continue;
                }
            }

            // Get children (with ordering, offset, limit applied)
            let children = self.find_child_docs(&parent_doc_id);

            // Simulate Go's per-parent child scan metrics.
            // In Go, fetchPrimaryDocsReferencingSecondaryDoc re-initializes the child
            // scan for each parent, reading ALL children from the collection. The scanNode
            // uses a filteredFetcher that skips non-matching docs inside FetchNext().
            if let Some(limit) = self.child_limit {
                // With a child limit, Go's collectDocs(limit) stops after finding
                // enough matches. The filteredFetcher reads docs from storage in CID
                // order, skipping non-matching docs internally. We simulate this by
                // walking the recorded scan order.
                let effective_limit = self.child_offset + limit;
                let mut matches_found = 0u64;
                let mut docs_read = 0u64;
                let mut fields_read = 0u64;
                let mut iterations = 0u64;

                for (fk, field_count) in &self.child_scan_order {
                    docs_read += 1;
                    fields_read += field_count;
                    if fk == &parent_doc_id {
                        matches_found += 1;
                        iterations += 1; // Each match ends a FetchNext call → Next() returns true
                        if matches_found >= effective_limit {
                            break; // collectDocs stops when limit reached
                        }
                    }
                }

                // If collection exhausted without hitting limit, add 1 for the final
                // false Next() call (FetchNext returns nil → Next returns false).
                if matches_found < effective_limit {
                    iterations += 1;
                }

                self.go_child_metrics.iterations += iterations;
                self.go_child_metrics.doc_fetches += docs_read;
                self.go_child_metrics.field_fetches += fields_read;
            } else {
                // Without a child limit, Go scans ALL children per parent.
                // Each FetchNext reads until finding a match (or end), so iterations
                // = matching children + 1 (for the final false Next()).
                let matching_count = self
                    .child_cache
                    .get(&parent_doc_id)
                    .map(|v| v.len() as u64)
                    .unwrap_or(0);
                self.go_child_metrics.iterations += matching_count + 1;
                self.go_child_metrics.doc_fetches += self.total_children_in_cache;
                self.go_child_metrics.field_fetches += self.total_fields_per_scan;
            }

            // Merge children array into parent
            self.merge_children(&mut parent_doc, children);
            self.current_doc = parent_doc;

            return Ok(true);
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.parent_plan.close().await?;
        // child_plan was already closed in build_child_cache()
        self.child_cache.clear();
        self.child_scan_order.clear();
        self.filter_child_cache.clear();
        // filter_child_plan was already closed in init()
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.parent_plan.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        // Go's explain uses "typeIndexJoin" as the wrapper node
        "typeIndexJoin"
    }

    fn explain_inner(&self) -> JsonValue {
        self.explain_inner_impl()
    }

    fn explain_debug_inner(&self) -> JsonValue {
        self.explain_debug_inner_impl()
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info_impl()
    }

    fn explain_execute_inner(&self) -> JsonValue {
        self.explain_execute_inner_impl()
    }
}
