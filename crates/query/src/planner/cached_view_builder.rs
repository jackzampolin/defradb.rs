//! Cached view plan builder for materialized views.
//!
//! Materialized views store query results in a cache. This module builds
//! execution plans that read from the cache instead of executing live queries.

use std::collections::HashMap;

use crate::error::Result;
use crate::mapper::Select;
use crate::plan::{CachedViewFetcher, SelectNode};
use crate::planner::{PlanNode, PlanResult, Planner};

impl Planner {
    /// Build a plan for a materialized view.
    ///
    /// Instead of executing the view's query, this reads cached results
    /// from the view cache storage. The CachedViewFetcher loads pre-computed
    /// results and applies any user-specified filters.
    pub(crate) fn build_cached_view_plan(
        &self,
        select: &Select,
        collection: &schema::CollectionVersion,
    ) -> Result<PlanResult> {
        // Build the target (view) mapping
        let mapping = self.build_mapping(select, collection)?;

        // Create the cached view fetcher
        let mut fetcher = CachedViewFetcher::new(collection.root_id, mapping.clone());

        // Attach the document fetcher if available (for loading cache entries)
        if let Some(doc_fetcher) = &self.fetcher {
            fetcher = fetcher.with_fetcher(doc_fetcher.clone());
        }

        // Wrap in SelectNode (matches Go's structure: selectNode -> scanNode)
        let plan: Box<dyn PlanNode> = if let Some(ref filter) = select.filter {
            Box::new(SelectNode::new(Box::new(fetcher), mapping).with_filter(filter.clone()))
        } else {
            Box::new(SelectNode::new(Box::new(fetcher), mapping))
        };

        Ok(PlanResult {
            plan,
            index_scan: None,
            ordering_only_fields: Vec::new(),
            aggregate_internal_keys: HashMap::new(),
            warnings: Vec::new(),
        })
    }
}
