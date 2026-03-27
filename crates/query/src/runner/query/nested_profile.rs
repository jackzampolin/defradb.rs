//! Profiling structs for nested query and scoped fulltext operations.

use std::time::Duration;

#[derive(Debug, Default)]
pub(crate) struct ScopedFulltextProfile {
    pub scoring_calls: usize,
    pub sort_calls: usize,
    pub top_k_calls: usize,
    pub items_seen: usize,
    pub target_fields_seen: usize,
    pub docs_indexed: usize,
    pub scoring_elapsed: Duration,
    pub sort_elapsed: Duration,
    pub top_k_elapsed: Duration,
}

#[derive(Debug, Default)]
pub(crate) struct NestedQueryProfile {
    pub precompute_fulltext_elapsed: Duration,
    pub plan_build_elapsed: Duration,
    pub plan_init_elapsed: Duration,
    pub plan_start_elapsed: Duration,
    pub plan_iteration_elapsed: Duration,
    pub doc_render_elapsed: Duration,
    pub ordering_only_strip_elapsed: Duration,
    pub plan_close_elapsed: Duration,
    pub relation_aggregate_elapsed: Duration,
    pub scoped_fulltext_elapsed: Duration,
    pub clean_filter_only_fields_elapsed: Duration,
    pub relation_limits_elapsed: Duration,
    pub result_count: usize,
    pub scoped_fulltext: ScopedFulltextProfile,
}
