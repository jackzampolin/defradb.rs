//! Ranking centroids for probing: which lists a query actually scans.

use crate::index::vector::engine::ann::Centroids;
use defra_core::vector::Metric;

/// The lists a query probes, nearest centroid first.
///
/// Always ranked by squared Euclidean distance over the centroids, whatever
/// the engine's configured metric: the coarse step is a partitioning
/// heuristic, not the ranking itself, and centroids are trained over vectors
/// already prepared the way that metric requires (normalized, under cosine).
///
/// `effort` overrides the configured `nprobe` the way `ef_search` does for a
/// graph; `None` takes the configured value. Either way the result holds at
/// least one list and never more than exist.
pub fn probe_lists(
    query: &[f32],
    centroids: &Centroids,
    configured_nprobe: usize,
    effort: Option<usize>,
) -> Vec<usize> {
    let nprobe = effort
        .map(|e| e.max(1))
        .unwrap_or(configured_nprobe)
        .min(centroids.k)
        .max(1);

    let mut lists: Vec<(usize, f64)> = (0..centroids.k)
        .map(|index| {
            (
                index,
                Metric::Euclidean.distance(query, centroids.get(index)),
            )
        })
        .collect();
    lists.sort_by(|a, b| a.1.total_cmp(&b.1));
    lists.truncate(nprobe);
    lists.into_iter().map(|(index, _)| index).collect()
}
