//! Fitting centroids.

use db_index::vector::core::squared_euclidean;
use db_index::vector::engine::ann::Clusterer;
use db_index::vector::quantize::KMeans;

mod common;

/// Four corners of a square, well separated, repeated. The right answer is
/// known without computing anything.
fn four_clusters(per_cluster: usize) -> (Vec<f32>, Vec<[f32; 2]>) {
    let centers = vec![[0.0, 0.0], [10.0, 0.0], [0.0, 10.0], [10.0, 10.0]];
    let mut flat = Vec::new();
    let mut corpus = common::Corpus::new(0xC1057);
    for _ in 0..per_cluster {
        for center in &centers {
            let jitter = corpus.vector(2);
            flat.push(center[0] + jitter[0] * 0.3);
            flat.push(center[1] + jitter[1] * 0.3);
        }
    }
    (flat, centers)
}

#[test]
fn separable_clusters_are_recovered() {
    let (sample, centers) = four_clusters(60);
    let (centroids, fit) = KMeans::new(1).fit(&sample, 2, 4);

    assert_eq!(fit.k, 4);
    for center in &centers {
        let (_, distance) = centroids.nearest(center);
        assert!(
            distance < 1.0,
            "no centroid landed near {center:?}, nearest was {distance} away"
        );
    }
}

/// Every centroid must own points. An empty cluster means the fit wasted a
/// centroid and the index would have a list nothing routes to.
#[test]
fn no_cluster_is_left_empty() {
    let (sample, _) = four_clusters(40);
    let (centroids, _) = KMeans::new(7).fit(&sample, 2, 4);

    let mut owned = vec![0usize; 4];
    for point in sample.chunks(2) {
        owned[centroids.nearest(point).0] += 1;
    }
    assert!(
        owned.iter().all(|count| *count > 0),
        "some centroid owns nothing: {owned:?}"
    );
}

/// More centroids than points cannot be meaningful, so `k` is clamped and the
/// fit reports what it actually produced.
#[test]
fn k_is_clamped_to_the_sample_size() {
    let sample = vec![1.0f32, 1.0, 2.0, 2.0, 3.0, 3.0];
    let (centroids, fit) = KMeans::new(3).fit(&sample, 2, 50);
    assert_eq!(fit.k, 3);
    assert_eq!(centroids.k, 3);
    assert_eq!(centroids.values.len(), 3 * 2);
}

#[test]
fn an_empty_sample_fits_nothing() {
    let (centroids, fit) = KMeans::new(1).fit(&[], 4, 8);
    assert_eq!(fit.k, 0);
    assert_eq!(centroids.k, 0);
    assert!(centroids.values.is_empty());
}

/// A rebuild must produce the same index, so the fit is a function of the seed.
#[test]
fn the_same_seed_fits_the_same_centroids() {
    let (sample, _) = four_clusters(30);
    let a = KMeans::new(99).fit(&sample, 2, 4).0;
    let b = KMeans::new(99).fit(&sample, 2, 4).0;
    assert_eq!(a, b);
}

/// More centroids can only fit the sample more tightly. A rise would mean the
/// iteration is diverging.
#[test]
fn inertia_falls_as_k_rises() {
    let mut corpus = common::Corpus::new(0x1E27);
    let sample: Vec<f32> = corpus.vectors(400, 8).into_iter().flatten().collect();

    let mut previous = f64::INFINITY;
    for k in [2usize, 4, 8, 16, 32] {
        let (_, fit) = KMeans::new(5).fit(&sample, 8, k);
        assert!(
            fit.inertia <= previous + 1e-6,
            "inertia rose at k={k}: {} after {previous}",
            fit.inertia
        );
        previous = fit.inertia;
    }
}

/// Identical points give k-means++ nothing to spread over; it must not divide
/// by a zero total or loop forever.
#[test]
fn a_degenerate_sample_still_fits() {
    let sample = vec![2.0f32; 40 * 4];
    let (centroids, fit) = KMeans::new(13).fit(&sample, 4, 8);
    assert_eq!(fit.k, 8);
    assert!(fit.inertia.abs() < 1e-9, "inertia was {}", fit.inertia);
    for index in 0..centroids.k {
        assert!(centroids.get(index).iter().all(|v| (*v - 2.0).abs() < 1e-6));
    }
}

/// The centroid a point is assigned to must really be its nearest.
#[test]
fn nearest_agrees_with_an_exhaustive_scan() {
    let mut corpus = common::Corpus::new(0x9A17);
    let sample: Vec<f32> = corpus.vectors(200, 6).into_iter().flatten().collect();
    let (centroids, _) = KMeans::new(2).fit(&sample, 6, 12);

    for point in sample.chunks(6) {
        let (got, distance) = centroids.nearest(point);
        let want = (0..centroids.k)
            .map(|i| (i, squared_euclidean(point, centroids.get(i))))
            .min_by(|a, b| a.1.total_cmp(&b.1))
            .unwrap();
        assert_eq!(got, want.0);
        assert!((distance - want.1).abs() < 1e-9);
    }
}
