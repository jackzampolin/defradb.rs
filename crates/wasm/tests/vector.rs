//! The vector index running in a browser.

#![cfg(target_arch = "wasm32")]

use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::engine::hnsw::Hnsw;
use db::index::vector::params::{Params, DEFAULT_M};
use db::index::vector::store::{MemoryNodeStore, NodeId};
use defra_core::vector::{dot, squared_euclidean, Metric, Tier};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

struct Corpus(u64);

impl Corpus {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn component(&mut self) -> f64 {
        let unit = (self.next_u64() >> 40) as f64 / (1u32 << 24) as f64;
        unit * 2.0 - 1.0
    }

    fn vector(&mut self, dimensions: usize) -> Vec<f64> {
        (0..dimensions).map(|_| self.component()).collect()
    }
}

fn reference_dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn reference_squared_euclidean(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn tolerance(terms: &[f64]) -> f64 {
    let magnitude: f64 = terms.iter().map(|t| t.abs()).sum();
    ((terms.len() + 1) as f64) * f64::EPSILON * magnitude.max(1.0)
}

#[wasm_bindgen_test]
fn the_build_selected_the_expected_tier() {
    let active = Tier::active();
    #[cfg(target_feature = "simd128")]
    assert_eq!(
        active,
        Tier::Simd128,
        "simd128 was enabled but not selected"
    );
    #[cfg(not(target_feature = "simd128"))]
    assert_eq!(
        active,
        Tier::Scalar,
        "no SIMD tier can exist without simd128"
    );
}

#[wasm_bindgen_test]
fn kernels_agree_with_a_scalar_reference() {
    let mut corpus = Corpus(0x5EED_1234);

    for dimensions in [1usize, 2, 3, 4, 7, 8, 15, 16, 17, 31, 64, 129, 768] {
        let a = corpus.vector(dimensions);
        let b = corpus.vector(dimensions);

        let products: Vec<f64> = a.iter().zip(&b).map(|(x, y)| x * y).collect();
        let got = dot(&a, &b);
        let want = reference_dot(&a, &b);
        assert!(
            (got - want).abs() <= tolerance(&products),
            "dot disagreed at {dimensions} dimensions: {got} vs {want}"
        );

        let squares: Vec<f64> = a.iter().zip(&b).map(|(x, y)| (x - y) * (x - y)).collect();
        let got = squared_euclidean(&a, &b);
        let want = reference_squared_euclidean(&a, &b);
        assert!(
            (got - want).abs() <= tolerance(&squares),
            "squared euclidean disagreed at {dimensions} dimensions: {got} vs {want}"
        );

        let narrow_a: Vec<f32> = a.iter().map(|x| *x as f32).collect();
        let narrow_b: Vec<f32> = b.iter().map(|x| *x as f32).collect();
        let widened_a: Vec<f64> = narrow_a.iter().map(|x| *x as f64).collect();
        let widened_b: Vec<f64> = narrow_b.iter().map(|x| *x as f64).collect();
        let products: Vec<f64> = widened_a
            .iter()
            .zip(&widened_b)
            .map(|(x, y)| x * y)
            .collect();
        let got = dot(&narrow_a, &narrow_b);
        let want = reference_dot(&widened_a, &widened_b);
        assert!(
            (got - want).abs() <= tolerance(&products),
            "f32 dot disagreed at {dimensions} dimensions: {got} vs {want}"
        );
    }
}

#[wasm_bindgen_test]
fn integral_widths_are_exact() {
    let a32 = [3i32, -4, 12, 0, 5];
    let b32 = [1i32, 2, -3, 7, 5];
    assert_eq!(dot(&a32, &b32), 3.0 - 8.0 - 36.0 + 0.0 + 25.0);

    let a64 = [3i64, -4, 12, 0, 5];
    let b64 = [1i64, 2, -3, 7, 5];
    assert_eq!(dot(&a64, &b64), 3.0 - 8.0 - 36.0 + 0.0 + 25.0);
    assert_eq!(squared_euclidean(&a64, &b64), 4.0 + 36.0 + 225.0 + 49.0);
}

#[wasm_bindgen_test]
fn cosine_holds_its_known_values() {
    let identical = Metric::Cosine.distance(&[3.0f64, 4.0], &[3.0f64, 4.0]);
    assert!(
        identical.abs() < 1e-12,
        "a vector is nearest itself: {identical}"
    );

    let orthogonal = Metric::Cosine.distance(&[1.0f64, 0.0], &[0.0f64, 1.0]);
    assert!(
        (orthogonal - 1.0).abs() < 1e-12,
        "orthogonal is 1: {orthogonal}"
    );

    let opposed = Metric::Cosine.distance(&[1.0f64, 0.0], &[-1.0f64, 0.0]);
    assert!((opposed - 2.0).abs() < 1e-12, "opposed is 2: {opposed}");

    let scaled = Metric::Cosine.distance(&[1.0f64, 2.0], &[100.0f64, 200.0]);
    assert!(scaled.abs() < 1e-9, "direction alone decides: {scaled}");
}

#[wasm_bindgen_test]
async fn the_graph_returns_the_nearest_neighbours() {
    const DIMENSIONS: usize = 16;
    const DOCUMENTS: usize = 200;
    const K: usize = 10;

    let mut corpus = Corpus(0xC0FFEE);
    let vectors: Vec<Vec<f64>> = (0..DOCUMENTS).map(|_| corpus.vector(DIMENSIONS)).collect();

    let mut graph = Hnsw::new(
        MemoryNodeStore::default(),
        Metric::Cosine,
        Params::new(DEFAULT_M),
        0xA5A5_A5A5,
    );
    for (i, vector) in vectors.iter().enumerate() {
        graph
            .insert(NodeId(i as u64 + 1), vector.as_slice())
            .await
            .expect("insert must succeed in the browser");
    }

    let query = &vectors[7];
    let hits = graph
        .search(query.as_slice(), K, None)
        .await
        .expect("search must succeed in the browser");

    assert_eq!(hits.len(), K, "the graph must return a full k");
    assert_eq!(
        hits[0].id,
        NodeId(8),
        "a corpus vector's nearest neighbour is its own document"
    );

    let mut exhaustive: Vec<(u64, f64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u64 + 1, Metric::Cosine.distance(query.as_slice(), v)))
        .collect();
    exhaustive.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

    let exact: Vec<u64> = exhaustive.iter().take(K).map(|(id, _)| *id).collect();
    let found: Vec<u64> = hits.iter().map(|hit| hit.id.0).collect();
    let overlap = found.iter().filter(|id| exact.contains(id)).count();
    assert!(
        overlap >= K - 1,
        "browser recall must match the host's: got {found:?}, exact {exact:?}"
    );
}
