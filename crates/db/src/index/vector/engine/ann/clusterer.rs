//! Fitting centroids over a resident sample.

use defra_core::vector::squared_euclidean;

/// Trained centroids, flat: `k * dimensions`.
#[derive(Debug, Clone, PartialEq)]
pub struct Centroids {
    pub k: usize,
    pub dimensions: usize,
    pub values: Vec<f32>,
}

impl Centroids {
    pub fn get(&self, index: usize) -> &[f32] {
        let at = index * self.dimensions;
        &self.values[at..at + self.dimensions]
    }

    /// The nearest centroid to `vector`, and its squared distance.
    pub fn nearest(&self, vector: &[f32]) -> (usize, f64) {
        let mut best = (0usize, f64::INFINITY);
        for index in 0..self.k {
            let distance = squared_euclidean(vector, self.get(index));
            if distance < best.1 {
                best = (index, distance);
            }
        }
        best
    }
}

/// How the fit went. Reported rather than assumed, because a sample smaller
/// than `k` cannot produce `k` meaningful centroids and the caller has to know.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Fit {
    /// Centroids actually produced, never more than the distinct sample size.
    pub k: usize,
    /// Iterations run before assignments stopped changing.
    pub iterations: usize,
    /// Mean squared distance from a sample point to its centroid.
    pub inertia: f64,
}

pub trait Clusterer {
    fn fit(&self, sample: &[f32], dimensions: usize, k: usize) -> (Centroids, Fit);
}
