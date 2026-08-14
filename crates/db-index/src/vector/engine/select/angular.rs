//! SSG's angular pruning (Fu, Wang, Cai 2019, arXiv:1907.06146).

use crate::vector::core::{dot, Metric};
use crate::vector::engine::ann::{Candidate, EdgeSelector};

/// Minimum angle between two kept edges, in degrees. The paper's value.
pub const DEFAULT_ANGLE_DEGREES: f32 = 60.0;

/// Keeps an edge only when its angle to every edge already kept is at least
/// `angle`, so a node's neighbours spread over directions rather than crowding
/// one bearing.
///
/// The angle is between the *difference* vectors `c - base` and `k - base`.
/// Those are never materialised: expanding the inner products lets every term
/// come from [`dot`], which dispatches to the platform's widest SIMD tier, and
/// only `dot(c, k)` is new per pair.
#[derive(Debug, Clone, Copy)]
pub struct Angular {
    cos_threshold: f64,
}

impl Angular {
    pub fn new(angle_degrees: f32) -> Self {
        Self {
            cos_threshold: (angle_degrees as f64).to_radians().cos(),
        }
    }

    pub fn cos_threshold(&self) -> f64 {
        self.cos_threshold
    }
}

impl Default for Angular {
    fn default() -> Self {
        Self::new(DEFAULT_ANGLE_DEGREES)
    }
}

/// `dot(c - b, k - b)`, expanded so no difference vector is built.
fn shifted_dot(c: &[f32], k: &[f32], cb: f64, kb: f64, bb: f64) -> f64 {
    dot(c, k) - cb - kb + bb
}

impl EdgeSelector for Angular {
    fn select(
        &self,
        _metric: Metric,
        base: &[f32],
        candidates: &[Candidate],
        max: usize,
    ) -> Vec<Candidate> {
        if base.is_empty() {
            return candidates.iter().take(max).cloned().collect();
        }

        let mut sorted = candidates.to_vec();
        sorted.sort();

        let bb = dot(base, base);
        let mut selected: Vec<Candidate> = Vec::with_capacity(max);
        let mut kept: Vec<(f64, f64)> = Vec::with_capacity(max);

        for candidate in sorted {
            if selected.len() >= max {
                break;
            }
            let cb = dot(&candidate.vector, base);
            let cc = dot(&candidate.vector, &candidate.vector);
            let norm_c = (cc - 2.0 * cb + bb).max(0.0);
            if norm_c <= f64::EPSILON {
                continue;
            }

            let spread = selected.iter().zip(&kept).all(|(k, (kb, norm_k))| {
                let numerator = shifted_dot(&candidate.vector, &k.vector, cb, *kb, bb);
                let cos = numerator / (norm_c.sqrt() * norm_k.sqrt());
                cos <= self.cos_threshold
            });

            if spread {
                kept.push((cb, norm_c));
                selected.push(candidate);
            }
        }
        selected
    }
}
