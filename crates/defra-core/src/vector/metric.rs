//! Distance metrics. Everything here returns a distance: smaller is closer.
//!
//! That convention is stated because the implementation this was adapted from
//! mixed the two. It negated the dot product inside the kernel so the raw
//! product already meant "closer", then took `sqrt` of it while computing
//! cosine, which is `sqrt` of a negative number for every non-zero vector. The
//! kernels return true products and the inversion happens once, here.
//!
//! A distance is always `f64`, whatever width the vectors are. It is a measure
//! *between* two vectors, not a value drawn from them: the angle between two
//! `i64` vectors is irrational far more often than not, and returning it in the
//! element's own type would round the answer to whichever grid the input
//! happened to use.
//!
//! Nothing here fails, panics, or returns NaN. Vectors of different lengths are
//! compared over their shared prefix, and an undefined comparison yields the
//! largest distance the metric can express, so it sorts last instead of
//! poisoning an ordering. Dimension agreement is enforced once, where documents
//! enter the index.

use super::kernel::{self, Element};

/// How two vectors are compared.
///
/// One per `schema::DistanceMetric`, which is Go's set: `Cosine`,
/// `Euclidean`, and `NegativeDot` for Go's `DOT`. The negation is ours; a dot
/// product is a similarity, and everything here is a distance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum Metric {
    /// `1 - cos(a, b)`, in `[0, 2]`.
    #[default]
    Cosine,
    /// Straight-line distance, `>= 0`.
    Euclidean,
    /// Negated dot product, so larger products sort as nearer. Unbounded in
    /// both directions; only meaningful when magnitudes are comparable.
    NegativeDot,
}

/// Two vectors pointing exactly opposite ways. Also what an undefined cosine
/// comparison returns, so it sorts last.
pub const MAX_COSINE_DISTANCE: f64 = 2.0;

/// Smallest *squared* norm that will be scaled to unit length, matching the Go
/// implementation's `normThreshold`. Below it a vector has no usable direction,
/// and dividing by its norm would amplify rounding noise.
pub const NORM_THRESHOLD: f64 = 1e-30;

impl Metric {
    /// Distance between two vectors, over their shared prefix.
    pub fn distance<T: Element>(self, a: &[T], b: &[T]) -> f64 {
        // Sliced here, once, rather than left to the kernels. They already stop
        // at the shorter slice, but a norm does not: cosine over the full `a`
        // against a shared-prefix dot product would divide by a magnitude the
        // numerator never saw.
        let shared = a.len().min(b.len());
        let (a, b) = (&a[..shared], &b[..shared]);
        match self {
            Metric::Cosine => cosine_distance(a, b),
            Metric::Euclidean => ordered(kernel::squared_euclidean(a, b).sqrt()),
            Metric::NegativeDot => ordered(-kernel::dot(a, b)),
        }
    }

    /// Distance for vectors the caller has already normalized.
    ///
    /// For `Cosine` this skips both norms, since `|a| = |b| = 1` makes the
    /// distance `1 - dot`. That is why Go normalizes once at insert.
    ///
    /// Stays `f64` end to end. The reference implementation narrows this to
    /// `f32` (`float32(1 - dot(a, b))`); since the two implementations are not
    /// required to read each other's files, there is nothing to gain by
    /// throwing 29 bits of mantissa away on the comparison an index walk makes
    /// millions of times.
    ///
    /// The other metrics are unaffected by normalization.
    pub fn distance_normalized<T: Element>(self, a: &[T], b: &[T]) -> f64 {
        match self {
            // The premise is `|a| = |b| = 1`, which is a property of the whole
            // vector. Prefixes of unequal-length vectors are not unit length,
            // so those fall through to the general path.
            Metric::Cosine if a.len() == b.len() => {
                ordered_cosine(1.0 - kernel::dot(a, b).clamp(-1.0, 1.0))
            }
            other => other.distance(a, b),
        }
    }

    /// Distance between two vectors already in the form the index stored them.
    pub fn distance_stored<T: Element>(self, a: &[T], b: &[T]) -> f64 {
        if self.requires_normalized() {
            self.distance_normalized(a, b)
        } else {
            self.distance(a, b)
        }
    }

    /// Whether the metric reads direction alone, so vectors must be scaled to
    /// unit length before it compares them.
    ///
    /// Only cosine does. The other two read magnitude, so scaling would change
    /// the answer rather than merely the values.
    pub fn requires_normalized(self) -> bool {
        match self {
            Metric::Cosine => true,
            Metric::Euclidean | Metric::NegativeDot => false,
        }
    }

    /// A vector in the form this metric compares: narrowed to the width the
    /// engines store, and scaled to unit length when the metric needs it.
    ///
    /// Every engine converts through here, so an engine cannot disagree with
    /// another about whether its metric normalizes.
    pub fn prepare<T: Element>(self, vector: &[T]) -> Vec<f32> {
        let mut out: Vec<f32> = vector.iter().map(|x| f32::narrow(x.widen())).collect();
        if self.requires_normalized() {
            normalize(&mut out);
        }
        out
    }

    /// How near two vectors are, where a *larger* result is nearer.
    ///
    /// This is the inverse convention to the rest of this module, and it exists
    /// because it is the one the `SIMILARITY` field publishes: a query orders
    /// descending by it, and a reader expects the best match to score highest.
    /// The planner scores an unrouted selection with this while the index ranks
    /// a routed one by [`distance`](Self::distance), so the two must stay
    /// order-inverse, which is what the per-metric mapping below guarantees.
    ///
    /// Cosine is already a similarity and passes through. The other two are
    /// distances and are negated, which is monotonic: each metric keeps its own
    /// ordering and only its direction changes. Euclidean drops the square root
    /// for the same reason, matching Go's `NegativeSquaredEuclidean`.
    ///
    /// Only the shared leading elements are compared. A caller that requires
    /// equal lengths checks first, because for a query that is a mistake worth
    /// reporting rather than absorbing.
    pub fn similarity<T: Element>(self, a: &[T], b: &[T]) -> f64 {
        let shared = a.len().min(b.len());
        let (a, b) = (&a[..shared], &b[..shared]);
        match self {
            // Not `1 - cosine_distance`: that clamps to [-1, 1], and the clamp
            // would report exactly -1 or 1 where the reference reports the raw
            // quotient. A vector with no length has no direction, so its
            // similarity to anything is zero rather than a division by zero.
            // The scale is also required finite, which the reference does not
            // check; without it an infinite component yields a NaN, and a NaN
            // in a sort key orders nothing.
            Metric::Cosine => {
                let scale = squared_norm(a).sqrt() * squared_norm(b).sqrt();
                if scale == 0.0 || !scale.is_finite() {
                    return 0.0;
                }
                kernel::dot(a, b) / scale
            }
            Metric::Euclidean => -kernel::squared_euclidean(a, b),
            Metric::NegativeDot => kernel::dot(a, b),
        }
    }
}

/// Euclidean norm of a vector.
pub fn norm<T: Element>(v: &[T]) -> f64 {
    squared_norm(v).sqrt()
}

/// Squared euclidean norm. Cheaper than [`norm`], and what [`NORM_THRESHOLD`]
/// is compared against.
pub fn squared_norm<T: Element>(v: &[T]) -> f64 {
    kernel::dot(v, v)
}

/// Scale `v` to unit length in place.
///
/// Returns `false` and leaves `v` untouched when there is no direction to
/// preserve, or when the element width cannot hold one: an integral width
/// would truncate `[3, 4]` to `[0, 0]`. The index rejects directionless vectors
/// at insert rather than storing a point it can never rank, and converts an
/// integral vector to its stored width before it gets here.
pub fn normalize<T: Element>(v: &mut [T]) -> bool {
    if T::IS_INTEGRAL {
        return false;
    }
    let sum_sq = squared_norm(v);
    if !usable_norm(sum_sq) {
        return false;
    }
    let norm = sum_sq.sqrt();
    for x in v.iter_mut() {
        *x = T::narrow(x.widen() / norm);
    }
    true
}

fn usable_norm(squared: f64) -> bool {
    squared.is_finite() && squared >= NORM_THRESHOLD
}

fn cosine_distance<T: Element>(a: &[T], b: &[T]) -> f64 {
    let sq_a = squared_norm(a);
    let sq_b = squared_norm(b);
    if !usable_norm(sq_a) || !usable_norm(sq_b) {
        return MAX_COSINE_DISTANCE;
    }
    let cos = kernel::dot(a, b) / (sq_a.sqrt() * sq_b.sqrt());
    // Rounding can push this a hair outside [-1, 1]; clamp so the distance
    // stays inside [0, 2].
    ordered_cosine(1.0 - cos.clamp(-1.0, 1.0))
}

fn ordered_cosine(distance: f64) -> f64 {
    if distance.is_nan() {
        MAX_COSINE_DISTANCE
    } else {
        distance
    }
}

/// Only reachable from non-finite input, where `inf * 0` or `inf - inf` means
/// "no usable distance".
fn ordered(distance: f64) -> f64 {
    if distance.is_nan() {
        f64::INFINITY
    } else {
        distance
    }
}
