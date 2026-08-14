//! Lloyd's k-means with k-means++ seeding, over a resident sample.

use crate::vector::core::squared_euclidean;
use crate::vector::engine::ann::{Centroids, Clusterer, Fit};

/// Lloyd's algorithm with k-means++ seeding.
#[derive(Debug, Clone, Copy)]
pub struct KMeans {
    pub max_iterations: usize,
    pub seed: u64,
}

impl KMeans {
    pub fn new(seed: u64) -> Self {
        Self {
            max_iterations: 25,
            seed,
        }
    }
}

impl Clusterer for KMeans {
    fn fit(&self, sample: &[f32], dimensions: usize, k: usize) -> (Centroids, Fit) {
        train(sample, dimensions, k, self.max_iterations, self.seed)
    }
}

/// Trains `k` centroids over `sample`, a flat `n * dimensions` buffer.
///
/// `k` is clamped to the sample size: asking for more centroids than points
/// would leave empty clusters whose contents are arbitrary.
pub fn train(
    sample: &[f32],
    dimensions: usize,
    k: usize,
    max_iterations: usize,
    seed: u64,
) -> (Centroids, Fit) {
    let n = sample.len().checked_div(dimensions).unwrap_or(0);
    let k = k.min(n).max(1);

    if n == 0 {
        return (
            Centroids {
                k: 0,
                dimensions,
                values: Vec::new(),
            },
            Fit {
                k: 0,
                iterations: 0,
                inertia: 0.0,
            },
        );
    }

    let point = |i: usize| &sample[i * dimensions..(i + 1) * dimensions];
    let mut rng = Rng(seed);
    let mut centroids = seed_plus_plus(sample, dimensions, n, k, &mut rng);

    let mut assignment = vec![usize::MAX; n];
    let mut iterations = 0;
    for _ in 0..max_iterations {
        iterations += 1;
        let mut moved = false;

        for (i, slot) in assignment.iter_mut().enumerate() {
            let (nearest, _) = centroids.nearest(point(i));
            if *slot != nearest {
                *slot = nearest;
                moved = true;
            }
        }

        let mut sums = vec![0.0f64; k * dimensions];
        let mut counts = vec![0usize; k];
        for (i, &c) in assignment.iter().enumerate() {
            counts[c] += 1;
            let at = c * dimensions;
            for (d, value) in point(i).iter().enumerate() {
                sums[at + d] += *value as f64;
            }
        }

        for (c, count) in counts.iter().enumerate() {
            if *count == 0 {
                // An empty cluster contributes nothing and would stay empty.
                // Reseed it on the point furthest from its own centroid.
                let mut worst = (0usize, -1.0f64);
                for (i, &owner) in assignment.iter().enumerate() {
                    let d = squared_euclidean(point(i), centroids.get(owner));
                    if d > worst.1 {
                        worst = (i, d);
                    }
                }
                let at = c * dimensions;
                centroids.values[at..at + dimensions].copy_from_slice(point(worst.0));
                assignment[worst.0] = c;
                moved = true;
                continue;
            }
            let at = c * dimensions;
            let divisor = *count as f64;
            for d in 0..dimensions {
                centroids.values[at + d] = (sums[at + d] / divisor) as f32;
            }
        }

        if !moved {
            break;
        }
    }

    let inertia = (0..n)
        .map(|i| squared_euclidean(point(i), centroids.get(centroids.nearest(point(i)).0)))
        .sum::<f64>()
        / n as f64;

    (
        centroids,
        Fit {
            k,
            iterations,
            inertia,
        },
    )
}

/// k-means++: each seed after the first is drawn with probability proportional
/// to its squared distance from the nearest seed already chosen. Uniform
/// seeding routinely puts two seeds in one cluster and leaves another empty.
fn seed_plus_plus(
    sample: &[f32],
    dimensions: usize,
    n: usize,
    k: usize,
    rng: &mut Rng,
) -> Centroids {
    let point = |i: usize| &sample[i * dimensions..(i + 1) * dimensions];
    let mut values = Vec::with_capacity(k * dimensions);

    let first = (rng.next_u64() % n as u64) as usize;
    values.extend_from_slice(point(first));

    let mut closest: Vec<f64> = (0..n)
        .map(|i| squared_euclidean(point(i), point(first)))
        .collect();

    for chosen in 1..k {
        let total: f64 = closest.iter().sum();
        let picked = if total <= 0.0 || !total.is_finite() {
            // Every remaining point coincides with a seed; any index will do.
            (rng.next_u64() % n as u64) as usize
        } else {
            let mut target = rng.next_unit() * total;
            let mut picked = n - 1;
            for (i, weight) in closest.iter().enumerate() {
                target -= weight;
                if target <= 0.0 {
                    picked = i;
                    break;
                }
            }
            picked
        };

        values.extend_from_slice(point(picked));
        let at = chosen * dimensions;
        for (i, nearest) in closest.iter_mut().enumerate() {
            let d = squared_euclidean(point(i), &values[at..at + dimensions]);
            if d < *nearest {
                *nearest = d;
            }
        }
    }

    Centroids {
        k,
        dimensions,
        values,
    }
}

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}
