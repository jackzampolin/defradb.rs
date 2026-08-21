//! Product quantization: codes, reconstruction, and distances from codes.

use db::index::vector::core::squared_euclidean;
use db::index::vector::engine::ann::Quantizer;
use db::index::vector::quantize::KMeans;
use db::index::vector::quantize::ProductQuantizer;

const DIMENSIONS: usize = 16;
const M: usize = 4;

fn sample(count: usize, dimensions: usize, seed: u64) -> Vec<f32> {
    crate::support::Corpus::new(seed)
        .vectors(count, dimensions)
        .into_iter()
        .flatten()
        .collect()
}

fn trained(seed: u64) -> (ProductQuantizer, Vec<f32>) {
    let flat = sample(600, DIMENSIONS, seed);
    let pq = ProductQuantizer::train(&KMeans::new(seed), &flat, DIMENSIONS, M)
        .expect("a divisible width trains");
    (pq, flat)
}

#[test]
fn a_code_is_one_byte_per_subquantizer() {
    let (pq, _) = trained(1);
    assert_eq!(pq.code_len(), M);
    assert_eq!(pq.dimensions(), DIMENSIONS);
    assert_eq!(pq.books().len(), M);
    for book in pq.books() {
        assert_eq!(book.dimensions, DIMENSIONS / M);
    }
}

/// `m` must divide the width, or one subspace would be a different shape and
/// its codebook incomparable with the others.
#[test]
fn an_indivisible_width_is_refused() {
    let flat = sample(100, 10, 2);
    let err = ProductQuantizer::train(&KMeans::new(2), &flat, 10, 4).unwrap_err();
    assert!(
        err.to_string().contains("divide"),
        "the error must say why, got: {err}"
    );
    assert!(ProductQuantizer::train(&KMeans::new(2), &flat, 0, 4).is_err());
    assert!(ProductQuantizer::train(&KMeans::new(2), &flat, 10, 0).is_err());
}

/// Reconstruction is lossy, but it must be much closer to the original than a
/// competing vector is. Otherwise the codes carry no usable signal.
#[test]
fn reconstruction_is_closer_than_a_stranger() {
    let (pq, flat) = trained(3);
    let mut code = vec![0u8; pq.code_len()];
    let mut decoded = vec![0.0f32; DIMENSIONS];

    let mut better = 0usize;
    let total = 200;
    for i in 0..total {
        let vector = &flat[i * DIMENSIONS..(i + 1) * DIMENSIONS];
        let stranger = &flat[(i + 300) * DIMENSIONS..(i + 301) * DIMENSIONS];
        pq.encode(vector, &mut code);
        pq.decode(&code, &mut decoded);

        if squared_euclidean(vector, &decoded) < squared_euclidean(vector, stranger) {
            better += 1;
        }
    }
    assert_eq!(
        better,
        total,
        "reconstruction was worse than an unrelated vector for {} of {total}",
        total - better
    );
}

/// The whole point of the lookup table: the distance it yields must equal the
/// distance to the reconstruction, or the scan is ranking on something else.
#[test]
fn the_lookup_table_agrees_with_the_reconstruction() {
    let (pq, flat) = trained(4);
    let mut code = vec![0u8; pq.code_len()];
    let mut decoded = vec![0.0f32; DIMENSIONS];

    let query = &flat[7 * DIMENSIONS..8 * DIMENSIONS];
    let table = pq.distance_table(query);
    assert_eq!(table.len(), M * 256);

    for i in 0..100 {
        let vector = &flat[i * DIMENSIONS..(i + 1) * DIMENSIONS];
        pq.encode(vector, &mut code);
        pq.decode(&code, &mut decoded);

        let from_table = pq.distance(&table, &code);
        let exact = squared_euclidean(query, &decoded);
        assert!(
            (from_table - exact).abs() <= 1e-4 * exact.max(1.0),
            "table said {from_table}, reconstruction says {exact}"
        );
    }
}

/// Ranking is what the index uses. Against its own codes, the quantizer must
/// put a vector nearest itself.
#[test]
fn a_vector_ranks_nearest_its_own_code() {
    let (pq, flat) = trained(5);
    let mut codes = Vec::new();
    let mut code = vec![0u8; pq.code_len()];
    for i in 0..200 {
        pq.encode(&flat[i * DIMENSIONS..(i + 1) * DIMENSIONS], &mut code);
        codes.push(code.clone());
    }

    let mut correct = 0usize;
    for i in 0..200 {
        let query = &flat[i * DIMENSIONS..(i + 1) * DIMENSIONS];
        let table = pq.distance_table(query);
        let nearest = codes
            .iter()
            .enumerate()
            .min_by(|a, b| {
                pq.distance(&table, a.1)
                    .total_cmp(&pq.distance(&table, b.1))
            })
            .unwrap()
            .0;
        if nearest == i {
            correct += 1;
        }
    }
    // Two vectors can share a code, so this is not required to be perfect; it
    // must be overwhelming or the codebooks are not separating anything.
    assert!(
        correct >= 190,
        "only {correct}/200 vectors ranked nearest their own code"
    );
}

/// More subquantizers means finer codes, so error must fall as `m` rises.
#[test]
fn error_falls_as_subquantizers_rise() {
    let flat = sample(600, DIMENSIONS, 6);
    let mut previous = f64::INFINITY;

    for m in [1usize, 2, 4, 8, 16] {
        let pq = ProductQuantizer::train(&KMeans::new(6), &flat, DIMENSIONS, m).unwrap();
        let mut code = vec![0u8; pq.code_len()];
        let mut decoded = vec![0.0f32; DIMENSIONS];

        let error: f64 = (0..200)
            .map(|i| {
                let vector = &flat[i * DIMENSIONS..(i + 1) * DIMENSIONS];
                pq.encode(vector, &mut code);
                pq.decode(&code, &mut decoded);
                squared_euclidean(vector, &decoded)
            })
            .sum::<f64>()
            / 200.0;

        assert!(
            error <= previous + 1e-6,
            "error rose at m={m}: {error} after {previous}"
        );
        previous = error;
    }
}

/// A quantizer rebuilt from stored codebooks must encode identically, or a
/// reopened index would disagree with the one that wrote it.
#[test]
fn a_quantizer_rebuilt_from_its_codebooks_is_identical() {
    let (pq, flat) = trained(8);
    let rebuilt = ProductQuantizer::from_books(DIMENSIONS, pq.books().to_vec()).unwrap();
    assert_eq!(pq, rebuilt);

    let mut a = vec![0u8; pq.code_len()];
    let mut b = vec![0u8; rebuilt.code_len()];
    for i in 0..100 {
        let vector = &flat[i * DIMENSIONS..(i + 1) * DIMENSIONS];
        pq.encode(vector, &mut a);
        rebuilt.encode(vector, &mut b);
        assert_eq!(a, b);
    }
}

#[test]
fn mismatched_codebooks_are_refused() {
    let (pq, _) = trained(9);
    assert!(ProductQuantizer::from_books(DIMENSIONS, Vec::new()).is_err());
    assert!(ProductQuantizer::from_books(DIMENSIONS + 1, pq.books().to_vec()).is_err());
}

/// Compression is the reason this kind exists, so it is asserted rather than
/// assumed.
#[test]
fn a_code_is_far_smaller_than_the_vector() {
    let flat = sample(400, 768, 10);
    let pq = ProductQuantizer::train(&KMeans::new(10), &flat, 768, 96).unwrap();

    let vector_bytes = 768 * size_of::<f32>();
    let code_bytes = pq.code_len();
    assert_eq!(code_bytes, 96);
    assert_eq!(vector_bytes / code_bytes, 32);
}
