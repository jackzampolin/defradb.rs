//! The bounded training sample.

use db_index::vector::engine::ann::Sampler;
use db_index::vector::quantize::Reservoir;

mod common;

const DIMENSIONS: usize = 8;

fn budget_for(vectors: usize) -> usize {
    vectors * DIMENSIONS * size_of::<f32>()
}

#[test]
fn the_sample_never_exceeds_its_byte_budget() {
    let budget = budget_for(100);
    let mut reservoir = Reservoir::new(DIMENSIONS, budget, 0x5EED);
    let mut corpus = common::Corpus::new(0x5EED);

    for vector in corpus.vectors(50_000, DIMENSIONS) {
        reservoir.offer(&vector);
        assert!(
            reservoir.resident_bytes() <= budget,
            "resident {} exceeded budget {budget}",
            reservoir.resident_bytes()
        );
    }

    assert_eq!(reservoir.len(), 100);
    assert_eq!(reservoir.seen(), 50_000);
}

/// The budget bounds bytes, so the vector count it allows must fall as the
/// width rises. A count-based cap would not.
#[test]
fn the_capacity_falls_as_the_width_rises() {
    let budget = 64 * 1024;
    let narrow = Reservoir::new(16, budget, 1);
    let wide = Reservoir::new(768, budget, 1);

    assert_eq!(narrow.capacity(), budget / (16 * 4));
    assert_eq!(wide.capacity(), budget / (768 * 4));
    assert!(wide.capacity() < narrow.capacity());
}

/// A budget too small for one vector still holds one, or training would see an
/// empty sample on a non-empty stream.
#[test]
fn a_budget_below_one_vector_still_holds_one() {
    let mut reservoir = Reservoir::new(DIMENSIONS, 1, 7);
    assert_eq!(reservoir.capacity(), 1);
    reservoir.offer(&[1.0; DIMENSIONS]);
    assert_eq!(reservoir.len(), 1);
}

/// Below the budget the sample is the stream, exactly.
#[test]
fn a_short_stream_is_kept_whole() {
    let mut reservoir = Reservoir::new(DIMENSIONS, budget_for(1000), 3);
    let mut corpus = common::Corpus::new(11);
    let vectors = corpus.vectors(40, DIMENSIONS);
    for vector in &vectors {
        reservoir.offer(vector);
    }

    assert_eq!(reservoir.len(), 40);
    let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
    assert_eq!(reservoir.as_flat(), flat.as_slice());
}

#[test]
fn an_empty_stream_samples_nothing() {
    let reservoir = Reservoir::new(DIMENSIONS, budget_for(10), 5);
    assert!(reservoir.is_empty());
    assert_eq!(reservoir.seen(), 0);
    assert!(reservoir.as_flat().is_empty());
}

/// A wrong-width vector would corrupt the flat buffer, so it is refused rather
/// than truncated or padded.
#[test]
fn a_wrong_width_vector_is_ignored() {
    let mut reservoir = Reservoir::new(DIMENSIONS, budget_for(10), 9);
    reservoir.offer(&[1.0; DIMENSIONS - 1]);
    reservoir.offer(&[1.0; DIMENSIONS + 1]);
    assert!(reservoir.is_empty());
    assert_eq!(reservoir.seen(), 0);

    reservoir.offer(&[1.0; DIMENSIONS]);
    assert_eq!(reservoir.len(), 1);
}

#[test]
fn the_same_seed_draws_the_same_sample() {
    let draw = |seed: u64| {
        let mut reservoir = Reservoir::new(DIMENSIONS, budget_for(50), seed);
        let mut corpus = common::Corpus::new(21);
        for vector in corpus.vectors(5_000, DIMENSIONS) {
            reservoir.offer(&vector);
        }
        reservoir.into_flat()
    };

    assert_eq!(draw(42), draw(42));
    assert_ne!(draw(42), draw(43));
}

/// Reservoir sampling is uniform over the stream, so sampled positions must
/// spread across it rather than clustering at whichever end the algorithm
/// favours. Asserting on two *specific* vectors would be a coin flip: each
/// survives with probability `capacity / stream`, here 1%.
#[test]
fn the_sample_is_drawn_from_the_whole_stream() {
    const STREAM: usize = 2_000;
    const CAPACITY: usize = 20;
    const DRAWS: u64 = 40;

    let mut corpus = common::Corpus::new(33);
    let vectors = corpus.vectors(STREAM, DIMENSIONS);
    let position = |chunk: &[f32]| vectors.iter().position(|v| v.as_slice() == chunk);

    let mut positions = Vec::new();
    for seed in 0..DRAWS {
        let mut reservoir = Reservoir::new(DIMENSIONS, budget_for(CAPACITY), seed);
        for vector in &vectors {
            reservoir.offer(vector);
        }
        positions.extend(reservoir.as_flat().chunks(DIMENSIONS).filter_map(position));
    }

    assert_eq!(positions.len(), CAPACITY * DRAWS as usize);
    let first_decile = positions.iter().filter(|p| **p < STREAM / 10).count();
    let last_decile = positions
        .iter()
        .filter(|p| **p >= STREAM - STREAM / 10)
        .count();

    // ~80 expected in each decile across 800 samples; zero would mean the
    // sampler cannot reach that end of the stream at all.
    assert!(first_decile > 0, "nothing sampled from the head");
    assert!(last_decile > 0, "nothing sampled from the tail");
}
