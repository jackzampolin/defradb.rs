//! Every SIMD tier is held to the scalar tier's answer, at both element widths,
//! to a machine-epsilon bound rather than a hand-picked tolerance. Which tiers
//! run depends on the machine: `Tier::is_available` decides, and every failure
//! message names the tier it came from.

use db_index::vector::core::{dot, squared_euclidean, Element, Tier, ALL_TIERS};

fn live_tiers() -> Vec<Tier> {
    ALL_TIERS
        .iter()
        .copied()
        .filter(|tier| tier.is_available())
        .collect()
}

/// Sweeps are expressed as multiples of this, so they stay correct if a wider
/// tier is added.
fn widest_lanes() -> usize {
    live_tiers()
        .iter()
        .map(|tier| tier.lanes())
        .max()
        .expect("the scalar tier is always live")
}

/// Error bound for a length-`n` reduction accumulated in `f64`.
///
/// Each of the `n` products can round by up to half an ULP, and so can each
/// addition folding them together: `(n + 1) * u * sum|terms|` with
/// `u = EPSILON / 2`. Using `EPSILON` outright leaves a factor of two in hand,
/// which is what the FMA-less SSE2 and `simd128` tiers spend on their extra
/// rounding. Vectorising changes *which* roundings happen, not this bound.
fn reduction_epsilon(terms: impl Iterator<Item = f64>, n: usize) -> f64 {
    let magnitude: f64 = terms.map(f64::abs).sum();
    ((n + 1) as f64) * f64::EPSILON * magnitude.max(f64::MIN_POSITIVE)
}

fn dot_epsilon<T: Element>(a: &[T], b: &[T]) -> f64 {
    reduction_epsilon(a.iter().zip(b).map(|(p, q)| p.widen() * q.widen()), a.len())
}

fn squared_euclidean_epsilon<T: Element>(a: &[T], b: &[T]) -> f64 {
    reduction_epsilon(
        a.iter().zip(b).map(|(p, q)| {
            let d = p.widen() - q.widen();
            d * d
        }),
        a.len(),
    )
}

/// Straddling every lane width in play (2 for SSE2, NEON and `simd128`, 4 for
/// AVX2, 8 for AVX-512), up to a realistic embedding dimension.
const LENGTHS: [usize; 17] = [0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 33, 64, 129, 1536];

fn vectors<T: Element>() -> Vec<(Vec<T>, Vec<T>)> {
    LENGTHS
        .into_iter()
        .map(|n| {
            let a = (0..n).map(|i| T::narrow((i as f64) * 0.5 - 3.0)).collect();
            let b = (0..n).map(|i| T::narrow(7.0 - (i as f64) * 0.25)).collect();
            (a, b)
        })
        .collect()
}

fn check_against_scalar<T: Element>(width: &str) {
    for (a, b) in vectors::<T>() {
        let want_dot = Tier::Scalar.dot(&a, &b).expect("scalar is always live");
        let want_sqe = Tier::Scalar
            .squared_euclidean(&a, &b)
            .expect("scalar is always live");
        let eps_dot = dot_epsilon(&a, &b);
        let eps_sqe = squared_euclidean_epsilon(&a, &b);

        for tier in live_tiers() {
            let got = tier.dot(&a, &b).expect("filtered to live tiers");
            assert!(
                (want_dot - got).abs() <= eps_dot,
                "{width} {} dot len={}: scalar={want_dot} got={got} eps={eps_dot}",
                tier.name(),
                a.len()
            );

            let got = tier
                .squared_euclidean(&a, &b)
                .expect("filtered to live tiers");
            assert!(
                (want_sqe - got).abs() <= eps_sqe,
                "{width} {} sq_euclid len={}: scalar={want_sqe} got={got} eps={eps_sqe}",
                tier.name(),
                a.len()
            );
        }
    }
}

#[test]
fn f32_every_live_tier_matches_scalar_within_machine_epsilon() {
    check_against_scalar::<f32>("f32");
}

#[test]
fn f64_every_live_tier_matches_scalar_within_machine_epsilon() {
    check_against_scalar::<f64>("f64");
}

/// Lengths that are not lane multiples must still be exact. Sums of small
/// integers are exact in `f64`, so a lane the scalar tail forgets shows up as
/// an integer shortfall rather than as rounding.
#[test]
fn remainders_are_not_dropped() {
    fn check<T: Element>(width: &str) {
        for n in 0..=(4 * widest_lanes()) {
            let ones = vec![T::narrow(1.0); n];
            let twos = vec![T::narrow(2.0); n];
            for tier in live_tiers() {
                assert_eq!(
                    tier.dot(&ones, &ones),
                    Some(n as f64),
                    "{width} {}: dot dropped a remainder at len {n}",
                    tier.name()
                );
                assert_eq!(
                    tier.squared_euclidean(&ones, &twos),
                    Some(n as f64),
                    "{width} {}: sq_euclid dropped a remainder at len {n}",
                    tier.name()
                );
            }
        }
    }
    check::<f32>("f32");
    check::<f64>("f64");
}

/// Unequal lengths are well-defined, not an error and not a panic: a distance
/// is defined over the shared prefix.
#[test]
fn unequal_lengths_use_the_shared_prefix_only() {
    fn check<T: Element>(width: &str) {
        let span = 4 * widest_lanes();
        let long = vec![T::narrow(1.0); span];
        for n in 0..=span {
            let short = vec![T::narrow(3.0); n];
            for tier in live_tiers() {
                assert_eq!(
                    tier.dot(&long, &short),
                    Some(3.0 * n as f64),
                    "{width} {}: dot must consume exactly the shared prefix ({n} of {span})",
                    tier.name()
                );
                assert_eq!(
                    tier.dot(&short, &long),
                    Some(3.0 * n as f64),
                    "{width} {}: argument order must not matter ({n} of {span})",
                    tier.name()
                );
                // (3 - 1)^2 per shared component.
                assert_eq!(
                    tier.squared_euclidean(&long, &short),
                    Some(4.0 * n as f64),
                    "{width} {}: sq_euclid must consume exactly the shared prefix",
                    tier.name()
                );
            }
        }
    }
    check::<f32>("f32");
    check::<f64>("f64");
}

/// Hand-computed, so the suite does not only agree with itself.
#[test]
fn known_values_are_exact() {
    // 1*4 + 2*5 + 3*6 = 32 and 3^2 * 3 = 27, both exactly representable.
    assert_eq!(dot(&[1.0f32, 2.0, 3.0], &[4.0f32, 5.0, 6.0]), 32.0);
    assert_eq!(dot(&[1.0f64, 2.0, 3.0], &[4.0f64, 5.0, 6.0]), 32.0);
    assert_eq!(
        squared_euclidean(&[1.0f32, 2.0, 3.0], &[4.0f32, 5.0, 6.0]),
        27.0
    );
    assert_eq!(
        squared_euclidean(&[1.0f64, 2.0, 3.0], &[4.0f64, 5.0, 6.0]),
        27.0
    );
}

/// The integer widths run on the scalar tier, but they are part of the same
/// API and must agree with the float widths on the same values.
#[test]
fn integer_widths_agree_with_the_float_widths() {
    for n in [0usize, 1, 3, 8, 17, 64] {
        let a: Vec<i64> = (0..n as i64).map(|i| i - 3).collect();
        let b: Vec<i64> = (0..n as i64).map(|i| 7 - i * 2).collect();
        let a32: Vec<i32> = a.iter().map(|x| *x as i32).collect();
        let b32: Vec<i32> = b.iter().map(|x| *x as i32).collect();
        let af: Vec<f64> = a.iter().map(|x| *x as f64).collect();
        let bf: Vec<f64> = b.iter().map(|x| *x as f64).collect();

        // Small integers are exact in f64, so these are equalities, not bounds.
        assert_eq!(dot(&a, &b), dot(&af, &bf), "i64 dot at len {n}");
        assert_eq!(dot(&a32, &b32), dot(&af, &bf), "i32 dot at len {n}");
        assert_eq!(
            squared_euclidean(&a, &b),
            squared_euclidean(&af, &bf),
            "i64 sq_euclid at len {n}"
        );
        assert_eq!(
            squared_euclidean(&a32, &b32),
            squared_euclidean(&af, &bf),
            "i32 sq_euclid at len {n}"
        );
    }
}

/// Narrowing out of the accumulator saturates rather than wrapping, so an
/// out-of-range value cannot become an unrelated one.
#[test]
fn narrowing_to_an_integer_saturates() {
    assert_eq!(i32::narrow(f64::from(i32::MAX) * 4.0), i32::MAX);
    assert_eq!(i32::narrow(f64::from(i32::MIN) * 4.0), i32::MIN);
    assert_eq!(i64::narrow(f64::INFINITY), i64::MAX);
    assert_eq!(i32::narrow(2.9), 2);
    assert_eq!(i32::narrow(-2.9), -2);
}

#[test]
fn empty_vectors_are_zero() {
    for tier in live_tiers() {
        assert_eq!(tier.dot::<f32>(&[], &[]), Some(0.0), "{}", tier.name());
        assert_eq!(tier.dot::<f64>(&[], &[]), Some(0.0), "{}", tier.name());
        assert_eq!(
            tier.squared_euclidean::<f32>(&[], &[]),
            Some(0.0),
            "{}",
            tier.name()
        );
        assert_eq!(
            tier.squared_euclidean::<f64>(&[], &[]),
            Some(0.0),
            "{}",
            tier.name()
        );
    }
}

#[test]
fn identical_vectors_have_zero_distance_exactly() {
    fn check<T: Element>(width: &str) {
        let v: Vec<T> = (0..97).map(|i| T::narrow((i as f64) * 1.5)).collect();
        for tier in live_tiers() {
            assert_eq!(
                tier.squared_euclidean(&v, &v),
                Some(0.0),
                "{width} {}: a - a is exactly 0 in every lane, so no rounding can occur",
                tier.name()
            );
        }
    }
    check::<f32>("f32");
    check::<f64>("f64");
}

/// The free functions must route to the widest tier this machine can run.
#[test]
fn the_active_tier_is_the_widest_available_one() {
    let active = Tier::active();
    assert!(
        active.is_available(),
        "{} is not runnable here",
        active.name()
    );

    let widest = live_tiers()
        .into_iter()
        .last()
        .expect("the scalar tier is always live");
    assert_eq!(
        active,
        widest,
        "active tier {} is narrower than the available {}",
        active.name(),
        widest.name()
    );

    let a: Vec<f32> = (0..129).map(|i| i as f32 * 0.25).collect();
    let b: Vec<f32> = (0..129).map(|i| 4.0 - i as f32 * 0.125).collect();
    assert_eq!(dot(&a, &b), active.dot(&a, &b).unwrap());
    assert_eq!(
        squared_euclidean(&a, &b),
        active.squared_euclidean(&a, &b).unwrap()
    );
}

/// A tier the machine cannot run refuses rather than quietly substituting a
/// narrower one. Passes trivially on a machine that has every tier.
#[test]
fn an_unavailable_tier_refuses_rather_than_downgrading() {
    let absent: Vec<Tier> = ALL_TIERS
        .iter()
        .copied()
        .filter(|tier| !tier.is_available())
        .collect();
    for tier in absent {
        assert_eq!(
            tier.dot(&[1.0f32, 2.0], &[3.0f32, 4.0]),
            None,
            "{} is unavailable and must not compute",
            tier.name()
        );
        assert_eq!(
            tier.squared_euclidean(&[1.0f64, 2.0], &[3.0f64, 4.0]),
            None,
            "{} is unavailable and must not compute",
            tier.name()
        );
    }
}

/// `ALL_TIERS` must widen monotonically: that ordering is what makes "the last
/// available one wins" the right selection rule.
#[test]
fn lane_counts_widen_monotonically() {
    assert_eq!(Tier::Scalar.lanes(), 1);
    let mut previous = 0;
    for tier in ALL_TIERS {
        let lanes = tier.lanes();
        assert!(
            lanes.is_power_of_two(),
            "{} has {lanes} lanes, not a power of two",
            tier.name()
        );
        assert!(
            lanes > previous,
            "{} has {lanes} lanes, not wider than the previous {previous}",
            tier.name()
        );
        previous = lanes;
    }
}
