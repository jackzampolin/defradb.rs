//! Metrics must be distances (smaller is closer), total (no error, no panic,
//! for any input) and orderable (never NaN), at both element widths.

use db_index::vector::core::{
    norm, normalize, squared_norm, Element, Metric, MAX_COSINE_DISTANCE, NORM_THRESHOLD,
};

const METRICS: [Metric; 3] = [Metric::Cosine, Metric::Euclidean, Metric::NegativeDot];

/// Runs `check` for the widths that can hold a scaled unit vector. Anything
/// about normalization belongs here rather than in [`for_every_width`].
macro_rules! for_float_widths {
    ($check:ident) => {{
        $check::<f32>("f32");
        $check::<f64>("f64");
    }};
}

/// Runs `check` for every physical width a vector field can hold.
macro_rules! for_every_width {
    ($check:ident) => {{
        $check::<f32>("f32");
        $check::<f64>("f64");
        $check::<i32>("i32");
        $check::<i64>("i64");
    }};
}

/// Distances accumulate in `f64`, but `f32` inputs carry only 24 bits of
/// mantissa, so the bound is set by the input width. Round-tripping a value
/// through `T` learns its epsilon without naming it.
fn tolerance<T: Element>() -> f64 {
    let one_ulp = T::narrow(1.0 + f64::from(f32::EPSILON)).widen() - 1.0;
    let epsilon = if one_ulp > 0.0 { one_ulp } else { f64::EPSILON };
    // A few operations' worth of headroom over a single rounding.
    8.0 * epsilon
}

fn vector<T: Element>(values: &[f64]) -> Vec<T> {
    values.iter().map(|v| T::narrow(*v)).collect()
}

/// Hand-computed cosine values. This is the test the adapted source would have
/// failed: its cosine returned NaN for every non-zero vector.
fn cosine_oracle<T: Element>(width: &str) {
    let tol = tolerance::<T>();
    let x = vector::<T>(&[1.0, 0.0, 0.0]);
    let y = vector::<T>(&[0.0, 1.0, 0.0]);
    let neg_x = vector::<T>(&[-1.0, 0.0, 0.0]);
    let long_x = vector::<T>(&[5.0, 0.0, 0.0]);

    let d = Metric::Cosine.distance(&x, &x);
    assert!(d.abs() < tol, "{width}: identical direction -> 0, got {d}");
    let d = Metric::Cosine.distance(&x, &y);
    assert!((d - 1.0).abs() < tol, "{width}: orthogonal -> 1, got {d}");
    let d = Metric::Cosine.distance(&x, &neg_x);
    assert!((d - 2.0).abs() < tol, "{width}: opposite -> 2, got {d}");
    let d = Metric::Cosine.distance(&x, &long_x);
    assert!(d.abs() < tol, "{width}: magnitude must not matter, got {d}");

    // 45 degrees -> 1 - cos45. Whole-number components, so every width holds
    // the inputs exactly even though the answer is irrational.
    let d = Metric::Cosine.distance(&vector::<T>(&[1.0, 0.0]), &vector::<T>(&[1.0, 1.0]));
    let want = 1.0 - std::f64::consts::FRAC_1_SQRT_2;
    assert!((d - want).abs() < tol, "{width}: 45 degrees, got {d}");
}

#[test]
fn cosine_matches_hand_computed_oracle() {
    for_every_width!(cosine_oracle);
}

/// A nearest-neighbour search orders distances, and NaN is not orderable.
#[test]
fn no_metric_produces_nan() {
    fn check<T: Element>(width: &str) {
        let cases: [(Vec<T>, Vec<T>); 8] = [
            (vector(&[1.0, 2.0, 3.0]), vector(&[4.0, 5.0, 6.0])),
            (vector(&[0.0, 0.0, 0.0]), vector(&[1.0, 2.0, 3.0])),
            (vector(&[0.0, 0.0, 0.0]), vector(&[0.0, 0.0, 0.0])),
            (vector(&[1.0, 0.0, 0.0]), vector(&[0.0, 0.0, 0.0])),
            (vector(&[f64::INFINITY, 0.0, 0.0]), vector(&[0.0, 0.0, 0.0])),
            (
                vector(&[f64::INFINITY, 0.0, 0.0]),
                vector(&[f64::INFINITY, 0.0, 0.0]),
            ),
            (vector(&[f64::NAN, 1.0, 2.0]), vector(&[1.0, 2.0, 3.0])),
            // Unequal lengths: a shared prefix, and a longer tail ignored.
            (vector(&[1.0, 2.0, 3.0, 4.0]), vector(&[1.0, 2.0])),
        ];
        for metric in METRICS {
            for (a, b) in &cases {
                let d = metric.distance(a, b);
                assert!(!d.is_nan(), "{width}: {metric:?} produced NaN");
                let d = metric.distance_normalized(a, b);
                assert!(
                    !d.is_nan(),
                    "{width}: {metric:?} normalized path produced NaN"
                );
            }
        }
    }
    for_every_width!(check);
}

#[test]
fn cosine_distance_stays_within_its_range() {
    fn check<T: Element>(width: &str) {
        for (a, b) in [
            (vec![1.0, 0.0], vec![1.0, 0.0]),
            (vec![1.0, 0.0], vec![-1.0, 0.0]),
            (vec![3.0, 4.0], vec![-4.0, 3.0]),
            (vec![0.0, 0.0], vec![1.0, 0.0]),
            (vec![1.0, 2.0, 3.0], vec![1.0, 2.0]),
        ] {
            let d = Metric::Cosine.distance(&vector::<T>(&a), &vector::<T>(&b));
            assert!(
                (0.0..=MAX_COSINE_DISTANCE).contains(&d),
                "{width}: {d} outside [0, {MAX_COSINE_DISTANCE}]"
            );
        }
    }
    for_every_width!(check);
}

/// The normalized fast path must agree with the general one.
#[test]
fn normalized_path_agrees_with_general_path() {
    fn check<T: Element>(width: &str) {
        let mut a = vector::<T>(&[3.0, 4.0, 0.0]);
        let mut b = vector::<T>(&[1.0, 2.0, 2.0]);
        assert!(normalize(&mut a));
        assert!(normalize(&mut b));

        let general = Metric::Cosine.distance(&a, &b);
        let fast = Metric::Cosine.distance_normalized(&a, &b);
        assert!(
            (general - fast).abs() < tolerance::<T>(),
            "{width}: general={general} fast={fast}"
        );
    }
    for_float_widths!(check);
}

#[test]
fn normalize_yields_unit_length() {
    fn check<T: Element>(width: &str) {
        let tol = tolerance::<T>();
        let mut v = vector::<T>(&[3.0, 4.0]);
        assert!(normalize(&mut v));
        assert!((norm(&v) - 1.0).abs() < tol, "{width}: not unit length");
        assert!(
            (v[0].widen() - 0.6).abs() < tol && (v[1].widen() - 0.8).abs() < tol,
            "{width}: direction not preserved"
        );
    }
    for_float_widths!(check);
}

#[test]
fn normalize_refuses_a_vector_with_no_direction() {
    fn check<T: Element>(width: &str) {
        for values in [
            vec![0.0, 0.0, 0.0],
            vec![f64::INFINITY, 0.0],
            vec![f64::NAN, 1.0],
        ] {
            let mut v = vector::<T>(&values);
            let before: Vec<f64> = v.iter().map(|x| x.widen()).collect();
            assert!(!normalize(&mut v), "{width}: {values:?} has no direction");
            let after: Vec<f64> = v.iter().map(|x| x.widen()).collect();
            assert!(
                before
                    .iter()
                    .zip(&after)
                    .all(|(x, y)| x == y || (x.is_nan() && y.is_nan())),
                "{width}: {values:?} must be left untouched"
            );
        }
    }
    for_float_widths!(check);
}

/// A distance is `f64` whatever the element width, and it must be free to take
/// a value the element type could never hold. Integer vectors at 45 degrees are
/// still `1 - cos45` apart.
#[test]
fn a_distance_is_floating_point_whatever_the_element_width() {
    fn check<T: Element>(width: &str) {
        let tol = tolerance::<T>();

        let cosine = Metric::Cosine.distance(&vector::<T>(&[1.0, 0.0]), &vector::<T>(&[1.0, 1.0]));
        let want = 1.0 - std::f64::consts::FRAC_1_SQRT_2;
        assert!(
            (cosine - want).abs() < tol,
            "{width}: expected {want}, got {cosine}"
        );
        assert!(
            cosine.fract() != 0.0,
            "{width}: a 45 degree separation is not a whole number, got {cosine}"
        );

        let euclidean =
            Metric::Euclidean.distance(&vector::<T>(&[0.0, 0.0]), &vector::<T>(&[1.0, 1.0]));
        assert!(
            (euclidean - std::f64::consts::SQRT_2).abs() < tol,
            "{width}: expected sqrt(2), got {euclidean}"
        );

        // The norm of an integer vector is irrational just as often.
        let n = norm(&vector::<T>(&[1.0, 1.0]));
        assert!((n - std::f64::consts::SQRT_2).abs() < tol, "{width}: {n}");
    }
    for_every_width!(check);
}

/// An integral width cannot hold a scaled unit vector, so `normalize` must
/// refuse rather than truncate `[3, 4]` into `[0, 0]` and destroy the
/// direction it was asked to preserve.
#[test]
fn normalize_refuses_an_integral_width() {
    fn check<T: Element>(width: &str) {
        let mut v = vector::<T>(&[3.0, 4.0]);
        assert!(!normalize(&mut v), "{width}: must refuse");
        assert_eq!(
            v.iter().map(|x| x.widen()).collect::<Vec<_>>(),
            vec![3.0, 4.0],
            "{width}: must be left untouched"
        );
    }
    check::<i32>("i32");
    check::<i64>("i64");

    // The float widths still normalize.
    let mut v = vec![3.0f32, 4.0];
    assert!(normalize(&mut v));
}

/// The threshold is on the *squared* norm, matching Go's `normThreshold`: a
/// vector just under it is refused even though its norm is around 1e-16, which
/// comparing the norm itself would have accepted. `f64` only, since the values
/// are far below `f32`'s smallest normal.
#[test]
fn the_norm_threshold_is_on_the_squared_norm() {
    let just_under = (NORM_THRESHOLD / 2.0).sqrt();
    let mut v = vec![just_under, 0.0];
    assert!(
        squared_norm(&v) < NORM_THRESHOLD,
        "test setup: squared norm must sit under the threshold"
    );
    assert!(
        norm(&v) > NORM_THRESHOLD,
        "test setup: the norm itself must sit well above it, or this proves nothing"
    );
    assert!(!normalize(&mut v), "below the squared-norm threshold");

    let just_over = (NORM_THRESHOLD * 4.0).sqrt();
    let mut v = vec![just_over, 0.0];
    assert!(squared_norm(&v) > NORM_THRESHOLD);
    assert!(normalize(&mut v), "above the squared-norm threshold");
    assert!((norm(&v) - 1.0).abs() < 8.0 * f64::EPSILON);
}

#[test]
fn euclidean_matches_hand_computed_oracle() {
    fn check<T: Element>(width: &str) {
        // 3-4-5 triangle.
        let d = Metric::Euclidean.distance(&vector::<T>(&[0.0, 0.0]), &vector::<T>(&[3.0, 4.0]));
        assert!((d - 5.0).abs() < tolerance::<T>(), "{width}: got {d}");
    }
    for_every_width!(check);
}

/// Dot product is not a metric: it has no identity of indiscernibles, so a
/// vector is *not* its own nearest neighbour. A longer collinear vector has a
/// larger product and outranks the query's own twin.
#[test]
fn negative_dot_ranks_a_longer_collinear_vector_above_an_identical_one() {
    fn check<T: Element>(width: &str) {
        let query = vector::<T>(&[1.0, 0.0]);
        let longer = vector::<T>(&[10.0, 0.0]);
        let identical = vector::<T>(&[1.0, 0.0]);

        let d_longer = Metric::NegativeDot.distance(&query, &longer);
        let d_identical = Metric::NegativeDot.distance(&query, &identical);

        assert_eq!(d_longer, -10.0, "{width}: -dot([1,0],[10,0])");
        assert_eq!(d_identical, -1.0, "{width}: -dot([1,0],[1,0])");
        assert!(
            d_longer < d_identical,
            "{width}: the longer collinear vector must sort nearer than the \
             query's own twin: {d_longer} !< {d_identical}"
        );
    }
    for_every_width!(check);
}

/// The contrast that makes the above safe to rely on: under cosine a vector
/// *is* its own nearest, because magnitude is normalized away.
#[test]
fn cosine_ranks_an_identical_vector_nearest() {
    fn check<T: Element>(width: &str) {
        let query = vector::<T>(&[1.0, 0.0]);
        let longer = vector::<T>(&[10.0, 0.0]);
        let identical = vector::<T>(&[1.0, 0.0]);

        let d_identical = Metric::Cosine.distance(&query, &identical);
        let d_longer = Metric::Cosine.distance(&query, &longer);

        assert!(d_identical.abs() < 1e-9, "{width}: {d_identical} != 0");
        assert!(
            (d_longer - d_identical).abs() < 1e-9,
            "{width}: collinear vectors are equidistant under cosine: \
             {d_longer} vs {d_identical}"
        );
    }
    for_every_width!(check);
}

/// A length mismatch is compared over the shared prefix, not raised.
///
/// Cosine is the case that can quietly break the contract: a norm is a property
/// of a whole vector, so taking it over all of `a` while the dot product only
/// saw a prefix divides by a magnitude the numerator never included. Identical
/// prefixes must give distance 0 however long the ignored tail is.
#[test]
fn a_length_mismatch_uses_the_shared_prefix() {
    fn check<T: Element>(width: &str) {
        let tol = tolerance::<T>();
        let long = vector::<T>(&[0.0, 0.0, 9.0, 9.0]);
        let short = vector::<T>(&[3.0, 4.0]);
        let d = Metric::Euclidean.distance(&long, &short);
        assert!(
            (d - 5.0).abs() < tol,
            "{width}: the ignored tail must not contribute, got {d}"
        );

        let long = vector::<T>(&[3.0, 4.0, 99.0]);
        let short = vector::<T>(&[3.0, 4.0]);
        for (a, b) in [(&long, &short), (&short, &long)] {
            let d = Metric::Cosine.distance(a, b);
            assert!(
                d.abs() < tol,
                "{width}: identical prefixes are the same direction, got {d}"
            );
            let d = Metric::Cosine.distance_normalized(a, b);
            assert!(
                d.abs() < tol,
                "{width}: the normalized path must agree, got {d}"
            );
            assert!(!Metric::NegativeDot.distance(a, b).is_nan());
        }
    }
    for_every_width!(check);
}

/// An empty pair has no direction and no separation, and must still answer.
#[test]
fn empty_vectors_are_handled() {
    for metric in METRICS {
        let d = metric.distance::<f32>(&[], &[]);
        assert!(!d.is_nan(), "{metric:?} produced NaN for empty vectors");
    }
    assert_eq!(
        Metric::Cosine.distance::<f64>(&[], &[]),
        MAX_COSINE_DISTANCE,
        "no direction sorts last"
    );
    assert_eq!(Metric::Euclidean.distance::<f64>(&[], &[]), 0.0);
}
