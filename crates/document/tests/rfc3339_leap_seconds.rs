//! Leap seconds are refused, because Go refuses them.
//!
//! chrono parses `:60` and reports the remainder as one second or more, which
//! no `time.Time` can hold. Left accepted, three distinct instants collapse onto
//! one index key: `:60`, `:60.5` and `:59.999999999` all clamp to the same
//! bytes, so a unique index reports a false duplicate and a range bound skips
//! rows.

use document::{is_leap_second, is_valid_rfc3339, parse_rfc3339};

/// Go rejects these on both parse paths: `format.go:1162` (`60 <= sec`) and
/// `format_rfc3339.go:112`, which caps the seconds field at 59.
#[test]
fn a_leap_second_is_refused() {
    for value in [
        "2016-12-31T23:59:60Z",
        "2016-12-31T23:59:60.5Z",
        "2015-06-30T23:59:60+00:00",
    ] {
        assert!(parse_rfc3339(value).is_none(), "{value} must not parse");
        assert!(!is_valid_rfc3339(value), "{value} must not validate");
    }
}

/// The instant a leap second used to alias onto stays valid and distinct.
#[test]
fn the_last_representable_instant_still_parses() {
    let parsed = parse_rfc3339("2016-12-31T23:59:59.999999999Z").expect("must parse");
    assert_eq!(parsed.timestamp_subsec_nanos(), 999_999_999);
    assert!(!is_leap_second(&parsed));
}

/// The aliasing this prevents: chrono gives these the same second and a
/// remainder that only differs above one second, so clamping made them equal.
#[test]
fn a_leap_second_shares_its_second_with_the_instant_it_aliased_onto() {
    let leap = chrono::DateTime::parse_from_rfc3339("2016-12-31T23:59:60Z").unwrap();
    let last = parse_rfc3339("2016-12-31T23:59:59.999999999Z").unwrap();

    assert_eq!(leap.timestamp(), last.timestamp());
    assert!(is_leap_second(&leap));
    assert_eq!(leap.timestamp_subsec_nanos(), 1_000_000_000);
}

#[test]
fn ordinary_timestamps_are_unaffected() {
    for value in [
        "2024-01-01T00:00:00Z",
        "1970-01-01T00:00:00Z",
        "9999-12-31T23:59:59.999999999Z",
        "1900-01-01T12:30:45.123456789+02:00",
    ] {
        assert!(is_valid_rfc3339(value), "{value} must stay valid");
    }
}
