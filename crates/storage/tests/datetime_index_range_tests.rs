//! DateTime index encoding across the full RFC3339 range.

use chrono::{DateTime, FixedOffset};
use document::NormalValue;
use schema::FieldKind;
use storage::encoding;
use storage::field_value::{decode_field_value, encode_field_value};

fn at(rfc3339: &str) -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(rfc3339).expect("the schema accepts this timestamp")
}

fn roundtrip(value: &NormalValue, descending: bool) -> NormalValue {
    let buf = encode_field_value(vec![], value, descending).expect("must encode");
    let (rest, decoded) =
        decode_field_value(&buf, descending, &FieldKind::datetime()).expect("must decode");
    assert!(rest.is_empty(), "trailing bytes after a timestamp");
    decoded
}

/// The reported case: a timestamp `parse_from_rfc3339` accepts and Go indexes,
/// which the single-i64-nanosecond encoder could not represent.
#[test]
fn a_year_9999_timestamp_encodes_and_round_trips() {
    let value = NormalValue::Time(at("9999-12-31T23:59:59Z"));
    assert_eq!(roundtrip(&value, false), value);
    assert_eq!(roundtrip(&value, true), value);
}

/// The other side of the old window.
#[test]
fn a_far_past_timestamp_encodes_and_round_trips() {
    for stamp in ["0001-01-01T00:00:00Z", "1000-06-15T12:30:45Z"] {
        let value = NormalValue::Time(at(stamp));
        assert_eq!(roundtrip(&value, false), value, "{stamp}");
        assert_eq!(roundtrip(&value, true), value, "{stamp}");
    }
}

/// `timestamp_nanos_opt` is `None` outside roughly 1677..2262, which is where
/// the old encoder failed. Every one of these must now encode.
#[test]
fn timestamps_outside_the_nanosecond_window_encode() {
    for stamp in [
        "1600-01-01T00:00:00Z",
        "1676-12-31T23:59:59Z",
        "2263-01-01T00:00:00Z",
        "5000-01-01T00:00:00Z",
        "9999-12-31T23:59:59.999999999Z",
    ] {
        let value = NormalValue::Time(at(stamp));
        assert!(
            at(stamp).timestamp_nanos_opt().is_none(),
            "{stamp} is inside the nanosecond window, so it is not a regression case"
        );
        assert_eq!(roundtrip(&value, false), value, "{stamp}");
    }
}

/// Index keys must sort chronologically, including across the boundary the old
/// encoder could not cross.
#[test]
fn index_keys_sort_chronologically_across_the_whole_range() {
    let ordered = [
        "0001-01-01T00:00:00Z",
        "1600-01-01T00:00:00Z",
        "1969-12-31T23:59:59Z",
        "1970-01-01T00:00:00Z",
        "1970-01-01T00:00:00.000000001Z",
        "2024-06-01T12:00:00Z",
        "2262-04-11T23:47:16Z",
        "2263-01-01T00:00:00Z",
        "9999-12-31T23:59:59Z",
    ];

    let keys: Vec<Vec<u8>> = ordered
        .iter()
        .map(|stamp| encode_field_value(vec![], &NormalValue::Time(at(stamp)), false).unwrap())
        .collect();

    for (pair, stamps) in keys.windows(2).zip(ordered.windows(2)) {
        assert!(
            pair[0] < pair[1],
            "{} must sort before {}",
            stamps[0],
            stamps[1]
        );
    }
}

#[test]
fn descending_index_keys_sort_in_reverse() {
    let ordered = [
        "0001-01-01T00:00:00Z",
        "1969-12-31T23:59:59Z",
        "2024-06-01T12:00:00Z",
        "9999-12-31T23:59:59Z",
    ];

    let keys: Vec<Vec<u8>> = ordered
        .iter()
        .map(|stamp| encode_field_value(vec![], &NormalValue::Time(at(stamp)), true).unwrap())
        .collect();

    for (pair, stamps) in keys.windows(2).zip(ordered.windows(2)) {
        assert!(
            pair[0] > pair[1],
            "{} must sort after {} when descending",
            stamps[0],
            stamps[1]
        );
    }
}

/// Sub-second precision has to survive, or two events in the same second
/// collide in the index.
#[test]
fn sub_second_precision_is_preserved_and_ordered() {
    let stamps = [
        "9999-01-01T00:00:00Z",
        "9999-01-01T00:00:00.000000001Z",
        "9999-01-01T00:00:00.5Z",
        "9999-01-01T00:00:00.999999999Z",
    ];

    let mut previous: Option<Vec<u8>> = None;
    for stamp in stamps {
        let value = NormalValue::Time(at(stamp));
        assert_eq!(roundtrip(&value, false), value, "{stamp}");

        let key = encode_field_value(vec![], &value, false).unwrap();
        if let Some(earlier) = previous {
            assert!(earlier < key, "{stamp} must sort after the previous");
        }
        previous = Some(key);
    }
}

/// A negative timestamp keeps a non-negative sub-second remainder, which is
/// what makes the pair sort correctly. Go's `time.Time` has the same property.
#[test]
fn pre_epoch_timestamps_keep_a_non_negative_remainder() {
    let (_, seconds, nanos) = encoding::decode_time_ascending(&encoding::encode_time_ascending(
        vec![],
        at("1969-12-31T23:59:59.25Z").timestamp(),
        at("1969-12-31T23:59:59.25Z").timestamp_subsec_nanos(),
    ))
    .unwrap();

    assert_eq!(seconds, -1);
    assert_eq!(nanos, 250_000_000);
}

/// The nillable variant takes the same path.
#[test]
fn a_nillable_timestamp_outside_the_window_encodes() {
    let value = NormalValue::NillableTime(Some(at("9999-12-31T23:59:59Z")));
    let buf = encode_field_value(vec![], &value, false).expect("must encode");
    let (_, decoded) =
        decode_field_value(&buf, false, &FieldKind::datetime()).expect("must decode");
    assert_eq!(decoded, NormalValue::Time(at("9999-12-31T23:59:59Z")));
}

/// Two independent varints after the marker, which is the shape Go writes.
#[test]
fn the_encoding_is_a_marker_and_two_varints() {
    let seconds = at("9999-12-31T23:59:59Z").timestamp();
    let buf = encoding::encode_time_ascending(vec![], seconds, 123);

    let (rest, decoded_seconds, decoded_nanos) = encoding::decode_time_ascending(&buf).unwrap();
    assert!(rest.is_empty());
    assert_eq!(decoded_seconds, seconds);
    assert_eq!(decoded_nanos, 123);

    assert_eq!(
        encoding::peek_type(&buf),
        encoding::EncodedType::Time,
        "the marker must still identify a timestamp"
    );
}
