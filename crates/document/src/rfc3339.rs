//! RFC3339 parsing that matches Go's `time.Parse`.
//!
//! chrono accepts leap seconds and reports them as a sub-second remainder at or
//! above one second. Go rejects them on both of its paths (`format.go:1162`
//! `60 <= sec`, and `format_rfc3339.go:112` which caps seconds at 59), and its
//! `time.Time` cannot represent one. Accepting a value Go refuses would also
//! alias it onto `:59.999999999` in an index key, so every DateTime entry point
//! parses through here.

use chrono::{DateTime, FixedOffset};

/// One second, in nanoseconds. A remainder at or above this is a leap second.
const NANOS_PER_SECOND: u32 = 1_000_000_000;

/// Whether this instant is a leap second, which Go cannot represent.
pub fn is_leap_second(value: &DateTime<FixedOffset>) -> bool {
    value.timestamp_subsec_nanos() >= NANOS_PER_SECOND
}

/// Parse an RFC3339 timestamp, rejecting what Go rejects.
pub fn parse_rfc3339(value: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .filter(|parsed| !is_leap_second(parsed))
}

/// Whether this string is a timestamp Go would accept.
pub fn is_valid_rfc3339(value: &str) -> bool {
    parse_rfc3339(value).is_some()
}
