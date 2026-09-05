//! Input validation utilities for HTTP handlers.

use crate::error::HttpError;
use defra_core::types::DocId;

/// Validate that a string is a valid identifier (collection name, field name, etc.)
///
/// Identifiers must:
/// - Start with a letter or underscore
/// - Contain only letters, digits, and underscores
/// - Not be empty
pub fn validate_identifier(name: &str) -> Result<(), HttpError> {
    if name.is_empty() {
        return Err(HttpError::BadRequest(
            "identifier cannot be empty".to_string(),
        ));
    }

    let valid = name.chars().enumerate().all(|(i, c)| {
        if i == 0 {
            c.is_ascii_alphabetic() || c == '_'
        } else {
            c.is_ascii_alphanumeric() || c == '_'
        }
    });

    if !valid {
        return Err(HttpError::BadRequest(format!(
            "'{}' is not a valid identifier (must match [A-Za-z_][A-Za-z0-9_]*)",
            name
        )));
    }

    Ok(())
}

/// Validate a collection name.
pub fn validate_collection_name(name: &str) -> Result<(), HttpError> {
    validate_identifier(name).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            name
        ))
    })
}

/// Validate a P2P peer address format.
///
/// Accepts both libp2p multiaddr format (starts with '/') and iroh
/// transport addresses (hex endpoint IDs or `iroh://` URIs).
/// Full validation happens in the P2P transport layer.
pub fn validate_multiaddr(address: &str) -> Result<(), HttpError> {
    if address.trim().is_empty() {
        return Err(HttpError::BadRequest("address cannot be empty".to_string()));
    }

    // None of the accepted shapes (multiaddr, iroh URI, bare endpoint
    // ID) may contain whitespace or control characters. Rejecting them
    // here fails fast with 400 instead of a confusing transport-layer
    // error deeper in the stack.
    if address.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(HttpError::BadRequest(format!(
            "invalid peer address '{}': must not contain whitespace or control characters",
            address
        )));
    }

    Ok(())
}

/// Validate a document ID format.
///
/// DefraDB document IDs use the canonical DocID parser shared with the document layer.
pub fn validate_doc_id(doc_id: &str) -> Result<(), HttpError> {
    if doc_id.trim().is_empty() {
        return Err(HttpError::BadRequest(
            "document ID cannot be empty".to_string(),
        ));
    }

    DocId::new(doc_id)
        .map(|_| ())
        .map_err(|e| HttpError::BadRequest(format!("invalid document ID '{}': {}", doc_id, e)))
}

/// Parse an optional request-body timeout expressed as a Go duration string.
///
/// Go's doc-sync handler reads `timeout` from the body and parses it with
/// `time.ParseDuration`, answering a malformed value with 400 rather than
/// falling back to a default (`http/handler_p2p.go:272-280`). An absent or
/// empty value means the caller set no deadline.
///
/// Stricter than the FFI helper in `crates/ffi/src/document/parse.rs`, which
/// accepts a bare integer as seconds for backwards compatibility. Go accepts no
/// unitless value other than `0`, so allowing `"5"` here would succeed on input
/// Go rejects.
pub fn parse_timeout(timeout: Option<&str>) -> Result<Option<std::time::Duration>, HttpError> {
    let Some(raw) = timeout.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    parse_go_duration(raw)
        .map(Some)
        .ok_or_else(|| HttpError::BadRequest(format!("time: invalid duration {raw:?}")))
}

/// Go's `1<<63`: one past the largest positive `time.Duration`. Go bounds its
/// running total against this rather than `i64::MAX` so the most negative
/// duration stays representable.
const MAX_DURATION_NANOS: u64 = 1 << 63;

/// Go `time.ParseDuration`: a signed sequence of decimal numbers, each with a
/// unit suffix. `None` on anything Go would reject, which the caller turns into
/// a 400.
///
/// Transcribed from Go rather than approximated, so each of its three overflow
/// checks has a counterpart here: per-unit, after the fractional part, and on
/// the running total. One deliberate divergence — Go accumulates in a wrapping
/// `uint64`, so two terms summing to exactly `1<<64` wrap to zero there and
/// parse as `0s`; we reject that instead.
fn parse_go_duration(value: &str) -> Option<std::time::Duration> {
    let (negative, mut rest) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };

    if rest == "0" {
        return Some(std::time::Duration::ZERO);
    }
    if rest.is_empty() {
        return None;
    }

    let mut total: u64 = 0;
    while !rest.is_empty() {
        let leading = rest.as_bytes()[0];
        if leading != b'.' && !leading.is_ascii_digit() {
            return None;
        }

        let before_int = rest.len();
        let (mut amount, remainder) = leading_int(rest)?;
        rest = remainder;
        let had_int = rest.len() != before_int;

        let mut fraction = 0_u64;
        let mut scale = 1.0_f64;
        let mut had_fraction = false;
        if let Some(after_point) = rest.strip_prefix('.') {
            let before_fraction = after_point.len();
            let (parsed, parsed_scale, remainder) = leading_fraction(after_point);
            fraction = parsed;
            scale = parsed_scale;
            rest = remainder;
            had_fraction = rest.len() != before_fraction;
        }
        if !had_int && !had_fraction {
            return None;
        }

        let unit_end = rest
            .find(|c: char| c == '.' || c.is_ascii_digit())
            .unwrap_or(rest.len());
        if unit_end == 0 {
            return None;
        }
        let unit = unit_nanos(&rest[..unit_end])?;
        rest = &rest[unit_end..];

        if amount > MAX_DURATION_NANOS / unit {
            return None;
        }
        amount *= unit;
        if fraction > 0 {
            // `f64` is what Go uses here too, and is exact enough because the
            // product is bounded by 3.6e12, the nanoseconds in one hour.
            amount = amount.checked_add((fraction as f64 * (unit as f64 / scale)) as u64)?;
            if amount > MAX_DURATION_NANOS {
                return None;
            }
        }
        total = total.checked_add(amount)?;
        if total > MAX_DURATION_NANOS {
            return None;
        }
    }

    // `Duration` is unsigned, and a negative timeout yields an already-expired
    // deadline in Go. Zero reproduces that: the operation gives up immediately.
    // The overflow checks above have already run, so an oversized negative is
    // rejected rather than collapsing to zero.
    if negative {
        return Some(std::time::Duration::ZERO);
    }
    if total > MAX_DURATION_NANOS - 1 {
        return None;
    }
    Some(std::time::Duration::from_nanos(total))
}

/// Go's `leadingInt`: consume `[0-9]*`, refusing anything that would carry the
/// accumulator past `1<<63`.
fn leading_int(value: &str) -> Option<(u64, &str)> {
    let mut amount: u64 = 0;
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if !byte.is_ascii_digit() {
            break;
        }
        if amount > MAX_DURATION_NANOS / 10 {
            return None;
        }
        amount = amount * 10 + u64::from(byte - b'0');
        if amount > MAX_DURATION_NANOS {
            return None;
        }
        index += 1;
    }
    Some((amount, &value[index..]))
}

/// Go's `leadingFraction`: consume `[0-9]*` after a decimal point, returning
/// the digits and the power of ten they were scaled by. Overflow drops the
/// remaining digits rather than failing, because they cannot change the value
/// at nanosecond resolution — Go does the same.
fn leading_fraction(value: &str) -> (u64, f64, &str) {
    let mut amount: u64 = 0;
    let mut scale = 1.0_f64;
    let mut overflowed = false;
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if !byte.is_ascii_digit() {
            break;
        }
        index += 1;
        if overflowed {
            continue;
        }
        if amount > (MAX_DURATION_NANOS - 1) / 10 {
            overflowed = true;
            continue;
        }
        let next = amount * 10 + u64::from(byte - b'0');
        if next > MAX_DURATION_NANOS {
            overflowed = true;
            continue;
        }
        amount = next;
        scale *= 10.0;
    }
    (amount, scale, &value[index..])
}

/// Go's `unitMap`.
fn unit_nanos(unit: &str) -> Option<u64> {
    match unit {
        "ns" => Some(1),
        "us" | "µs" | "μs" => Some(1_000),
        "ms" => Some(1_000_000),
        "s" => Some(1_000_000_000),
        "m" => Some(60_000_000_000),
        "h" => Some(3_600_000_000_000),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier_valid() {
        assert!(validate_identifier("Users").is_ok());
        assert!(validate_identifier("_private").is_ok());
        assert!(validate_identifier("User123").is_ok());
        assert!(validate_identifier("_").is_ok());
        assert!(validate_identifier("A").is_ok());
        assert!(validate_identifier("_1").is_ok());
    }

    #[test]
    fn test_validate_identifier_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("123Users").is_err());
        assert!(validate_identifier("User-Name").is_err());
        assert!(validate_identifier("User Name").is_err());
    }

    #[test]
    fn test_validate_identifier_unicode() {
        // Unicode characters should be rejected
        assert!(validate_identifier("Usuários").is_err());
        assert!(validate_identifier("用户").is_err());
        assert!(validate_identifier("Users日本").is_err());
        assert!(validate_identifier("Пользователи").is_err());
    }

    #[test]
    fn test_validate_identifier_control_chars() {
        // Control characters should be rejected
        assert!(validate_identifier("Users\0").is_err()); // null byte
        assert!(validate_identifier("Users\n").is_err()); // newline
        assert!(validate_identifier("Users\t").is_err()); // tab
        assert!(validate_identifier("Users\r").is_err()); // carriage return
    }

    #[test]
    fn test_validate_identifier_special_chars() {
        // Special characters should be rejected
        assert!(validate_identifier("Users!").is_err());
        assert!(validate_identifier("Users@").is_err());
        assert!(validate_identifier("Users#").is_err());
        assert!(validate_identifier("Users$").is_err());
        assert!(validate_identifier("Users%").is_err());
        assert!(validate_identifier("Users.field").is_err());
        assert!(validate_identifier("Users/path").is_err());
        assert!(validate_identifier("Users;DROP").is_err());
        assert!(validate_identifier("Users'--").is_err());
        assert!(validate_identifier("Users\"quote").is_err());
    }

    #[test]
    fn test_validate_identifier_long_name() {
        // Very long identifier should still be valid if it meets the pattern
        let long_name = "a".repeat(1000);
        assert!(validate_identifier(&long_name).is_ok());
    }

    #[test]
    fn test_validate_multiaddr_valid() {
        // libp2p multiaddr format
        assert!(validate_multiaddr("/ip4/127.0.0.1/tcp/9000").is_ok());
        assert!(validate_multiaddr("/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWtest").is_ok());
        assert!(validate_multiaddr("/dns/example.com/tcp/9000").is_ok());
        assert!(validate_multiaddr("/ip6/::1/tcp/9000").is_ok());
        // iroh address formats
        assert!(validate_multiaddr("iroh://abc123def456").is_ok());
        assert!(validate_multiaddr("abc123def456").is_ok());
    }

    #[test]
    fn test_validate_multiaddr_invalid() {
        assert!(validate_multiaddr("").is_err());
    }

    #[test]
    fn test_validate_multiaddr_rejects_inner_whitespace() {
        assert!(validate_multiaddr("!!! not an addr !!!").is_err());
        assert!(validate_multiaddr("/ip4/127.0.0.1/tcp/9000 ").is_err());
        assert!(validate_multiaddr(" /ip4/127.0.0.1/tcp/9000").is_err());
        assert!(validate_multiaddr("/ip4/127.0.0.1/ tcp/9000").is_err());
        assert!(validate_multiaddr("iroh://abc 123").is_err());
        assert!(validate_multiaddr("/ip4/127.0.0.1/tcp/9000\n").is_err());
    }

    #[test]
    fn test_validate_multiaddr_whitespace() {
        assert!(validate_multiaddr("   ").is_err());
        assert!(validate_multiaddr("\t").is_err());
        assert!(validate_multiaddr("\n").is_err());
    }

    #[test]
    fn test_validate_collection_name_valid() {
        assert!(validate_collection_name("Users").is_ok());
        assert!(validate_collection_name("_private").is_ok());
        assert!(validate_collection_name("Collection123").is_ok());
    }

    #[test]
    fn test_validate_collection_name_invalid() {
        assert!(validate_collection_name("").is_err());
        assert!(validate_collection_name("123Collection").is_err());
        assert!(validate_collection_name("User-Name").is_err());
    }

    #[test]
    fn test_validate_doc_id_valid() {
        assert!(validate_doc_id("bae-3fc941b7-505c-5ce2-91a0-b180930ec8a9").is_ok());
        assert!(validate_doc_id("bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc").is_ok());
    }

    #[test]
    fn test_validate_doc_id_invalid() {
        assert!(validate_doc_id("").is_err());
        assert!(validate_doc_id("   ").is_err());
        assert!(validate_doc_id("invalid-id").is_err());
        assert!(validate_doc_id("bae-").is_err());
        assert!(validate_doc_id("bae-GHIJK").is_err()); // G-Z are not hex
        assert!(validate_doc_id("BAE-abc").is_err()); // Must be lowercase bae-
    }

    use std::time::Duration;

    #[test]
    fn timeout_absent_or_empty_means_no_deadline() {
        assert_eq!(parse_timeout(None).unwrap(), None);
        assert_eq!(parse_timeout(Some("")).unwrap(), None);
        assert_eq!(parse_timeout(Some("   ")).unwrap(), None);
    }

    #[test]
    fn timeout_accepts_the_forms_go_emits() {
        // `time.Duration.String()`, which is what Go's client sends.
        assert_eq!(
            parse_timeout(Some("5s")).unwrap(),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            parse_timeout(Some("1m30s")).unwrap(),
            Some(Duration::from_secs(90))
        );
        assert_eq!(
            parse_timeout(Some("500ms")).unwrap(),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            parse_timeout(Some("1.5s")).unwrap(),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            parse_timeout(Some("2h45m0s")).unwrap(),
            Some(Duration::from_secs(9900))
        );
        assert_eq!(parse_timeout(Some("0")).unwrap(), Some(Duration::ZERO));
        assert_eq!(
            parse_timeout(Some("100µs")).unwrap(),
            Some(Duration::from_micros(100))
        );
    }

    /// Go's `time.ParseDuration` requires a unit on everything but `0`, so a
    /// bare number is a 400 rather than a silent "seconds" reading.
    #[test]
    fn timeout_rejects_what_go_rejects() {
        for rejected in ["5", "abc", "5x", "s", "1m2", "-", "."] {
            assert!(
                parse_timeout(Some(rejected)).is_err(),
                "{rejected:?} should be rejected"
            );
        }
    }

    /// Go builds an already-expired context from a negative timeout; an
    /// immediate give-up is the same observable behaviour.
    #[test]
    fn timeout_treats_negative_as_already_expired() {
        assert_eq!(parse_timeout(Some("-5s")).unwrap(), Some(Duration::ZERO));
    }

    /// Go's `time.ParseDuration` errors on anything past `int64` nanoseconds
    /// instead of clamping, so these are 400s. The `f64` accumulator this
    /// replaced turned them into ~584-year deadlines, and a 584-year deadline
    /// on the pubsub path is a request that never returns.
    #[test]
    fn timeout_rejects_durations_go_cannot_represent() {
        for rejected in [
            "9999999h",
            "9223372036854775808ns",
            "2562047h47m16.854775808s",
            "-9999999h",
        ] {
            assert!(
                parse_timeout(Some(rejected)).is_err(),
                "{rejected:?} overflows Go's int64 nanoseconds and must be a 400"
            );
        }
    }

    /// The one input where we knowingly disagree with Go. Go accumulates in a
    /// wrapping `uint64`, so these two terms sum to exactly `1<<64`, wrap to
    /// zero, and parse as `0s` — verified against go1.26.5. Reproducing that
    /// would turn a duration far past the maximum into "give up immediately",
    /// so we reject it instead.
    #[test]
    fn timeout_rejects_the_pair_go_wraps_to_zero() {
        assert!(parse_timeout(Some("9223372036854775808ns9223372036854775808ns")).is_err());
    }

    /// `time.Duration`'s exact maximum, in the form `Duration.String()` emits.
    /// Doubles as the precision guard: an `f64` accumulator rounds this up by
    /// one nanosecond, off the boundary it is meant to sit exactly on.
    #[test]
    fn timeout_accepts_gos_maximum_exactly() {
        assert_eq!(
            parse_timeout(Some("2562047h47m16.854775807s")).unwrap(),
            Some(Duration::from_nanos(9_223_372_036_854_775_807)),
        );
    }
}
