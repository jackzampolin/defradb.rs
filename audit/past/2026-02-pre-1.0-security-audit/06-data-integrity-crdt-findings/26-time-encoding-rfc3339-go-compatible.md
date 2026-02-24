# Time Encoding: RFC3339 Nano Format Go-Compatible

**Severity:** Informational
**Category:** CID Determinism
**Status:** Verified Clean

## Summary

Time values are encoded as RFC3339 strings matching Go's `time.RFC3339Nano` behavior. The encoding is deterministic: nanosecond precision is included only when non-zero, timezone offset is preserved from the original parse, and the format is consistent across platforms. Since time strings are embedded in CBOR content that feeds CID computation, this formatting is critical for deterministic DocIDs.

## Affected Files

- `crates/document/src/encoding.rs:17-25` (`format_time_rfc3339_nano()`)

## Details

### Format Behavior

```rust
pub fn format_time_rfc3339_nano(t: &DateTime<FixedOffset>) -> String {
    if t.nanosecond() % 1_000_000_000 == 0 {
        // No fractional seconds: "2017-07-23T03:46:56-05:00"
        t.to_rfc3339_opts(SecondsFormat::Secs, true)
    } else {
        // With fractional seconds: "2017-07-23T03:46:56.123456789-05:00"
        t.to_rfc3339_opts(SecondsFormat::Nanos, true)
    }
}
```

### Go Compatibility

| Input | Go Output | Rust Output | Match |
|-------|-----------|-------------|-------|
| Zero nanoseconds | `2017-07-23T03:46:56-05:00` | `2017-07-23T03:46:56-05:00` | Yes |
| Non-zero nanoseconds | `2017-07-23T03:46:56.123456789-05:00` | `2017-07-23T03:46:56.123456789-05:00` | Yes |
| UTC timezone | `2017-07-23T08:46:56Z` | `2017-07-23T08:46:56Z` | Yes |

The second parameter `true` in `to_rfc3339_opts` causes UTC offsets to be displayed as "Z" rather than "+00:00", matching Go's behavior.

### Precision Consistency

Go's `RFC3339Nano` always outputs 9 decimal digits when nanoseconds are non-zero. Rust's `SecondsFormat::Nanos` also outputs exactly 9 digits. There is no ambiguity — precision is always either 0 digits (no fractional part) or 9 digits.

### Timezone Preservation

`DateTime<FixedOffset>` preserves the original timezone offset from parsing. Two timestamps representing the same instant but in different timezones will produce different strings:
- `2024-01-01T00:00:00Z` and `2024-01-01T05:30:00+05:30` → different strings → different CIDs

This is correct behavior — the Go implementation also preserves timezone offsets.

## Conclusion

Time encoding is deterministic, Go-compatible, and consistent across platforms. No CID divergence risk from time formatting.
