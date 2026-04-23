//! Tests for replicator types, including Go wire-format parity.

use chrono::{TimeZone, Utc};
use p2p::replicator::{ReplicatorError, ReplicatorInfo, ReplicatorStatus};
use p2p::{Multiaddr, PeerId};

// ---------------------------------------------------------------------------
// Go-produced fixtures (generated via `encoding/json` on the real
// `client.Replicator` struct — see `defradb/client/replicator.go`).
// Any drift here means a Rust node can't read peerstore data written by Go.
// ---------------------------------------------------------------------------

const GO_ACTIVE_WITH_ADDRS: &str = r#"{"ID":"12D3KooWGjMkcMy5PM9iSbgWWgUnH5dQhvzhNu7w3Gk4kHZBsxnJ","Addresses":["/ip4/127.0.0.1/tcp/4001"],"CollectionIDs":["users","posts"],"Status":0,"LastStatusChange":"2024-06-15T12:34:56Z"}"#;

const GO_INACTIVE_NULL_ADDRS: &str = r#"{"ID":"12D3KooWGjMkcMy5PM9iSbgWWgUnH5dQhvzhNu7w3Gk4kHZBsxnJ","Addresses":null,"CollectionIDs":["orders"],"Status":1,"LastStatusChange":"2024-06-15T12:34:56Z"}"#;

const GO_ZERO_STATUS_CHANGE: &str = r#"{"ID":"12D3KooWGjMkcMy5PM9iSbgWWgUnH5dQhvzhNu7w3Gk4kHZBsxnJ","Addresses":[],"CollectionIDs":["users"],"Status":0,"LastStatusChange":"0001-01-01T00:00:00Z"}"#;

const GO_FIXTURE_PEER_ID: &str = "12D3KooWGjMkcMy5PM9iSbgWWgUnH5dQhvzhNu7w3Gk4kHZBsxnJ";

// ---------------------------------------------------------------------------
// Go → Rust decode parity
// ---------------------------------------------------------------------------

#[test]
fn decodes_go_active_with_addresses() {
    let info = ReplicatorInfo::from_bytes(GO_ACTIVE_WITH_ADDRS.as_bytes()).unwrap();
    assert_eq!(info.peer_id_str(), GO_FIXTURE_PEER_ID);
    assert_eq!(info.collections, vec!["users", "posts"]);
    assert_eq!(
        info.addresses_str(),
        &["/ip4/127.0.0.1/tcp/4001".to_string()]
    );
    assert_eq!(info.status, ReplicatorStatus::Active);
    assert_eq!(
        info.last_status_change,
        Utc.with_ymd_and_hms(2024, 6, 15, 12, 34, 56).unwrap()
    );
}

#[test]
fn decodes_go_null_addresses_as_empty_vec() {
    // Go marshals `nil []string` as JSON `null`; Rust should treat this as
    // an empty Vec rather than erroring on the type mismatch.
    let info = ReplicatorInfo::from_bytes(GO_INACTIVE_NULL_ADDRS.as_bytes()).unwrap();
    assert_eq!(info.status, ReplicatorStatus::Inactive);
    assert!(info.addresses_str().is_empty());
    assert_eq!(info.collections, vec!["orders"]);
}

#[test]
fn decodes_go_time_zero_value() {
    // Go's `time.Time{}` zero value serializes as `"0001-01-01T00:00:00Z"`.
    let info = ReplicatorInfo::from_bytes(GO_ZERO_STATUS_CHANGE.as_bytes()).unwrap();
    assert_eq!(
        info.last_status_change,
        Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0).unwrap()
    );
}

// ---------------------------------------------------------------------------
// Rust → Go byte-exact encode parity
// ---------------------------------------------------------------------------

#[test]
fn encodes_byte_exact_to_go_active_with_addresses() {
    let mut info = ReplicatorInfo::from_raw(
        GO_FIXTURE_PEER_ID.to_string(),
        vec!["users".to_string(), "posts".to_string()],
        vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
    );
    info.last_status_change = Utc.with_ymd_and_hms(2024, 6, 15, 12, 34, 56).unwrap();
    info.status = ReplicatorStatus::Active;

    let bytes = info.to_bytes().unwrap();
    assert_eq!(
        std::str::from_utf8(&bytes).unwrap(),
        GO_ACTIVE_WITH_ADDRS,
        "Rust JSON output must match Go byte-for-byte so a shared peerstore round-trips"
    );
}

#[test]
fn encodes_byte_exact_to_go_zero_time() {
    // Defaults: fresh struct has Status=Active and LastStatusChange=time.Time{}.
    let info = ReplicatorInfo::from_raw(
        GO_FIXTURE_PEER_ID.to_string(),
        vec!["users".to_string()],
        vec![],
    );

    let bytes = info.to_bytes().unwrap();
    assert_eq!(std::str::from_utf8(&bytes).unwrap(), GO_ZERO_STATUS_CHANGE);
}

// ---------------------------------------------------------------------------
// Round-trip
// ---------------------------------------------------------------------------

#[test]
fn json_round_trip_preserves_all_fields() {
    let peer_id = PeerId::random();
    let mut info =
        ReplicatorInfo::new(peer_id, vec!["users".to_string(), "posts".to_string()]).unwrap();
    info.status = ReplicatorStatus::Inactive;
    info.last_status_change = Utc.with_ymd_and_hms(2026, 4, 23, 14, 0, 0).unwrap();

    let bytes = info.to_bytes().unwrap();
    let restored = ReplicatorInfo::from_bytes(&bytes).unwrap();
    assert_eq!(info, restored);
}

#[test]
fn timestamp_encodes_like_go_rfc3339nano_with_trailing_zeros() {
    // Go's `time.Time.MarshalJSON` uses RFC3339Nano, which strips trailing
    // zeros from the fractional seconds (`.100` → `.1`, `.001` stays `.001`).
    // Chrono's default serializer pads to 3/6/9 digits, so a naive
    // implementation would emit `.100Z` and drift from a Go-produced peer
    // store record. This test pins the Go-compatible behavior.
    let cases: [(i64, u32, &str); 5] = [
        // subsec ns, expected fractional suffix (or empty)
        (1_700_000_000, 0, ""),
        (1_700_000_000, 1_000_000, ".001"), // 1 ms
        (1_700_000_000, 100_000_000, ".1"), // 100 ms (the drift case)
        (1_700_000_000, 1_000, ".000001"),  // 1 µs
        (1_700_000_000, 1, ".000000001"),   // 1 ns
    ];
    for (secs, ns, frac) in cases {
        let mut info =
            ReplicatorInfo::from_raw("peer".to_string(), vec!["users".to_string()], vec![]);
        info.last_status_change = Utc.timestamp_opt(secs, ns).unwrap();

        let json = info.to_bytes().unwrap();
        let s = std::str::from_utf8(&json).unwrap();
        // The emitted timestamp string must equal the Go-style rendering.
        let go_expected = format!(r#""LastStatusChange":"2023-11-14T22:13:20{frac}Z""#);
        assert!(
            s.contains(&go_expected),
            "subsec_ns={ns} expected to contain {go_expected} but got: {s}"
        );

        // Round-trip preserves the exact instant.
        let restored = ReplicatorInfo::from_bytes(&json).unwrap();
        assert_eq!(restored.last_status_change, info.last_status_change);
    }
}

#[test]
fn decode_ignores_unknown_fields() {
    // serde_json ignores unknown fields by default. Documenting that contract
    // here protects against accidental `#[serde(deny_unknown_fields)]` that
    // would break forward-compat with future Go-side schema additions.
    let future = r#"{"ID":"peer","Addresses":[],"CollectionIDs":["users"],"Status":0,"LastStatusChange":"0001-01-01T00:00:00Z","SomeNewField":123,"Another":"x"}"#;
    let info = ReplicatorInfo::from_bytes(future.as_bytes()).unwrap();
    assert_eq!(info.collections, vec!["users"]);
}

// ---------------------------------------------------------------------------
// Constructors and validation
// ---------------------------------------------------------------------------

#[test]
fn new_rejects_empty_collections() {
    let peer_id = PeerId::random();
    let result = ReplicatorInfo::new(peer_id, vec![]);
    assert!(matches!(result, Err(ReplicatorError::EmptyCollections)));
}

#[test]
fn new_accepts_non_empty_collections() {
    let peer_id = PeerId::random();
    let info = ReplicatorInfo::new(peer_id, vec!["users".to_string()]).unwrap();
    assert_eq!(info.peer_id(), Some(peer_id));
    assert_eq!(info.status, ReplicatorStatus::Active);
    assert!(info.addresses().is_empty());
}

#[test]
fn from_raw_skips_validation() {
    // from_raw is the escape hatch for reconstructing from storage or
    // building test fixtures — no checks on collections or peer ID.
    let info = ReplicatorInfo::from_raw("invalid".to_string(), vec![], vec![]);
    assert!(info.peer_id().is_none());
    assert!(info.try_peer_id().is_err());
    assert!(info.collections.is_empty());
}

#[test]
fn invalid_addresses_filtered_on_read() {
    let peer_id = PeerId::random();
    let info = ReplicatorInfo::from_raw(
        peer_id.to_string(),
        vec!["users".to_string()],
        vec![
            "/ip4/127.0.0.1/tcp/4001".to_string(),
            "not-a-multiaddr".to_string(),
        ],
    );
    let parsed = info.addresses();
    assert_eq!(parsed.len(), 1);
    let expected: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();
    assert_eq!(parsed[0], expected);
}

// ---------------------------------------------------------------------------
// Decode error surfaces
// ---------------------------------------------------------------------------

#[test]
fn from_bytes_rejects_non_json() {
    assert!(ReplicatorInfo::from_bytes(&[0x00, 0x01, 0x02]).is_err());
}

#[test]
fn from_bytes_rejects_empty() {
    assert!(ReplicatorInfo::from_bytes(&[]).is_err());
}

#[test]
fn from_bytes_rejects_truncated_json() {
    let truncated = &GO_ACTIVE_WITH_ADDRS.as_bytes()[..GO_ACTIVE_WITH_ADDRS.len() / 2];
    assert!(ReplicatorInfo::from_bytes(truncated).is_err());
}

#[test]
fn from_bytes_rejects_unknown_status() {
    let bad = r#"{"ID":"peer","Addresses":[],"CollectionIDs":["users"],"Status":42,"LastStatusChange":"0001-01-01T00:00:00Z"}"#;
    assert!(ReplicatorInfo::from_bytes(bad.as_bytes()).is_err());
}
