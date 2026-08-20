//! Tests for replicator types, including Go wire-format parity.

use chrono::{TimeZone, Utc};
#[cfg(feature = "libp2p-transport")]
use p2p::replicator::ReplicatorError;
use p2p::replicator::{
    EqOnlyFilterMatcher, ReplicationFilterMatcher as _, ReplicatorInfo, ReplicatorStatus,
};
#[cfg(feature = "libp2p-transport")]
use p2p::{Multiaddr, PeerId};
use p2p::{ReplicationFilter, ReplicationFilters};

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

#[test]
fn unfiltered_rust_extension_does_not_change_go_bytes() {
    let info = ReplicatorInfo::from_raw(
        GO_FIXTURE_PEER_ID.to_string(),
        vec!["users".to_string()],
        vec![],
    );

    let json = std::str::from_utf8(&info.to_bytes().unwrap())
        .unwrap()
        .to_string();
    assert_eq!(json, GO_ZERO_STATUS_CHANGE);
    assert!(
        !json.contains("Filters"),
        "empty Rust-only filter metadata must not drift from Go peerstore JSON"
    );
}

#[test]
fn filtered_replicator_round_trips_rust_extension() {
    let matcher = EqOnlyFilterMatcher;
    let mut filters = ReplicationFilters::new();
    filters.insert(
        "users".to_string(),
        ReplicationFilter::new("agent_did", serde_json::json!("did:key:z6M")),
    );
    let info = ReplicatorInfo::from_raw_with_filters(
        GO_FIXTURE_PEER_ID.to_string(),
        vec!["users".to_string()],
        vec![],
        filters,
    );

    let bytes = info.to_bytes().unwrap();
    assert_eq!(
        std::str::from_utf8(&bytes).unwrap(),
        r#"{"ID":"12D3KooWGjMkcMy5PM9iSbgWWgUnH5dQhvzhNu7w3Gk4kHZBsxnJ","Addresses":[],"CollectionIDs":["users"],"Status":0,"LastStatusChange":"0001-01-01T00:00:00Z","Filters":{"users":{"predicate":{"agent_did":{"_eq":"did:key:z6M"}}}}}"#
    );

    let restored = ReplicatorInfo::from_bytes(&bytes).unwrap();
    assert_eq!(restored, info);
    assert!(restored.matches_filter(
        &matcher,
        "users",
        &serde_json::json!({"agent_did": "did:key:z6M", "body": "selected"})
    ));
    assert!(!restored.matches_filter(
        &matcher,
        "users",
        &serde_json::json!({"agent_did": "did:key:other"})
    ));

    // Legacy wire format deserializes into the same Predicate variant.
    let legacy_json = r#"{"Field":"agent_did","Value":"did:key:z6M"}"#;
    let from_legacy: ReplicationFilter = serde_json::from_str(legacy_json).unwrap();
    assert_eq!(
        from_legacy,
        ReplicationFilter::new("agent_did", serde_json::json!("did:key:z6M"))
    );
}

#[test]
fn filter_matches_numbers_numerically() {
    let matcher = EqOnlyFilterMatcher;

    // A whole-number Float field materializes as an integer in Go-compatible
    // JSON, so a 2.0 filter value must still match a stored 2.
    let float_filter = ReplicationFilter::new("score", serde_json::json!(2.0));
    assert!(matcher.matches("", &float_filter, &serde_json::json!({"score": 2})));
    assert!(matcher.matches("", &float_filter, &serde_json::json!({"score": 2.0})));
    assert!(!matcher.matches("", &float_filter, &serde_json::json!({"score": 3})));

    // Non-whole floats and integers still match their own form.
    let frac_filter = ReplicationFilter::new("ratio", serde_json::json!(2.5));
    assert!(matcher.matches("", &frac_filter, &serde_json::json!({"ratio": 2.5})));

    // A string filter value never matches a numeric field (type mismatch).
    let str_filter = ReplicationFilter::new("score", serde_json::json!("2"));
    assert!(!matcher.matches("", &str_filter, &serde_json::json!({"score": 2})));

    // Two distinct large integers must NOT match: comparing them as f64 would
    // lose precision above 2^53 and falsely match.
    let big = 9_007_199_254_740_993_i64; // 2^53 + 1
    let big_filter = ReplicationFilter::new("id", serde_json::json!(big));
    assert!(matcher.matches("", &big_filter, &serde_json::json!({ "id": big })));
    assert!(!matcher.matches("", &big_filter, &serde_json::json!({ "id": big + 1 })));
}

// ---------------------------------------------------------------------------
// Round-trip
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "libp2p-transport")]
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
#[cfg(feature = "libp2p-transport")]
fn status_update_records_first_transition_only() {
    let peer_id = PeerId::random();
    let mut info = ReplicatorInfo::new(peer_id, vec!["users".to_string()]).unwrap();
    let inactive_at = Utc.with_ymd_and_hms(2026, 4, 26, 10, 0, 0).unwrap();
    let repeated_at = Utc.with_ymd_and_hms(2026, 4, 26, 10, 5, 0).unwrap();
    let active_at = Utc.with_ymd_and_hms(2026, 4, 26, 10, 10, 0).unwrap();

    assert!(info.set_status_if_changed(ReplicatorStatus::Inactive, inactive_at));
    assert_eq!(info.status, ReplicatorStatus::Inactive);
    assert_eq!(info.last_status_change, inactive_at);

    assert!(!info.set_status_if_changed(ReplicatorStatus::Inactive, repeated_at));
    assert_eq!(info.last_status_change, inactive_at);

    assert!(info.set_status_if_changed(ReplicatorStatus::Active, active_at));
    assert_eq!(info.status, ReplicatorStatus::Active);
    assert_eq!(info.last_status_change, active_at);
}

#[test]
#[cfg(feature = "libp2p-transport")]
fn set_status_if_changed_now_matches_go_recovery_rule() {
    // Go's `updateReplicatorStatus` (`defradb/internal/db/p2p/replicator.go:495`)
    // resets `LastStatusChange` to `time.Time{}` on `Inactive → Active`,
    // and stamps `time.Now()` on `Active → Inactive`. Mirroring that here
    // keeps the JSON wire format identical to a Go-produced peerstore record.
    let peer_id = PeerId::random();
    let mut info = ReplicatorInfo::new(peer_id, vec!["users".to_string()]).unwrap();
    let go_zero = Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0).unwrap();

    let before = Utc::now();
    assert!(info.set_status_if_changed_now(ReplicatorStatus::Inactive));
    let after = Utc::now();
    assert_eq!(info.status, ReplicatorStatus::Inactive);
    assert!(
        info.last_status_change >= before && info.last_status_change <= after,
        "Active → Inactive must stamp now(), got {}",
        info.last_status_change
    );

    assert!(info.set_status_if_changed_now(ReplicatorStatus::Active));
    assert_eq!(info.status, ReplicatorStatus::Active);
    assert_eq!(
        info.last_status_change, go_zero,
        "Inactive → Active must reset to Go's time.Time{{}} zero value"
    );
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
#[cfg(feature = "libp2p-transport")]
fn new_rejects_empty_collections() {
    let peer_id = PeerId::random();
    let result = ReplicatorInfo::new(peer_id, vec![]);
    assert!(matches!(result, Err(ReplicatorError::EmptyCollections)));
}

#[test]
#[cfg(feature = "libp2p-transport")]
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
    assert_eq!(info.peer_id_str(), "invalid");
    #[cfg(feature = "libp2p-transport")]
    assert!(info.peer_id().is_none());
    #[cfg(feature = "libp2p-transport")]
    assert!(info.try_peer_id().is_err());
    assert!(info.collections.is_empty());
}

#[test]
#[cfg(feature = "libp2p-transport")]
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

// ---------------------------------------------------------------------------
// ReplicationFilter enum and EqOnlyFilterMatcher
// ---------------------------------------------------------------------------

#[test]
fn legacy_wire_deserializes_to_predicate() {
    let json = r#"{"Field":"agent_did","Value":"x"}"#;
    let filter: ReplicationFilter = serde_json::from_str(json).unwrap();
    let mut expected_conds = serde_json::Map::new();
    let mut op = serde_json::Map::new();
    op.insert("_eq".to_string(), serde_json::json!("x"));
    expected_conds.insert("agent_did".to_string(), serde_json::Value::Object(op));
    assert_eq!(filter, ReplicationFilter::Predicate(expected_conds));
}

#[test]
fn predicate_with_in_round_trips() {
    let mut conds = serde_json::Map::new();
    conds.insert(
        "agent_did".to_string(),
        serde_json::json!({"_in": ["did:a", "did:b"]}),
    );
    let filter = ReplicationFilter::Predicate(conds);
    let json = serde_json::to_string(&filter).unwrap();
    assert!(
        json.contains("predicate"),
        "predicate variant serializes as tagged"
    );
    let restored: ReplicationFilter = serde_json::from_str(&json).unwrap();
    assert_eq!(filter, restored);
}

#[test]
fn eq_only_matcher_evaluates_filters() {
    use p2p::replicator::EqOnlyFilterMatcher;
    let matcher = EqOnlyFilterMatcher;

    // No filter for collection → true
    let info = ReplicatorInfo::from_raw("peer".to_string(), vec!["users".to_string()], vec![]);
    assert!(info.matches_filter(&matcher, "users", &serde_json::json!({"x": 1})));

    // Matching _eq → true
    let mut filters = ReplicationFilters::new();
    filters.insert(
        "users".to_string(),
        ReplicationFilter::eq("agent_did", serde_json::json!("did:x")),
    );
    let info_filtered = ReplicatorInfo::from_raw_with_filters(
        "peer".to_string(),
        vec!["users".to_string()],
        vec![],
        filters,
    );
    assert!(info_filtered.matches_filter(
        &matcher,
        "users",
        &serde_json::json!({"agent_did": "did:x"})
    ));
    assert!(!info_filtered.matches_filter(
        &matcher,
        "users",
        &serde_json::json!({"agent_did": "did:y"})
    ));
}
