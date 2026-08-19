//! A store written with the old DateTime index encoding is rebuilt on open.
//!
//! The encoding changed from one varint of nanoseconds to a marker plus
//! separate seconds and nanos varints. Nothing rebuilt those entries before:
//! the existing reindex path is driven by schema migration, not by a storage
//! format change. An upgraded store therefore answered range queries with
//! new-format bounds against old-format entries, so rows went missing, deletes
//! orphaned their entries, and unique indexes stopped rejecting duplicates.

use storage::backends::MemoryStore;

/// The marker byte every encoded time key starts with, per `encoding::mod`.
const TIME_MARKER: u8 = 8;

/// The encoding this branch replaces: a single varint of nanoseconds.
fn old_format_key(unix_nanos: i64) -> Vec<u8> {
    storage::encoding::encode_varint_ascending(vec![TIME_MARKER], unix_nanos)
}

/// The two encodings genuinely differ, which is what makes a rebuild necessary.
/// Without this the rest of the test could pass for the wrong reason.
#[test]
fn the_old_and_new_encodings_differ() {
    let value = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap();

    let new = storage::field_value::encode_field_value(
        Vec::new(),
        &document::NormalValue::Time(value),
        false,
    )
    .expect("the new encoding must succeed");
    let old = old_format_key(value.timestamp_nanos_opt().expect("in range"));

    assert_ne!(new, old, "a rebuild is only needed if the bytes differ");
}

/// Opening a store with no format stamp rebuilds and then stamps it, so the
/// second open finds the store current instead of rebuilding again.
#[tokio::test]
async fn an_unstamped_store_is_stamped_on_open() {
    use db::migration::index_format::CURRENT_INDEX_FORMAT;

    let store = MemoryStore::new();

    let db = db::DB::open(store.clone()).await.expect("first open");
    assert_eq!(
        db.stored_index_format().await.expect("read the stamp"),
        Some(CURRENT_INDEX_FORMAT),
        "opening must stamp the format it wrote"
    );
    drop(db);

    let db = db::DB::open(store).await.expect("second open");
    assert_eq!(
        db.stored_index_format().await.expect("read the stamp"),
        Some(CURRENT_INDEX_FORMAT),
        "the stamp must survive a reopen"
    );
}

/// A fresh store carries no documents, so the rebuild costs nothing and the
/// stamp still lands.
#[tokio::test]
async fn a_fresh_store_opens_clean() {
    use db::migration::index_format::CURRENT_INDEX_FORMAT;

    let db = db::DB::open(MemoryStore::new()).await.expect("open");
    assert_eq!(
        db.stored_index_format().await.unwrap(),
        Some(CURRENT_INDEX_FORMAT)
    );
}

/// The concrete failure a rebuild prevents: an entry written with the old
/// encoding falls outside the bounds a range query builds with the new one, so
/// the row is invisible rather than merely slow to find.
#[test]
fn an_old_format_entry_falls_outside_new_format_range_bounds() {
    let key_for = |rfc3339: &str| {
        let value = chrono::DateTime::parse_from_rfc3339(rfc3339).unwrap();
        let new = storage::field_value::encode_field_value(
            Vec::new(),
            &document::NormalValue::Time(value),
            false,
        )
        .expect("encodes");
        let old = old_format_key(value.timestamp_nanos_opt().expect("in range"));
        (new, old)
    };

    let (lower, _) = key_for("2024-01-01T00:00:00Z");
    let (upper, _) = key_for("2024-12-31T23:59:59Z");
    let (target_new, target_old) = key_for("2024-06-15T12:00:00Z");

    assert!(
        lower <= target_new && target_new <= upper,
        "the new-format entry is inside the window, as it must be"
    );
    assert!(
        target_old < lower || upper < target_old,
        "the old-format entry for the same instant is outside the window, so a \
         scan over these bounds never returns it"
    );
}
