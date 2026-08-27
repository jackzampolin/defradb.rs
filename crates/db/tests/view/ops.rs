//! Which views a refresh touches, and which it refuses.
//!
//! The selector precedence itself is `collection::selector`; these are the
//! refresh-specific rules layered on top of it.

use db::is_refreshable_view;
use db::CollectionSelector;
use schema::CollectionVersion;
use schema::QuerySource;

fn view(name: &str, version_id: &str, collection_id: &str) -> CollectionVersion {
    CollectionVersion::new(name, version_id, collection_id, vec![])
}

/// A collection version shaped like a real materialized view.
fn materialized(name: &str) -> CollectionVersion {
    let mut version = view(name, "v1", "c1");
    version.query = Some(QuerySource::new(serde_json::json!({})));
    version.is_materialized = true;
    version
}

/// Eligibility is separate from selection: a version has to be a materialized,
/// queryable view before any selector is consulted.
#[test]
fn only_materialized_views_are_refreshable() {
    assert!(is_refreshable_view(&materialized("OrdersView")));
    assert!(
        !is_refreshable_view(&view("Orders", "v1", "c1")),
        "a plain collection is not a view"
    );

    let mut unmaterialized = materialized("OrdersView");
    unmaterialized.is_materialized = false;
    assert!(
        !is_refreshable_view(&unmaterialized),
        "an unmaterialized view has no cache to rebuild"
    );
}

/// Rust excludes embedded-only views where Go does not. That difference is
/// deliberate, because an embedded-only view cannot be queried, so this pins it
/// rather than leaving it to be read as an oversight.
#[test]
fn an_embedded_only_view_is_never_refreshed() {
    let mut embedded = materialized("OrdersView");
    embedded.is_embedded_only = true;
    assert!(!is_refreshable_view(&embedded));

    assert!(
        CollectionSelector::with_names(vec!["OrdersView".to_string()]).selects(&embedded),
        "the selector still matches it; eligibility is what excludes it"
    );
}

/// A selector that matches nothing is a caller mistake, not an empty refresh.
///
/// Go looks a version up directly and propagates not-found
/// (`internal/db/collection.go:211`), so a typo must not read as success. The
/// filter alone returned `Ok(())` over an empty list.
#[tokio::test]
async fn an_unknown_version_id_is_an_error_not_a_silent_success() {
    let db = db::DB::open(storage::backends::MemoryStore::new())
        .await
        .expect("open");

    let error = db
        .refresh_views(CollectionSelector {
            version_id: Some("bae-does-not-exist".to_string()),
            ..CollectionSelector::all()
        })
        .await
        .expect_err("an unknown version must be reported");

    assert!(
        error.to_string().contains("bae-does-not-exist"),
        "the error must name the version: {error}"
    );
}

#[tokio::test]
async fn a_name_precedes_an_unknown_version_id() {
    let db = db::DB::open(storage::backends::MemoryStore::new())
        .await
        .expect("open");
    db.create_collection(materialized("OrdersView"))
        .await
        .expect("store view");

    db.refresh_views(CollectionSelector {
        names: Some(vec!["OrdersView".to_string()]),
        version_id: Some("bae-does-not-exist".to_string()),
        ..CollectionSelector::all()
    })
    .await
    .expect("the version id only filters the active name candidate");
}

#[tokio::test]
async fn an_unknown_collection_id_is_an_error() {
    let db = db::DB::open(storage::backends::MemoryStore::new())
        .await
        .expect("open");

    let error = db
        .refresh_views(CollectionSelector {
            collection_id: Some("no-such-collection".to_string()),
            ..CollectionSelector::all()
        })
        .await
        .expect_err("an unknown collection must be reported");

    assert!(error.to_string().contains("no-such-collection"), "{error}");
}

#[tokio::test]
async fn get_inactive_is_allowed_when_no_inactive_view_is_selected() {
    let db = db::DB::open(storage::backends::MemoryStore::new())
        .await
        .expect("open");

    db.refresh_views(CollectionSelector {
        get_inactive: true,
        ..CollectionSelector::all()
    })
    .await
    .expect("including inactive candidates must not fail an active-only selection");
}

/// Refusing beats corrupting. `build_view_cache` resolves against the active
/// schemas and matches by name, so refreshing an actually inactive version
/// would clear the shared cache and rebuild it from the active definition.
#[tokio::test]
async fn refreshing_a_selected_inactive_view_is_refused() {
    let db = db::DB::open(storage::backends::MemoryStore::new())
        .await
        .expect("open");
    let mut inactive_view = materialized("InactiveView");
    inactive_view.is_active = false;
    db.create_collection(inactive_view)
        .await
        .expect("store inactive view");

    let error = db
        .refresh_views(CollectionSelector {
            get_inactive: true,
            ..CollectionSelector::all()
        })
        .await
        .expect_err("inactive selection must be refused, not silently wrong");

    assert!(
        error.to_string().contains("not supported"),
        "the refusal must say why: {error}"
    );
}
