//! `RefreshViewsOptions` selects the views Go's collection lookup would.
//!
//! Go's `RefreshViews` selects with `options.GetCollectionsOptions`, resolved
//! by `getCollections` (`internal/db/collection.go`). That runs in two stages:
//! a switch that picks the candidate set, then a filter over it. The switch is
//! what makes `collection_id` yield to a name or a version, and what lets a
//! name plus `get_inactive` reach an inactive version at all. A single flat
//! AND over the four selectors gets both wrong.

use db::{is_refreshable_view, RefreshViewsOptions};
use schema::{CollectionVersion, QuerySource};

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

fn inactive(name: &str, version_id: &str, collection_id: &str) -> CollectionVersion {
    let mut version = view(name, version_id, collection_id);
    version.is_active = false;
    version
}

#[test]
fn no_selectors_select_everything_active() {
    let options = RefreshViewsOptions::all();
    assert!(options.selects(&view("Orders", "v1", "c1")));
    assert!(!options.needs_all_versions());
}

#[test]
fn no_selectors_skip_inactive_versions() {
    assert!(!RefreshViewsOptions::all().selects(&inactive("Orders", "v1", "c1")));
}

#[test]
fn names_select_only_the_named_views() {
    let options = RefreshViewsOptions::with_names(vec!["Orders".to_string()]);
    assert!(options.selects(&view("Orders", "v1", "c1")));
    assert!(!options.selects(&view("Invoices", "v2", "c2")));
}

#[test]
fn a_version_id_selects_only_that_version() {
    let options = RefreshViewsOptions {
        version_id: Some("v1".to_string()),
        ..RefreshViewsOptions::all()
    };
    assert!(options.selects(&view("Orders", "v1", "c1")));
    assert!(!options.selects(&view("Orders", "v2", "c1")));
}

/// Go exempts a requested version from the inactive drop, so asking for a
#[test]
fn a_collection_id_selects_that_collections_views() {
    let options = RefreshViewsOptions {
        collection_id: Some("c1".to_string()),
        ..RefreshViewsOptions::all()
    };
    assert!(options.selects(&view("Orders", "v1", "c1")));
    assert!(!options.selects(&view("Invoices", "v2", "c2")));
}

/// `collection_id` picks candidates in Go rather than filtering them, so a
/// name selection takes precedence and a disagreeing collection id is ignored.
/// A flat AND over all four selectors would select nothing here.
#[test]
fn a_name_wins_over_a_disagreeing_collection_id() {
    let options = RefreshViewsOptions {
        names: Some(vec!["Orders".to_string()]),
        collection_id: Some("other".to_string()),
        ..RefreshViewsOptions::all()
    };
    assert!(options.selects(&view("Orders", "v1", "c1")));
}

/// Same rule for a version selection: it is picked in stage 1, ahead of any
/// collection id.
#[test]
fn a_version_id_wins_over_a_disagreeing_collection_id() {
    let options = RefreshViewsOptions {
        version_id: Some("v1".to_string()),
        collection_id: Some("other".to_string()),
        ..RefreshViewsOptions::all()
    };
    assert!(options.selects(&view("Orders", "v1", "c1")));
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
        RefreshViewsOptions::with_names(vec!["OrdersView".to_string()]).selects(&embedded),
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
        .refresh_views(RefreshViewsOptions {
            version_id: Some("bae-does-not-exist".to_string()),
            ..RefreshViewsOptions::all()
        })
        .await
        .expect_err("an unknown version must be reported");

    assert!(
        error.to_string().contains("bae-does-not-exist"),
        "the error must name the version: {error}"
    );
}

#[tokio::test]
async fn an_unknown_collection_id_is_an_error() {
    let db = db::DB::open(storage::backends::MemoryStore::new())
        .await
        .expect("open");

    let error = db
        .refresh_views(RefreshViewsOptions {
            collection_id: Some("no-such-collection".to_string()),
            ..RefreshViewsOptions::all()
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

    db.refresh_views(RefreshViewsOptions {
        get_inactive: true,
        ..RefreshViewsOptions::all()
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
        .refresh_views(RefreshViewsOptions {
            get_inactive: true,
            ..RefreshViewsOptions::all()
        })
        .await
        .expect_err("inactive selection must be refused, not silently wrong");

    assert!(
        error.to_string().contains("not supported"),
        "the refusal must say why: {error}"
    );
}
