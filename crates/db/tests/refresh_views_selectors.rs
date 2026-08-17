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
/// version by id reaches it whether or not it is the active one.
#[test]
fn a_version_id_reaches_an_inactive_version_without_get_inactive() {
    let options = RefreshViewsOptions {
        version_id: Some("v1".to_string()),
        ..RefreshViewsOptions::all()
    };
    assert!(options.selects(&inactive("Orders", "v1", "c1")));
    assert!(
        options.needs_all_versions(),
        "an inactive version cannot be found in the active-only listing"
    );
}

#[test]
fn a_collection_id_selects_that_collections_views() {
    let options = RefreshViewsOptions {
        collection_id: Some("c1".to_string()),
        ..RefreshViewsOptions::all()
    };
    assert!(options.selects(&view("Orders", "v1", "c1")));
    assert!(!options.selects(&view("Invoices", "v2", "c2")));
}

/// Go's stage 1 takes the by-name case only when `get_inactive` is false. With
/// it set, selection falls through to the full listing and the name filter
/// narrows it, which is how an inactive version is reached by name.
#[test]
fn a_name_with_get_inactive_reaches_an_inactive_version() {
    let options = RefreshViewsOptions {
        names: Some(vec!["Orders".to_string()]),
        get_inactive: true,
        ..RefreshViewsOptions::all()
    };
    assert!(options.selects(&inactive("Orders", "v1", "c1")));
    assert!(!options.selects(&inactive("Invoices", "v2", "c2")));
    assert!(options.needs_all_versions());
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

/// With `get_inactive`, the by-name case does not fire, so a collection id is
/// back in play alongside the name.
#[test]
fn a_collection_id_applies_alongside_a_name_when_inactive_are_included() {
    let options = RefreshViewsOptions {
        names: Some(vec!["Orders".to_string()]),
        collection_id: Some("c1".to_string()),
        get_inactive: true,
        ..RefreshViewsOptions::all()
    };
    assert!(options.selects(&view("Orders", "v1", "c1")));
    assert!(!options.selects(&view("Orders", "v2", "other")));
}

#[test]
fn get_inactive_selects_inactive_versions() {
    let options = RefreshViewsOptions {
        get_inactive: true,
        ..RefreshViewsOptions::all()
    };
    assert!(options.selects(&inactive("Orders", "v1", "c1")));
    assert!(options.selects(&view("Orders", "v2", "c1")));
    assert!(options.needs_all_versions());
}

/// The active-only listing is the cheaper of the two, so it stays the default
/// for every selection that cannot match an inactive version.
#[test]
fn only_inactive_reaching_selections_need_the_full_listing() {
    assert!(!RefreshViewsOptions::all().needs_all_versions());
    assert!(!RefreshViewsOptions::with_names(vec!["Orders".to_string()]).needs_all_versions());
    assert!(!RefreshViewsOptions {
        collection_id: Some("c1".to_string()),
        ..RefreshViewsOptions::all()
    }
    .needs_all_versions());
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
