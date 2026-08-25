//! `CollectionSelector` picks the collection versions Go's lookup would.
//!
//! Go resolves `options.GetCollectionsOptions` through `getCollections`
//! (`internal/db/collection.go:193-301`) in two stages: a switch that picks
//! the candidate set, then a filter over it. The switch is what makes
//! `collection_id` yield to a name or a version, and what lets a name plus
//! `get_inactive` reach an inactive version at all. A single flat AND over the
//! four selectors gets both wrong, so both cases are pinned here.
//!
//! One lookup serves every surface that takes these selectors, so these rules
//! are shared by `GET /collections` and `POST /view/refresh` alike.

use db::CollectionSelector;
use schema::CollectionVersion;

fn version(name: &str, version_id: &str, collection_id: &str) -> CollectionVersion {
    CollectionVersion::new(name, version_id, collection_id, vec![])
}

fn inactive(name: &str, version_id: &str, collection_id: &str) -> CollectionVersion {
    let mut version = version(name, version_id, collection_id);
    version.is_active = false;
    version
}

#[test]
fn no_selectors_select_everything_active() {
    let options = CollectionSelector::all();
    assert!(options.selects(&version("Orders", "v1", "c1")));
    assert!(!options.needs_all_versions());
}

#[test]
fn no_selectors_skip_inactive_versions() {
    assert!(!CollectionSelector::all().selects(&inactive("Orders", "v1", "c1")));
}

#[test]
fn names_select_only_the_named_versions() {
    let options = CollectionSelector::with_names(vec!["Orders".to_string()]);
    assert!(options.selects(&version("Orders", "v1", "c1")));
    assert!(!options.selects(&version("Invoices", "v2", "c2")));
}

#[test]
fn a_version_id_selects_only_that_version() {
    let options = CollectionSelector {
        version_id: Some("v1".to_string()),
        ..CollectionSelector::all()
    };
    assert!(options.selects(&version("Orders", "v1", "c1")));
    assert!(!options.selects(&version("Orders", "v2", "c1")));
}

/// Go exempts a requested version from the inactive drop, so asking for a
#[test]
fn a_collection_id_selects_that_collections_versions() {
    let options = CollectionSelector {
        collection_id: Some("c1".to_string()),
        ..CollectionSelector::all()
    };
    assert!(options.selects(&version("Orders", "v1", "c1")));
    assert!(!options.selects(&version("Invoices", "v2", "c2")));
}

/// `collection_id` picks candidates in Go rather than filtering them, so a
/// name selection takes precedence and a disagreeing collection id is ignored.
/// A flat AND over all four selectors would select nothing here.
#[test]
fn a_name_wins_over_a_disagreeing_collection_id() {
    let options = CollectionSelector {
        names: Some(vec!["Orders".to_string()]),
        collection_id: Some("other".to_string()),
        ..CollectionSelector::all()
    };
    assert!(options.selects(&version("Orders", "v1", "c1")));
}

/// Same rule for a version selection: it is picked in stage 1, ahead of any
/// collection id.
#[test]
fn a_version_id_wins_over_a_disagreeing_collection_id() {
    let options = CollectionSelector {
        version_id: Some("v1".to_string()),
        collection_id: Some("other".to_string()),
        ..CollectionSelector::all()
    };
    assert!(options.selects(&version("Orders", "v1", "c1")));
}

/// The unnarrowed case is what lets a caller keep its cheaper listing path,
/// so it has to be exactly "no selector set".
#[test]
fn only_an_empty_selector_is_unfiltered() {
    assert!(CollectionSelector::all().is_unfiltered());

    let narrowed = [
        CollectionSelector::with_names(vec!["Users".to_string()]),
        CollectionSelector {
            version_id: Some("v1".to_string()),
            ..CollectionSelector::all()
        },
        CollectionSelector {
            collection_id: Some("c1".to_string()),
            ..CollectionSelector::all()
        },
        CollectionSelector {
            get_inactive: true,
            ..CollectionSelector::all()
        },
    ];
    for selector in narrowed {
        assert!(
            !selector.is_unfiltered(),
            "{selector:?} narrows the listing"
        );
    }
}

/// Reaching an inactive version by name is the case Go's switch exists for:
/// the name-and-active candidate arm is deliberately missed, and the stage-two
/// name filter narrows the everything arm instead.
#[test]
fn a_name_with_get_inactive_reaches_an_inactive_version() {
    let selector = CollectionSelector {
        get_inactive: true,
        ..CollectionSelector::with_names(vec!["Users".to_string()])
    };
    assert!(selector.selects(&inactive("Users", "v1", "c1")));
    assert!(!selector.selects(&inactive("Books", "v1", "c1")));
}

/// `needs_all_versions` decides whether the caller has to load inactive rows
/// at all, so a version id has to imply it: a named version is returned
/// whether or not it is active.
#[test]
fn a_version_id_needs_the_full_listing() {
    assert!(!CollectionSelector::all().needs_all_versions());
    assert!(CollectionSelector {
        version_id: Some("v2".to_string()),
        ..CollectionSelector::all()
    }
    .needs_all_versions());
    assert!(CollectionSelector {
        get_inactive: true,
        ..CollectionSelector::all()
    }
    .needs_all_versions());
}
