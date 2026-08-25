//! The collection selectors Go accepts as query parameters.
//!
//! Go's `GetCollections` and `RefreshViews` take the same four selectors and
//! resolve them through one lookup (`getCollections`,
//! `internal/db/collection.go`). This is that query string, parsed once, so
//! the two Rust surfaces cannot drift apart the way two copies would.

use serde::Deserialize;

/// `name`, `version_id`, `collection_id` and `get_inactive`, as Go's client
/// sends them (`http/client.go:422-448`).
///
/// Go keys these off `Query().Has(..)`, not off the value being non-empty
/// (`http/handler_store.go:391-402`), so `?name=` is a name set to the empty
/// string rather than an absent selector. It then selects nothing, which is
/// what an empty `Option` value reproduces here.
#[derive(Debug, Default, Deserialize)]
pub struct CollectionSelectorQuery {
    pub name: Option<String>,
    pub version_id: Option<String>,
    pub collection_id: Option<String>,
    pub get_inactive: Option<bool>,
}

impl CollectionSelectorQuery {
    /// Resolve to the shared lookup, with no extra names.
    pub fn into_selector(self) -> db::CollectionSelector {
        self.into_selector_with(None)
    }

    /// Resolve to the shared lookup, unioning `extra_names` into `name`.
    ///
    /// `RefreshViews` also takes a `Names` body, and both it and the query's
    /// `name` mean "restrict to these", so they union. Neither one widens the
    /// selection back out to everything.
    pub fn into_selector_with(self, extra_names: Option<Vec<String>>) -> db::CollectionSelector {
        let mut names = extra_names;
        if let Some(name) = self.name {
            let names = names.get_or_insert_with(Vec::new);
            if !names.contains(&name) {
                names.push(name);
            }
        }

        db::CollectionSelector {
            names,
            version_id: self.version_id,
            collection_id: self.collection_id,
            get_inactive: self.get_inactive.unwrap_or(false),
        }
    }
}
