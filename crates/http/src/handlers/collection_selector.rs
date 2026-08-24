//! The collection selectors Go accepts as query parameters.
//!
//! Go's `GetCollections` and `RefreshViews` take the same four selectors and
//! resolve them through one lookup (`getCollections`,
//! `internal/db/collection.go`). This is that query string, parsed once, so
//! the two Rust surfaces cannot drift apart the way two copies would.

use serde::Deserialize;

use crate::error::HttpError;

/// `name`, `version_id`, `collection_id` and `get_inactive`, as Go's client
/// sends them (`http/client.go:422-448`).
#[derive(Debug, Default, Deserialize)]
pub struct CollectionSelectorQuery {
    pub name: Option<String>,
    pub version_id: Option<String>,
    pub collection_id: Option<String>,
    pub get_inactive: Option<bool>,
}

/// Refuse a selector that is present but empty.
///
/// `?name=` selects nothing under these rules, while a server that treats an
/// empty value as "unset" answers with everything. Which of those Go does
/// could not be confirmed against its source here, and both silently give the
/// caller something other than what it asked for, which is the bug this
/// selector handling exists to fix. Refusing says so instead of guessing.
fn non_empty(value: Option<String>, field: &str) -> Result<Option<String>, HttpError> {
    match value {
        Some(value) if value.trim().is_empty() => Err(HttpError::BadRequest(format!(
            "'{field}' was sent with an empty value; omit it to select everything"
        ))),
        other => Ok(other),
    }
}

impl CollectionSelectorQuery {
    /// Resolve to the shared lookup, with no extra names.
    pub fn into_selector(self) -> Result<db::CollectionSelector, HttpError> {
        self.into_selector_with(None)
    }

    /// Resolve to the shared lookup, unioning `extra_names` into `name`.
    ///
    /// `RefreshViews` also takes a `Names` body, and both it and the query's
    /// `name` mean "restrict to these", so they union. Neither one widens the
    /// selection back out to everything.
    pub fn into_selector_with(
        self,
        extra_names: Option<Vec<String>>,
    ) -> Result<db::CollectionSelector, HttpError> {
        let mut names = extra_names;
        if let Some(name) = non_empty(self.name, "name")? {
            let names = names.get_or_insert_with(Vec::new);
            if !names.contains(&name) {
                names.push(name);
            }
        }

        Ok(db::CollectionSelector {
            names,
            version_id: non_empty(self.version_id, "version_id")?,
            collection_id: non_empty(self.collection_id, "collection_id")?,
            get_inactive: self.get_inactive.unwrap_or(false),
        })
    }
}
