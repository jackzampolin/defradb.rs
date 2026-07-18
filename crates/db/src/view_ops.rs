//! Materialized view cache operations.
//!
//! This module contains operations for refreshing and managing
//! materialized view caches.

use crate::error::{Error, Result};
use datastore::NamespaceView;
use schema::CollectionVersion;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::datastore::ViewCacheKey;
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;
#[cfg(not(target_arch = "wasm32"))]
use tokio::time::{self, Instant, MissedTickBehavior};

/// Options for refreshing materialized views.
#[derive(Debug, Clone, Default)]
pub struct RefreshViewsOptions {
    /// Only refresh views with these names (None = all views)
    pub names: Option<Vec<String>>,
}

impl RefreshViewsOptions {
    /// Create options that refresh all views.
    pub fn all() -> Self {
        Self { names: None }
    }

    /// Create options that refresh only the named views.
    pub fn with_names(names: Vec<String>) -> Self {
        Self { names: Some(names) }
    }
}

/// Scheduled refresh metadata for a downsampled materialized view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledViewRefresh {
    pub name: String,
    pub interval: Duration,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
struct ScheduledViewState {
    interval: Duration,
    next_refresh: Instant,
}

fn legacy_downsample_refresh_interval(raw: &str) -> Option<Duration> {
    let trimmed = raw.trim();
    let secs = trimmed
        .strip_suffix('s')
        .unwrap_or(trimmed)
        .parse::<u64>()
        .ok()?;
    Some(Duration::from_secs(secs))
}

/// Delete all keys matching a prefix from a namespace view.
async fn delete_prefix(store: &NamespaceView, prefix: Vec<u8>) -> Result<()> {
    let opts = IterOptions::new().with_prefix(prefix);
    let mut iter = store.iterator(opts).await.map_err(Error::Storage)?;
    let mut keys_to_delete = Vec::new();
    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
        keys_to_delete.push(pair.key.to_vec());
    }
    iter.close().await.map_err(Error::Storage)?;
    for key in keys_to_delete {
        store.delete(&key).await.map_err(Error::Storage)?;
    }
    Ok(())
}

impl<S: Store> crate::database::DB<S> {
    /// Refresh all materialized views matching the options.
    ///
    /// This clears and rebuilds the view cache for each materialized view.
    /// If options.names is provided, only views with matching names are refreshed.
    /// Otherwise, all materialized views are refreshed.
    pub async fn refresh_views(&self, options: Option<RefreshViewsOptions>) -> Result<()>
    where
        S: 'static,
    {
        // Get all collections
        let collections = self.get_all_active_collections_internal()?;

        // Filter to materialized views (excluding embedded-only types which can't be queried)
        let names_filter = options.as_ref().and_then(|o| o.names.as_ref());
        let views_to_refresh: Vec<_> = collections
            .iter()
            .filter(|col| col.query.is_some() && col.is_materialized && !col.is_embedded_only)
            .filter(|col| {
                names_filter
                    .map(|names| names.contains(&col.name))
                    .unwrap_or(true)
            })
            .collect();

        for view in views_to_refresh {
            let action_execution = self
                .register_action(&view.collection_id, crate::Action::REFRESH_DATASTORE)
                .await?;

            let result: Result<()> = async {
                self.clear_view_cache(view.root_id).await?;
                self.build_view_cache(view).await
            }
            .await;

            if let Err(error) = result {
                if let Err(action_error) =
                    self.fail_action(action_execution, &error.to_string()).await
                {
                    tracing::error!(
                        error = %action_error,
                        collection_id = %view.collection_id,
                        "Failed to record view refresh action error"
                    );
                }
                return Err(error);
            }

            self.complete_action(action_execution).await?;
        }

        Ok(())
    }

    /// Return all active downsampled materialized views with their refresh interval.
    pub fn scheduled_view_refreshes(&self) -> Result<Vec<ScheduledViewRefresh>> {
        let mut scheduled_views: Vec<_> = self
            .get_all_active_collections_internal()?
            .into_iter()
            .filter_map(|col| {
                col.downsample_interval
                    .as_deref()
                    .and_then(legacy_downsample_refresh_interval)
                    .and_then(|interval| {
                        if col.query.is_some() && col.is_materialized && !col.is_embedded_only {
                            Some(ScheduledViewRefresh {
                                name: col.name,
                                interval,
                            })
                        } else {
                            None
                        }
                    })
            })
            .collect();
        scheduled_views.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(scheduled_views)
    }

    /// Spawn a background task that periodically refreshes downsampled materialized views.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_scheduled_view_refresh_task(self: Arc<Self>) -> JoinHandle<()>
    where
        S: 'static,
    {
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(250));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut schedules: HashMap<String, ScheduledViewState> = HashMap::new();

            loop {
                ticker.tick().await;

                let scheduled_views = match self.scheduled_view_refreshes() {
                    Ok(views) => views,
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "Failed to load scheduled downsample views"
                        );
                        continue;
                    }
                };

                let now = Instant::now();
                let active_names: HashSet<String> = scheduled_views
                    .iter()
                    .map(|view| view.name.clone())
                    .collect();
                let mut due_names = Vec::new();

                for view in scheduled_views {
                    match schedules.entry(view.name.clone()) {
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(ScheduledViewState {
                                interval: view.interval,
                                next_refresh: now + view.interval,
                            });
                        }
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            let state = entry.get_mut();
                            if state.interval != view.interval {
                                state.interval = view.interval;
                                state.next_refresh = now + view.interval;
                            }
                            if state.next_refresh <= now {
                                due_names.push(view.name.clone());
                                state.next_refresh = now + state.interval;
                            }
                        }
                    }
                }

                schedules.retain(|name, _| active_names.contains(name));

                for name in due_names {
                    if let Err(error) = self
                        .refresh_views(Some(RefreshViewsOptions::with_names(vec![name.clone()])))
                        .await
                    {
                        tracing::warn!(
                            view = %name,
                            error = %error,
                            "Failed to refresh scheduled downsample view"
                        );
                    }
                }
            }
        })
    }

    /// Clear the view cache for a collection.
    async fn clear_view_cache(&self, collection_id: u32) -> Result<()> {
        let txn = self.new_txn(false).await?;

        // Scope the datastore lifetime - must be dropped before commit
        {
            let datastore = txn.datastore()?;
            let prefix = ViewCacheKey::collection_prefix(collection_id);
            delete_prefix(&datastore, prefix).await?;
        }

        txn.commit().await?;
        Ok(())
    }

    /// Build the view cache for a materialized view.
    ///
    /// This executes the view's underlying query and stores the results in the cache.
    /// We temporarily set is_materialized=false to execute the live query.
    async fn build_view_cache(&self, collection: &CollectionVersion) -> Result<()>
    where
        S: 'static,
    {
        let query_source = match &collection.query {
            Some(qs) => qs,
            None => return Ok(()),
        };

        // Get all collections and create a modified version of this view
        // with is_materialized=false so the query will execute live
        let collections = self.get_all_active_collections_internal()?;
        let modified_collections: Vec<CollectionVersion> = collections
            .into_iter()
            .map(|mut col| {
                if col.name == collection.name {
                    col.is_materialized = false;
                }
                col
            })
            .collect();

        // Build the view query string
        let query_str = format!(
            "query {{ {} {{ {} }} }}",
            collection.name,
            self.build_view_fields_from_source(&query_source.query)?
        );

        // Execute the view query
        let txn = self.new_txn(true).await?;
        let fetcher = crate::doc_fetcher::DbDocFetcher::new(txn);

        // Keep a handle to the transaction mutex so we can discard it after the query
        let txn_handle = fetcher.shared_txn();

        // Build query runner with modified collection (is_materialized=false)
        let query_runner = query::QueryRunner::new(fetcher, modified_collections)
            .with_lens_store(self.lens_store.clone());

        let results = query_runner
            .execute_query(&query_str)
            .await
            .map_err(|e| Error::Other(format!("failed to execute view query: {}", e)))?;

        // Drop the query runner to release its reference to the fetcher
        drop(query_runner);

        // Explicitly discard the read transaction before starting write transaction
        // This releases all references to the underlying read transaction
        if let Some(read_txn) = txn_handle.lock().await.take() {
            let _ = read_txn.force_discard();
        }

        // Store results in cache
        let write_txn = self.new_txn(false).await?;

        // Scope the datastore lifetime - must be dropped before commit
        {
            let datastore = write_txn.datastore()?;

            // Parse results and store each item
            // Results are: { "ViewName": [ {...}, {...}, ... ] }
            if let Some(items) = results.get(&collection.name).and_then(|v| v.as_array()) {
                for (idx, item) in items.iter().enumerate() {
                    let key = ViewCacheKey::new(collection.root_id, idx as u64);
                    let value = serde_json::to_vec(item).map_err(|e| {
                        Error::Other(format!("failed to serialize view item: {}", e))
                    })?;
                    datastore
                        .set(&key.bytes(), &value)
                        .await
                        .map_err(Error::Storage)?;
                }
            }
        }

        write_txn.commit().await?;
        Ok(())
    }

    /// Build a field list for the view query from the stored Select JSON.
    fn build_view_fields_from_source(&self, query: &serde_json::Value) -> Result<String> {
        let source_fields = query
            .get("Fields")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                Error::Other("view QuerySource.Query missing 'Fields' array".to_string())
            })?;

        let mut fields = Vec::new();
        for field_json in source_fields {
            let field_name = field_json
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let alias = field_json.get("Alias").and_then(|v| v.as_str());

            if let Some(inner_fields) = field_json.get("Fields").and_then(|v| v.as_array()) {
                // Nested relation
                let mut inner_field_strs: Vec<String> = Vec::new();
                for inner in inner_fields {
                    let name = inner
                        .get("Name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();
                    let inner_alias = inner.get("Alias").and_then(|v| v.as_str());
                    if let Some(a) = inner_alias {
                        inner_field_strs.push(format!("{}: {}", a, name));
                    } else {
                        inner_field_strs.push(name.to_string());
                    }
                }
                fields.push(format!(
                    "{} {{ {} }}",
                    field_name,
                    inner_field_strs.join(" ")
                ));
            } else if let Some(a) = alias {
                fields.push(format!("{}: {}", a, field_name));
            } else {
                fields.push(field_name.to_string());
            }
        }

        Ok(fields.join(" "))
    }

    /// Get all active collections as CollectionVersion objects (internal helper).
    fn get_all_active_collections_internal(&self) -> Result<Vec<CollectionVersion>> {
        let cache = self
            .collections
            .read()
            .map_err(|_| Error::Other("failed to acquire collections lock".to_string()))?;
        Ok(cache.values().map(|c| c.schema().clone()).collect())
    }
}
