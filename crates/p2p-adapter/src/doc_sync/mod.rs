//! Document-sync operations, extracted so their peer-dispatch step can be
//! substituted in tests. Mirrors the `manage` module's layout.

pub(crate) mod dispatch;
#[cfg(feature = "libp2p")]
pub(crate) mod pubsub_replies;
pub(crate) mod sync;
#[cfg(test)]
pub(crate) mod test_support;

/// Deadline applied when the caller supplies none.
///
/// Go uses 5s in the same situation: `syncDocuments` gives its wait context
/// that timeout whenever the inherited one has no deadline
/// (`internal/db/p2p/sync_doc.go:123-128`), so a caller that sends no
/// `timeout` gets the same budget from either implementation.
pub(crate) const DEFAULT_DOC_SYNC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
