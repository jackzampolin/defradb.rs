//! The shared backend conformance suite, run against regolith.
//!
//! These tests belonged to every backend the tree used to carry. regolith is
//! the only one now, so it is the only thing that can hold the contract they
//! describe: read-your-writes, iterator direction and bounds, seek, callback
//! ordering, and behaviour under concurrent transactions.

use std::sync::Arc;
use storage::RegolithStore;

async fn store() -> RegolithStore {
    RegolithStore::in_memory().expect("in-memory regolith store")
}

async fn arc_store() -> Arc<RegolithStore> {
    Arc::new(store().await)
}

storage::generate_backend_tests!(store);
storage::generate_backend_concurrency_tests!(arc_store);
storage::generate_backend_dropable_tests!(store);
