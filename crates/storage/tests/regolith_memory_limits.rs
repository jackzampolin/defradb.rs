//! An in-memory store accepts what an on-disk one accepts.
//!
//! `Options::memory()` is sized from the engine's embedded profile, whose
//! 256 KiB value ceiling is a limit a caller can hit rather than a
//! memory-tuning knob. Adopting it wholesale made the in-memory store refuse
//! documents the on-disk store took, which is a different database rather
//! than a faster one. These pin the two to the same limits.

use storage::corekv::{Reader, Store, Writer};
use storage::{RegolithStore, RegolithStoreOptions};

#[tokio::test]
async fn the_memory_profile_keeps_the_server_size_limits() {
    let server = RegolithStoreOptions::new();
    let memory = RegolithStoreOptions::memory();

    assert_eq!(memory.engine.max_value_size, server.engine.max_value_size);
    assert_eq!(memory.engine.max_key_size, server.engine.max_key_size);
}

/// The environment constraints still come from the engine's preset, which is
/// the reason to use it at all: `MemEnv` cannot spawn a compaction worker.
#[tokio::test]
async fn the_memory_profile_keeps_the_environment_constraints() {
    let memory = RegolithStoreOptions::memory();

    assert_eq!(memory.engine.max_background_compactions, 0);
    assert!(!memory.engine.env.capabilities().threads);
}

#[tokio::test]
async fn an_in_memory_store_round_trips_a_value_larger_than_the_embedded_ceiling() {
    let store = RegolithStore::in_memory().unwrap();
    // Comfortably past the 256 KiB the embedded profile allows.
    let value = vec![0xAB; 1024 * 1024];

    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"large", &value).await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    assert_eq!(
        txn.get(b"large").await.unwrap().as_deref(),
        Some(value.as_slice())
    );
}
