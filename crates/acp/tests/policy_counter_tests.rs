//! Policy-ID counter persistence (#1448).
//!
//! Go's `acp_core` keeps the policy-ID counter in its KV store and advances it
//! with a read-modify-write on every create, so re-registering identical policy
//! YAML after a restart yields a fresh id. These tests pin that behavior.

use std::sync::Arc;

use acp::policy_yaml::{generate_policy_id, parse_policy_yaml};
use acp::PersistentZanzibarStore;
use storage::RegolithStore;
use zanzibar::store::{MemoryZanzibarStore, ZanzibarStore};
use zanzibar::types::Policy;

/// The policy used to capture the golden vectors below, byte-for-byte as it was
/// fed to a Go node.
const GO_ORACLE_POLICY: &str = r#"
name: test policy
description: A Valid DefraDB Policy Interface
resources:
  - name: users
    permissions:
      - name: read
        expr: reader + writer
      - name: update
        expr: writer
      - name: delete
        expr: writer
    relations:
      - name: reader
        types:
          - actor
      - name: writer
        types:
          - actor
"#;

/// Ids observed from Go DefraDB v1.0.0 (`3de01484`, acp_core v0.8.1) registering
/// GO_ORACLE_POLICY as the first, second and third policy on a node.
const GO_POLICY_IDS: [&str; 3] = [
    "d2a431a4492a7a75794df940f3d4727ac13c0c80260392d894ea0fac203a2f20",
    "4948314a3ace74f52296a1ee73640a26541ac2b6198cc04c8d78daf39eab3d16",
    "6986a6565d38df9494f930bd7cacfa366ac319a064751fe6188a345dc6a73cb8",
];

/// Our id derivation must agree with Go's byte-for-byte, or persisting the
/// counter fixes nothing.
#[test]
fn policy_id_matches_go_oracle() {
    let parsed = parse_policy_yaml(GO_ORACLE_POLICY).expect("parse oracle policy");

    for (index, expected) in GO_POLICY_IDS.iter().enumerate() {
        let counter = index as u64 + 1;
        assert_eq!(
            &generate_policy_id(&parsed, counter),
            expected,
            "policy id diverged from Go at counter={}",
            counter
        );
    }
}

#[tokio::test]
async fn memory_store_counter_starts_at_one_and_increments() {
    let store = MemoryZanzibarStore::new();

    assert_eq!(store.next_policy_counter().await.unwrap(), 1);
    assert_eq!(store.next_policy_counter().await.unwrap(), 2);
    assert_eq!(store.next_policy_counter().await.unwrap(), 3);
}

#[tokio::test]
async fn persistent_store_counter_starts_at_one_and_increments() {
    let store = PersistentZanzibarStore::from_store(Arc::new(RegolithStore::in_memory().unwrap()));

    assert_eq!(store.next_policy_counter().await.unwrap(), 1);
    assert_eq!(store.next_policy_counter().await.unwrap(), 2);
    assert_eq!(store.next_policy_counter().await.unwrap(), 3);
}

/// The restart case: a new store instance over the same backing store must
/// continue the sequence, not restart at 1.
#[tokio::test]
async fn persistent_store_counter_survives_reopen() {
    let backing = Arc::new(RegolithStore::in_memory().unwrap());

    let before = PersistentZanzibarStore::from_store(backing.clone());
    assert_eq!(before.next_policy_counter().await.unwrap(), 1);
    assert_eq!(before.next_policy_counter().await.unwrap(), 2);
    drop(before);

    let after = PersistentZanzibarStore::from_store(backing);
    assert_eq!(
        after.next_policy_counter().await.unwrap(),
        3,
        "counter must survive reopening the store"
    );
}

/// Stores written before this change have policies but no counter key. Starting
/// from 1 there would re-mint an id that is already in use, so the absent key is
/// seeded from the number of policies already stored.
#[tokio::test]
async fn persistent_store_counter_seeds_from_existing_policies() {
    let store = PersistentZanzibarStore::from_store(Arc::new(RegolithStore::in_memory().unwrap()));

    for id in ["policy-a", "policy-b", "policy-c"] {
        store
            .store_policy(&Policy::new(id, "legacy"))
            .await
            .unwrap();
    }

    assert_eq!(
        store.next_policy_counter().await.unwrap(),
        4,
        "three pre-existing policies means counters 1..3 are spent"
    );
}

/// NAC shares this key space but mints its policy id from a constant rather
/// than the counter, so it must not shift the seed. Go keeps NAC and DAC in
/// separate stores with independent counters, where its first DAC policy is
/// always counter 1.
#[tokio::test]
async fn persistent_store_counter_seed_ignores_the_nac_policy() {
    let store = PersistentZanzibarStore::from_store(Arc::new(RegolithStore::in_memory().unwrap()));

    store
        .store_policy(&Policy::new(
            acp::nac::NODE_POLICY_ID,
            "Node Access Control Policy",
        ))
        .await
        .unwrap();

    assert_eq!(
        store.next_policy_counter().await.unwrap(),
        1,
        "a NAC-enabled node's first DAC policy must still be counter 1"
    );
}

#[tokio::test]
async fn persistent_store_counter_seeds_at_one_when_empty() {
    let store = PersistentZanzibarStore::from_store(Arc::new(RegolithStore::in_memory().unwrap()));

    assert_eq!(store.next_policy_counter().await.unwrap(), 1);
}

/// Two store instances over one backing store are past the reach of the
/// in-process lock, so transaction conflicts must be retried by the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_store_counter_never_repeats_across_instances() {
    let backing = Arc::new(RegolithStore::in_memory().unwrap());

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = PersistentZanzibarStore::from_store(backing.clone());
        handles.push(tokio::spawn(
            async move { store.next_policy_counter().await },
        ));
    }

    let mut issued = Vec::new();
    for handle in handles {
        issued.push(handle.await.unwrap().unwrap());
    }

    issued.sort_unstable();

    assert_eq!(
        issued,
        (1..=8).collect::<Vec<u64>>(),
        "concurrent store instances must each receive a distinct counter value"
    );
}

/// Go's counter is a non-atomic read-modify-write guarded by a per-call mutex,
/// so concurrent creates there can mint colliding ids. Ours must not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_store_counter_is_atomic_under_concurrency() {
    let store = Arc::new(PersistentZanzibarStore::from_store(Arc::new(
        RegolithStore::in_memory().unwrap(),
    )));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store.next_policy_counter().await.unwrap()
        }));
    }

    let mut counters = Vec::new();
    for handle in handles {
        counters.push(handle.await.unwrap());
    }
    counters.sort_unstable();

    assert_eq!(
        counters,
        (1..=8).collect::<Vec<u64>>(),
        "concurrent callers must each receive a distinct counter value"
    );
}
