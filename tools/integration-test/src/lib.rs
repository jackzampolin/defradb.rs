use std::path::PathBuf;

pub mod client;
pub mod cluster;
pub mod fixtures;
pub mod identity;
pub mod node;
pub mod observe;
pub mod p2p_helpers;
pub mod poll;
pub mod ports;
pub mod process;
pub mod run;
pub mod sourcehub;
pub mod sse;
pub mod wasm_lens;

pub use client::DefraClient;
pub use cluster::{TestCluster, TestClusterBuilder};
pub use fixtures::{
    documents_schema_with_policy, interaction_schema_with_policy, multi_resource_policy,
    peak_schema_with_policy, secret_schema_with_policy, tweet_schema_with_policy, typed_schema,
    users_schema_with_policy, workout_schema_with_policy, HIKING_ACP_POLICY, MULTI_ROLE_ACP_POLICY,
    PRODUCT_SCHEMA, SECRET_ACP_POLICY, STANDARD_FIELDS, USER_ACP_POLICY, XARCHIVE_ACP_POLICY,
};
pub use identity::{
    generate_ed25519_identity, generate_identity, generate_secp256r1_identity, TestIdentity,
};
pub use p2p_helpers::{
    extract_doc_id, extract_p2p_addr, extract_p2p_addr_with_identity, extract_peer_id,
    setup_three_node_chain, setup_two_node_iroh, setup_two_node_replicated, wait_for_doc_count,
    wait_for_field_values, P2P_POLL_INTERVAL, P2P_TIMEOUT,
};
pub use poll::poll_until;
pub use sse::{
    open_events_sse, open_merge_events_sse, open_peer_events_sse, wait_for_merge_events,
    wait_for_peer_events,
};

/// Return the absolute path to the workspace root.
///
/// Derived from CARGO_MANIFEST_DIR (tools/integration-test) at compile time.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to canonicalize workspace root")
}

/// Generate `rust_<name>` and `go_<name>` test wrappers for a single-node test.
///
/// Usage: `for_each_runtime!(name, inner_fn);` (bare builder)
///        `for_each_runtime!(name, inner_fn, .with_acp_local());` (with modifiers)
#[macro_export]
macro_rules! for_each_runtime {
    ($name:ident, $inner:ident) => {
        ::paste::paste! {
            #[tokio::test]
            async fn [<rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(1).build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_ $name>]() {
                let cluster = $crate::TestCluster::builder().go_nodes(1).build().await.unwrap();
                $inner(cluster).await;
            }
        }
    };
    ($name:ident, $inner:ident, $($modifier:tt)+) => {
        ::paste::paste! {
            #[tokio::test]
            async fn [<rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(1) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_ $name>]() {
                let cluster = $crate::TestCluster::builder().go_nodes(1) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }
        }
    };
}

/// Generate `rust_rust_<name>`, `go_go_<name>`, and `go_rust_<name>` test wrappers
/// for a multi-node P2P test.
///
/// Usage: `for_each_p2p_topology!(name, inner_fn, .with_p2p());`
#[macro_export]
macro_rules! for_each_p2p_topology {
    ($name:ident, $inner:ident, $($modifier:tt)+) => {
        ::paste::paste! {
            #[tokio::test]
            async fn [<rust_rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(2) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_go_ $name>]() {
                let cluster = $crate::TestCluster::builder().go_nodes(2) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(1).go_nodes(1) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }
        }
    };
}

/// Generate `rust_rust_<name>`, `go_go_<name>`, and `go_rust_<name>` test wrappers
/// for a 3-node P2P test (node0=target, node1=flooder, node2=legitimate).
///
/// Usage: `for_each_p2p_topology_3!(name, inner_fn, .with_p2p());`
#[macro_export]
macro_rules! for_each_p2p_topology_3 {
    ($name:ident, $inner:ident, $($modifier:tt)+) => {
        ::paste::paste! {
            #[tokio::test]
            async fn [<rust_rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(3) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_go_ $name>]() {
                let cluster = $crate::TestCluster::builder().go_nodes(3) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(2).go_nodes(1) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }
        }
    };
}

/// Like `for_each_p2p_topology!` but marks all generated tests as `#[ignore]`.
/// Use for stress tests that are too resource-intensive for normal CI runs.
///
/// Run with: `cargo test -p integration-test --test <binary> -- --ignored`
#[macro_export]
macro_rules! for_each_p2p_topology_ignored {
    ($name:ident, $inner:ident, $($modifier:tt)+) => {
        ::paste::paste! {
            #[tokio::test]
            #[ignore]
            async fn [<rust_rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(2) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            #[ignore]
            async fn [<go_go_ $name>]() {
                let cluster = $crate::TestCluster::builder().go_nodes(2) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            #[ignore]
            async fn [<go_rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(1).go_nodes(1) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }
        }
    };
}

/// Like `for_each_p2p_topology_3!` but marks all generated tests as `#[ignore]`.
/// Use for stress tests that are too resource-intensive for normal CI runs.
///
/// Run with: `cargo test -p integration-test --test <binary> -- --ignored`
#[macro_export]
macro_rules! for_each_p2p_topology_3_ignored {
    ($name:ident, $inner:ident, $($modifier:tt)+) => {
        ::paste::paste! {
            #[tokio::test]
            #[ignore]
            async fn [<rust_rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(3) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            #[ignore]
            async fn [<go_go_ $name>]() {
                let cluster = $crate::TestCluster::builder().go_nodes(3) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            #[ignore]
            async fn [<go_rust_ $name>]() {
                let cluster = $crate::TestCluster::builder().rust_nodes(2).go_nodes(1) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }
        }
    };
}
