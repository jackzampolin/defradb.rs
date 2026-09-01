use std::path::PathBuf;

mod cross_runtime;
mod feature_binaries;

pub use cross_runtime::{assert_query_equivalent, query_both};
pub use feature_binaries::build_cli_variant;

// Re-export modules from defra-harness
pub use defra_harness::client;
pub use defra_harness::cluster;
pub use defra_harness::divergences;
pub use defra_harness::fixtures;
pub use defra_harness::identity;
pub use defra_harness::node;
pub use defra_harness::observe;
pub use defra_harness::poll;
pub use defra_harness::ports;

// Convenience re-exports
pub use defra_harness::fixtures::*;
pub use defra_harness::identity::*;
pub use defra_harness::poll::poll_until;
pub use defra_harness::BinarySource;
pub use defra_harness::DefraClient;
pub use defra_harness::NodeKind;
pub use defra_harness::TestCluster;
pub use defra_harness::TestClusterBuilder;

// Re-export modules moved to backbone
pub use defra_harness::p2p_helpers;
pub use defra_harness::sse;

pub use defra_harness::p2p_helpers::{
    extract_doc_id, extract_p2p_addr, extract_p2p_addr_with_identity, extract_peer_id,
    setup_three_node_chain, setup_two_node_iroh, setup_two_node_replicated, wait_for_doc_count,
    wait_for_field_values, P2P_POLL_INTERVAL, P2P_TIMEOUT,
};
pub use defra_harness::sse::{
    open_events_sse, open_merge_events_sse, open_peer_events_sse, wait_for_merge_events,
    wait_for_peer_events,
};
pub use defra_harness::WasmLens;

/// Provides a `WasmLens` configured for the defradb.rs test-lenses directory.
pub mod wasm_lens {
    use crate::workspace_root;
    use defra_harness::WasmLens;

    pub fn wasm_lens_defra() -> WasmLens {
        WasmLens::new(workspace_root().join("tools/integration-test/test-lenses/set_default"))
    }
}

#[ctor::ctor]
fn init_workspace_root() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to canonicalize workspace root");
    std::env::set_var("DEFRA_WORKSPACE_ROOT", &root);
}

/// Return the absolute path to the defradb.rs workspace root.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to canonicalize workspace root")
}

/// Return the configured Rust CLI binary, falling back to Cargo's debug output.
pub fn rust_binary() -> PathBuf {
    std::env::var_os("DEFRA_RUST_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target/debug/defra"))
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
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().rust_nodes(1).build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_ $name>]() {
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().go_nodes(1).build().await.unwrap();
                $inner(cluster).await;
            }
        }
    };
    ($name:ident, $inner:ident, $($modifier:tt)+) => {
        ::paste::paste! {
            #[tokio::test]
            async fn [<rust_ $name>]() {
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().rust_nodes(1) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_ $name>]() {
                let _root = $crate::workspace_root();
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
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().rust_nodes(2) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_go_ $name>]() {
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().go_nodes(2) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_rust_ $name>]() {
                let _root = $crate::workspace_root();
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
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().rust_nodes(3) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_go_ $name>]() {
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().go_nodes(3) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            async fn [<go_rust_ $name>]() {
                let _root = $crate::workspace_root();
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
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().rust_nodes(2) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            #[ignore]
            async fn [<go_go_ $name>]() {
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().go_nodes(2) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            #[ignore]
            async fn [<go_rust_ $name>]() {
                let _root = $crate::workspace_root();
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
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().rust_nodes(3) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            #[ignore]
            async fn [<go_go_ $name>]() {
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().go_nodes(3) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }

            #[tokio::test]
            #[ignore]
            async fn [<go_rust_ $name>]() {
                let _root = $crate::workspace_root();
                let cluster = $crate::TestCluster::builder().rust_nodes(2).go_nodes(1) $($modifier)+ .build().await.unwrap();
                $inner(cluster).await;
            }
        }
    };
}
