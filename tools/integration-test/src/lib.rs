use std::path::PathBuf;

pub mod client;
pub mod cluster;
pub mod fixtures;
pub mod identity;
pub mod node;
pub mod observe;
pub mod poll;
pub mod ports;
pub mod process;
pub mod run;

pub use client::DefraClient;
pub use cluster::{TestCluster, TestClusterBuilder};
pub use fixtures::{
    documents_schema_with_policy, users_schema_with_policy, MULTI_ROLE_ACP_POLICY, PRODUCT_SCHEMA,
    USER_ACP_POLICY,
};
pub use identity::{generate_ed25519_identity, generate_identity, TestIdentity};
pub use poll::poll_until;

/// Return the absolute path to the workspace root.
///
/// Derived from CARGO_MANIFEST_DIR (tools/integration-test) at compile time.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("failed to canonicalize workspace root")
}
