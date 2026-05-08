use std::time::Duration;

use identity::{Identity, IdentityKeyType, RawIdentity};
use integration_test::{TestCluster, TestIdentity};

const FUNDED_PRIVATE_KEYS: [&str; 1] =
    ["ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"];

pub fn funded_identity(index: usize) -> TestIdentity {
    let private_key_hex = FUNDED_PRIVATE_KEYS[index].to_string();
    let key_bytes = hex::decode(&private_key_hex).expect("funded key hex");
    let identity = RawIdentity::from_identity_key_type(IdentityKeyType::Secp256k1, &key_bytes)
        .expect("funded identity");

    TestIdentity {
        private_key_hex,
        did: identity.did().expect("funded identity did").to_string(),
        public_key_hex: Some(hex::encode(identity.public_key_bytes())),
        key_type: Some("secp256k1".to_string()),
    }
}

/// ACP precompile address on hub.rs (0x0810).
const ACP_PRECOMPILE: &str = "0x0000000000000000000000000000000000000810";

/// Function selector for `getPolicy(bytes32)`: keccak256("getPolicy(bytes32)")[:4].
const GET_POLICY_SELECTOR: &str = "a3f685f9";

/// Start a 1-node hub.rs devnet cluster and wait for it to be healthy.
pub async fn start_hub_cluster() -> hub_harness::cluster::TestCluster {
    let cluster = hub_harness::cluster::TestCluster::builder()
        .nodes(1)
        .build()
        .await
        .expect("start hub.rs cluster");
    cluster
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("hub.rs cluster ready");
    cluster
}

/// Build a DefraDB test cluster configured to use hub.rs for document ACP.
///
/// Sets `DEFRA_HUB_RS_ADDRESS` and `DEFRA_ACP_DOCUMENT_TYPE` env vars before
/// spawning nodes (the CLI reads these at startup). Env vars are cleared after
/// the cluster is built so they don't leak to other tests.
pub async fn build_defra_with_hub_rs(
    hub_rpc_url: &str,
    identity: &str,
    n_nodes: usize,
    p2p: bool,
) -> TestCluster {
    unsafe {
        std::env::set_var("DEFRA_HUB_RS_ADDRESS", hub_rpc_url);
        std::env::set_var("DEFRA_ACP_DOCUMENT_TYPE", "hub-rs");
    }

    let mut builder = TestCluster::builder().rust_nodes(n_nodes).skip_build();
    for index in 0..n_nodes {
        builder = builder.with_node_identity(index, identity.to_string());
    }
    if p2p {
        builder = builder.with_p2p();
    }
    let cluster = builder.build().await.expect("build defra cluster");

    unsafe {
        std::env::remove_var("DEFRA_HUB_RS_ADDRESS");
        std::env::remove_var("DEFRA_ACP_DOCUMENT_TYPE");
    }
    cluster
}

/// Query the ACP precompile directly via `eth_call` to verify a policy exists on-chain.
///
/// Calls `getPolicy(bytes32 policyId)` on the ACP precompile (`0x0810`).
/// Returns `true` if the response contains non-empty bytes (policy exists).
pub async fn policy_exists_on_chain(hub_rpc_url: &str, policy_id: &str) -> bool {
    let pid_hex = policy_id.strip_prefix("0x").unwrap_or(policy_id);
    let pid_padded = format!("{:0>64}", pid_hex);
    let calldata = format!("0x{}{}", GET_POLICY_SELECTOR, pid_padded);

    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{
            "to": ACP_PRECOMPILE,
            "data": calldata,
        }, "latest"]
    });

    let resp: serde_json::Value = client
        .post(hub_rpc_url)
        .json(&body)
        .send()
        .await
        .expect("eth_call request failed")
        .json()
        .await
        .expect("eth_call response parse failed");

    let result = resp["result"].as_str().unwrap_or("0x");
    // Non-empty means policy exists (more than just "0x" or empty ABI-encoded bytes)
    result.len() > 66
}
