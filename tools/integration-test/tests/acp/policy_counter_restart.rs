//! Policy-ID counter must survive a node restart (#1448).
//!
//! Go's `acp_core` persists the counter that salts each policy-ID hash, so
//! re-registering identical policy YAML after a restart yields a *fresh* id. Go
//! pins this itself in
//! `Test_LocalACP_PersistentMemory_AddPolicy_CreatingSamePolicyReturnsDifferentIDs`
//! (`acp/dac/local_test.go`). Rust previously reset an in-memory counter on
//! every start and returned the original id, silently adopting the earlier
//! policy's identity.
//!
//! These run under `for_each_runtime!`, so the Go node acts as the oracle rather
//! than the assertion merely restating Rust's own behavior.
//!
//! A persistent store is required: the harness defaults to `--store memory`,
//! where the ACP backend is a `MemoryZanzibarStore` and a restart wipes the
//! counter along with everything else, so the divergence cannot appear.
//!
//! NAC is left off. It stores its policy in the same key space, and the
//! counter's absent-key seeding deliberately skips it, which is covered by a
//! unit test rather than here.

use std::time::Duration;

use integration_test::{for_each_runtime, generate_identity, TestCluster, USER_ACP_POLICY};

fn policy_id_of(result: &serde_json::Value) -> String {
    result["PolicyID"]
        .as_str()
        .or_else(|| result["policyID"].as_str())
        .expect("missing PolicyID in policy add result")
        .to_string()
}

async fn policy_counter_survives_restart_test(mut cluster: TestCluster) {
    let node = cluster.client(0);
    let binary_path = node.binary_path().to_path_buf();
    let alice = generate_identity(&binary_path).expect("failed to generate identity");

    let before = policy_id_of(
        &node
            .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
            .expect("failed to add policy before restart"),
    );

    cluster
        .restart_node(0, Duration::from_secs(30))
        .await
        .expect("restart node");

    let node = cluster.client(0);
    let after = policy_id_of(
        &node
            .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
            .expect("failed to add policy after restart"),
    );

    assert_ne!(
        before, after,
        "re-registering identical policy YAML after a restart must mint a new \
         policy id; the same id twice means the counter reset"
    );
}

/// Guards the other half of the contract: the counter advances within a single
/// session, so the restart assertion cannot pass for the wrong reason.
async fn policy_counter_advances_in_session_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary_path = node.binary_path().to_path_buf();
    let alice = generate_identity(&binary_path).expect("failed to generate identity");

    let first = policy_id_of(
        &node
            .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
            .expect("failed to add first policy"),
    );
    let second = policy_id_of(
        &node
            .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
            .expect("failed to add second policy"),
    );

    assert_ne!(
        first, second,
        "identical policy YAML submitted twice in one session must mint distinct ids"
    );
}

for_each_runtime!(
    policy_counter_survives_restart,
    policy_counter_survives_restart_test,
    // The harness hands one `--store` string to both binaries, and this test
    // restarts a node so it cannot use `memory`. `badger` is the only name Go
    // and Rust both take.
    .with_acp_local().with_store("badger")
);

for_each_runtime!(
    policy_counter_advances_in_session,
    policy_counter_advances_in_session_test,
    // The harness hands one `--store` string to both binaries, and this test
    // restarts a node so it cannot use `memory`. `badger` is the only name Go
    // and Rust both take.
    .with_acp_local().with_store("badger")
);
