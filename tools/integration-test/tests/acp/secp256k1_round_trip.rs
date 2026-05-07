//! End-to-end ACP round-trip with a secp256k1-keyed identity over P2P.
//!
//! `crates/db-merge/src/peer_identity.rs` ships unit tests that prove a
//! secp256k1 libp2p key recovers the same `did:key:z7r8...` DID Go
//! produces for the same private bytes. Those tests cover the conversion
//! pipeline in isolation; this test exercises the full flow:
//!
//!   secp256k1 owner -> create ACP-protected doc on node0
//!     -> P2P replicate to node1
//!       -> node1 registers doc under owner's secp256k1 DID
//!         -> owner-only ACP operations succeed against node1
//!         -> anonymous queries on node1 are blocked
//!
//! The "owner-only on node1" assertion is the key parity check: granting a
//! reader relationship via `acp_relationship_add` requires the caller to be
//! the registered owner of the document. If the secp256k1 DID survived the
//! round trip — protobuf encoding, gossipsub publish, merge, and ACP
//! registration — only the original owner can grant a relation on node1.
//!
//! Tracks #890.

use std::time::Duration;

use integration_test::{
    generate_identity, poll_until, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

#[tokio::test]
async fn rust_rust_secp256k1_acp_round_trip() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let binary_path = node0.binary_path().to_path_buf();

    // The Rust CLI defaults `defra identity new` to secp256k1 (see
    // crates/cli/src/commands/identity.rs default_value = "secp256k1"),
    // so generate_identity() produces a secp256k1 owner. Pin the format
    // here so the test fails loudly if that default ever flips.
    let alice = generate_identity(&binary_path).expect("generate alice secp256k1 identity");
    assert!(
        alice.did.starts_with("did:key:z7r8"),
        "expected secp256k1 DID prefix did:key:z7r8 but got {}",
        alice.did,
    );

    // A second secp256k1 identity used as the relationship target: granting
    // Bob a reader relation on node1 only succeeds if node1 has Alice
    // registered as the owner of the document.
    let bob = generate_identity(&binary_path).expect("generate bob secp256k1 identity");
    assert!(bob.did.starts_with("did:key:z7r8"));

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 p2p did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 p2p did not start");

    let info1 = node1.p2p_info().expect("node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 p2p address");

    // Mirror the policy + schema on both nodes so node1 can merge the
    // replicated doc against an identical resource definition.
    let policy0 = node0
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add ACP policy on node0");
    let policy_id = policy0["PolicyID"]
        .as_str()
        .or_else(|| policy0["policyID"].as_str())
        .expect("PolicyID from node0 policy add response");
    node1
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add ACP policy on node1");

    let schema = users_schema_with_policy(policy_id);
    node0
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add User schema on node0");
    node1
        .schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add User schema on node1");

    // Wire up explicit replication node0 -> node1 so node1 receives the
    // protected doc through the standard merge path.
    node0.p2p_connect(&[addr1]).expect("p2p connect");
    node0
        .p2p_collection_add(&["User"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["User"])
        .expect("collection add node1");
    node0
        .p2p_replicator_set_with_identity(&["User"], addr1, &alice.private_key_hex)
        .expect("set replicator");

    // Alice creates a protected doc — broadcast Creator field carries her
    // secp256k1 DID, which the recipient registers as the document owner.
    let create_result = node0
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Protected", age: 42}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create protected doc as alice");
    let doc_id = create_result["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID from create response")
        .to_string();

    // Wait until Alice can read the replicated doc on node1. This proves
    // both that the document arrived AND that node1 registered her secp256k1
    // DID as owner (otherwise her authenticated query would return zero rows).
    let node1_ref = &node1;
    let alice_key = alice.private_key_hex.clone();
    poll_until(
        || {
            node1_ref
                .query_with_identity("query { User { _docID name } }", &alice_key)
                .ok()
                .and_then(|v| v["User"].as_array().map(|arr| !arr.is_empty()))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "protected doc did not replicate to node1 under alice's secp256k1 identity",
    )
    .await;

    // Strongest behavioral assertion: the relationship grant call requires
    // the caller to be the document's owner. If the secp256k1 DID survived
    // the round trip and node1 registered Alice correctly, this succeeds.
    // If node1 registered "anonymous peer" or some other DID, this errors.
    node1
        .acp_relationship_add("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect(
            "alice (secp256k1 owner) must be able to grant reader on node1 — \
             requires node1 to have registered her DID as the document owner",
        );

    // Cross-check from Bob's side: the grant is meaningless unless it
    // actually unlocked read access on node1.
    let bob_view_after_grant = node1
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("bob query after grant");
    let bob_users_after = bob_view_after_grant["User"]
        .as_array()
        .expect("bob User result not array");
    assert_eq!(
        bob_users_after.len(),
        1,
        "bob (granted reader on node1) must see the protected doc"
    );
    assert_eq!(bob_users_after[0]["name"], "Protected");

    // And the negative half: anonymous queries on node1 are still blocked,
    // confirming the document remains ACP-gated rather than slipping through
    // as a public record.
    let anon_view = node1
        .query("query { User { _docID name } }")
        .expect("anonymous query on node1");
    let anon_users = anon_view["User"]
        .as_array()
        .expect("anon User result not array");
    assert!(
        anon_users.is_empty(),
        "anonymous reader must not see the secp256k1-owned protected doc; got {:?}",
        anon_users
    );
}
