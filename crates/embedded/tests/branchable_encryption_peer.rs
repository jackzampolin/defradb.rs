//! Guards #1603 on the embedded path: a `@branchable` collection under a DAC
//! policy, a `reader` grant on the collection object to node 1's node identity,
//! a pubsub subscription (no replicator), and an anonymous encrypted create on
//! node 0 that the owner then reads on node 1. Node 1 carries its node identity
//! from construction because node 0's KMS authenticates the key request against
//! it, as Go does (`cbindings/node_new.go` sets it independently of signing).
#![cfg(feature = "libp2p")]

use std::sync::Arc;
use std::time::Duration;

use acp::StorePolicyOptions;
use anyhow::{anyhow, bail, Context, Result};
use defra_core::current_identity::with_scoped_identity;
use embedded::{EmbeddedNode, EmbeddedNodeConfig, Libp2pConfig, SigningConfig, TransportConfig};
use identity::{Did, Identity};
use query::QueryRequest;
use tokio::time::{sleep, Instant};

const POLICY: &str = r#"name: test-user-policy
description: A test policy for user document access control

resources:
  - name: users
    permissions:
      - name: read
        expr: writer + reader
      - name: update
        expr: writer
      - name: delete
        expr: writer
    relations:
      - name: writer
        types:
          - actor
      - name: reader
        types:
          - actor"#;

type Node = EmbeddedNode<storage::RegolithStore>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branchable_grant_syncs_plain_doc_to_peer() -> Result<()> {
    run_scenario(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branchable_grant_syncs_encrypted_doc_to_peer() -> Result<()> {
    run_scenario(true).await
}

async fn run_scenario(encrypt: bool) -> Result<()> {
    let jack = new_identity()?;
    let node1_id = new_identity()?;

    let node0 = build_node(SigningConfig::Disabled).await?;
    let node1 = build_node(SigningConfig::RegisteredIdentity {
        did: node1_id.to_string(),
    })
    .await?;

    let result = scenario(&node0, &node1, &jack, &node1_id, encrypt).await;

    node0.shutdown().await;
    node1.shutdown().await;
    result
}

async fn scenario(
    node0: &Node,
    node1: &Node,
    jack: &Did,
    node1_id: &Did,
    encrypt: bool,
) -> Result<()> {
    let policy_id = add_policy(node0).await?;
    let policy_id1 = add_policy(node1).await?;
    assert_eq!(
        policy_id, policy_id1,
        "local policy ids must match across nodes"
    );

    let sdl = format!(
        r#"type Users @branchable @policy(id: "{policy_id}", resource: "users") {{ name: String  age: Int }}"#
    );
    add_schema(node0, &sdl, jack).await?;
    add_schema(node1, &sdl, jack).await?;

    let collection_id = node0
        .database
        .get_collection("Users")?
        .context("Users collection missing on node 0")?
        .collection_id()
        .to_string();
    grant_collection_reader(node0, jack, node1_id, &policy_id, &collection_id).await?;
    grant_collection_reader(node1, jack, node1_id, &policy_id, &collection_id).await?;

    let p2p0 = node0.p2p().context("node 0 has no p2p")?;
    let p2p1 = node1.p2p().context("node 1 has no p2p")?;
    let addr0 = wait_for_listen_addr(p2p0).await?;
    p2p1.ops()
        .connect_peer(&addr0)
        .await
        .map_err(|e| anyhow!(e))?;
    p2p0.ops()
        .add_collections(vec!["Users".into()])
        .await
        .map_err(|e| anyhow!(e))?;
    p2p1.ops()
        .add_collections(vec!["Users".into()])
        .await
        .map_err(|e| anyhow!(e))?;

    let encrypt_arg = if encrypt { ", encrypt: true" } else { "" };
    let mutation = format!(
        r#"mutation {{ add_Users(input: {{name: "Fred", age: 33}}{encrypt_arg}) {{ _docID }} }}"#
    );
    let response = node0.execute(&mutation).await;
    if response.has_errors() {
        bail!("create on node 0 failed: {:?}", response.errors);
    }

    wait_for_fred(node1, jack).await
}

fn new_identity() -> Result<Did> {
    let raw = identity::RawIdentity::from_ed25519(crypto::generate_ed25519()?)?;
    let did = raw.did()?;
    defra_core::signing::store_identity(
        did.as_ref(),
        defra_core::signing::SigningConfig {
            key_type: defra_core::signing::SigningKeyType::Ed25519,
            private_key_bytes: defra_core::signing::SigningConfig::private_key_bytes_from_vec(
                raw.private_key_bytes().to_vec(),
            ),
            public_key_bytes: raw.public_key_bytes().to_vec(),
            public_key_hex: hex::encode(raw.public_key_bytes()),
            remote_signer: None,
            signing_authorization: None,
        },
    );
    Ok(did)
}

async fn build_node(signing: SigningConfig) -> Result<Node> {
    let config = EmbeddedNodeConfig {
        transport: TransportConfig::Libp2p(Libp2pConfig {
            listen_addr: "/ip4/127.0.0.1/tcp/0".to_string(),
        }),
        signing,
        ..Default::default()
    };
    embedded::build_with_store(Arc::new(storage::RegolithStore::in_memory()?), config).await
}

async fn add_policy(node: &Node) -> Result<String> {
    let store = node
        .local_zanzibar_store
        .as_ref()
        .context("local zanzibar store missing")?;
    let parsed = acp::policy_yaml::parse_policy_yaml(POLICY).map_err(|e| anyhow!(e))?;
    let counter = store.next_policy_counter().await?;
    let policy = acp::policy_yaml::build_policy(&parsed, counter).map_err(|e| anyhow!(e))?;
    let options = StorePolicyOptions::new()
        .with_validation()
        .with_dpi_enforcement();
    store.store_policy_with_options(&policy, &options).await?;
    Ok(policy.id)
}

async fn add_schema(node: &Node, sdl: &str, creator: &Did) -> Result<()> {
    let collections = query::parse_sdl(sdl).map_err(|e| anyhow!("SDL parse error: {e}"))?;
    schema::definition_validation::validate_new_collections(&collections)
        .map_err(|e| anyhow!("schema validation error: {e}"))?;
    with_scoped_identity(Some(creator.to_string()), async {
        node.database
            .create_collections_atomic_with_acp_registration(
                collections,
                node.document_acp.clone(),
                Some(creator.clone()),
            )
            .await
    })
    .await?;
    Ok(())
}

async fn grant_collection_reader(
    node: &Node,
    requestor: &Did,
    target: &Did,
    policy_id: &str,
    collection_id: &str,
) -> Result<()> {
    with_scoped_identity(Some(requestor.to_string()), async {
        node.document_acp
            .add_actor_relationship(
                requestor,
                target,
                policy_id,
                "users",
                collection_id,
                "reader",
                &[],
            )
            .await
    })
    .await?;
    Ok(())
}

async fn wait_for_listen_addr(system: &embedded::ManagedP2PSystem) -> Result<String> {
    let peer_id = system.ops().local_peer_id().await.map_err(|e| anyhow!(e))?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addrs = system
            .ops()
            .listen_addresses()
            .await
            .map_err(|e| anyhow!(e))?;
        if let Some(addr) = addrs
            .into_iter()
            .find(|addr| addr.starts_with("/ip4/127.0.0.1/tcp/"))
        {
            return Ok(format!("{addr}/p2p/{peer_id}"));
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for a libp2p listen address");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_fred(node: &Node, reader: &Did) -> Result<()> {
    let expected = serde_json::json!([{ "name": "Fred" }]);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = with_scoped_identity(Some(reader.to_string()), async {
            node.query_runner
                .execute(
                    QueryRequest::new("query { Users { name } }")
                        .with_identity(Some(reader.clone())),
                )
                .await
        })
        .await;
        let users = response.data.as_ref().and_then(|data| data.get("Users"));
        if users == Some(&expected) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "node 1 never returned Fred as jack; last response: data={:?} errors={:?}",
                response.data,
                response.errors
            );
        }
        sleep(Duration::from_millis(250)).await;
    }
}
