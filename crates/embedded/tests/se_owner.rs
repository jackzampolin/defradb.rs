//! Embedded SE-owner integration test (#976).
//!
//! Validates that an embedded node can act as a searchable-encryption (SE)
//! query OWNER: it provisions the SE key at RUNTIME (via
//! `set_replicator_push_options`, mirroring FFI `set_se_encryption_key`),
//! writes an encrypted-indexed document, pushes the SE artifact to its
//! replicator, then runs `encrypted_<Collection>(filter: {...}) { docIDs }`.
//! The owner generates the search tag and fans it out to the replicator over
//! the SE query protocol; the replicator byte-matches the pushed artifact and
//! returns the docID. The owner never resolves locally.
//!
//! Topology (Go's owner-queries-replicator model): node A = OWNER (writes the
//! doc, sets the replicator, runs the query); node B = REPLICATOR (serves
//! docIDs from the artifacts A pushed). Both hold the same 32-byte SE key.

use anyhow::{bail, Context, Result};
use embedded::{EmbeddedNode, EmbeddedStore, NodeBuilder, ReplicatorPushOptions};
use tokio::time::{sleep, Duration, Instant};
use zeroize::Zeroizing;

const USER_SCHEMA: &str = "type User { name: String  age: Int  city: String }";

/// Fixed 32-byte SE key with a distinct value per byte, so a wrong/empty key
/// would not coincidentally match.
const SHARED_SE_KEY: [u8; 32] = [
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
    0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_libp2p_se_owner_queries_replicator() -> Result<()> {
    let owner = NodeBuilder::default()
        .with_libp2p("/ip4/127.0.0.1/tcp/0")
        .build()
        .await?;
    let replicator = NodeBuilder::default()
        .with_libp2p("/ip4/127.0.0.1/tcp/0")
        .build()
        .await?;

    // Same schema + encrypted index on `name` on both nodes.
    owner.add_schema(USER_SCHEMA).await?;
    replicator.add_schema(USER_SCHEMA).await?;
    owner.add_encrypted_index("User", "name").await?;
    replicator.add_encrypted_index("User", "name").await?;

    // Provision the SAME SE key at runtime on BOTH nodes (mirrors FFI
    // set_se_encryption_key -> set_replicator_push_options).
    set_se_key(&owner, SHARED_SE_KEY)?;
    set_se_key(&replicator, SHARED_SE_KEY)?;

    let p2p_owner = owner.p2p().cloned().context("owner missing p2p system")?;
    let p2p_replicator = replicator
        .p2p()
        .cloned()
        .context("replicator missing p2p system")?;

    let replicator_peer = p2p_replicator
        .ops()
        .local_peer_id()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let replicator_addr = wait_for_shareable_addr(&p2p_replicator, &replicator_peer).await?;

    // Owner connects to replicator and registers it as the User replicator.
    p2p_owner
        .ops()
        .connect_peer(&replicator_addr)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    wait_for_connected_peer(&p2p_owner, &replicator_peer).await?;

    p2p_owner
        .ops()
        .add_collections(vec!["User".to_string()])
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    p2p_replicator
        .ops()
        .add_collections(vec!["User".to_string()])
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    p2p_owner
        .ops()
        .add_replicator(
            vec!["User".to_string()],
            Some(&replicator_addr),
            Default::default(),
            Vec::new(),
            None,
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Owner writes the encrypted-indexed doc; this pushes the SE artifact to B.
    let created = owner
        .execute(r#"mutation { add_User(input: {name: "John", age: 21, city: "NYC"}) { _docID } }"#)
        .await;
    if created.has_errors() {
        bail!("add_User returned errors: {:?}", created.errors);
    }
    let doc_id = created
        .data
        .as_ref()
        .and_then(|d| d.get("add_User"))
        .and_then(|v| v.as_array())
        .and_then(|items| items.first())
        .and_then(|item| item.get("_docID"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .context("add_User response missing _docID")?;

    // Query runs on the OWNER (node A); A fans the tag out to replicator B.
    wait_for_se_query(&owner, "User", r#"{name: {_eq: "John"}}"#, &doc_id).await?;

    p2p_owner.shutdown().await;
    p2p_replicator.shutdown().await;
    owner.database.close().await?;
    replicator.database.close().await?;
    Ok(())
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_iroh_se_owner_queries_replicator() -> Result<()> {
    use std::net::{IpAddr, Ipv4Addr};

    use embedded::IrohConfig;

    fn iroh_config() -> IrohConfig {
        IrohConfig {
            bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            bind_port: Some(0),
            relay_mode: p2p::iroh::IrohRelayModeConfig::Disabled,
            discovery: p2p::iroh::IrohDiscoveryConfig::Disabled,
            secret_key_path: None,
        }
    }

    let owner = NodeBuilder::default()
        .with_iroh(iroh_config())
        .build()
        .await?;
    let replicator = NodeBuilder::default()
        .with_iroh(iroh_config())
        .build()
        .await?;

    owner.add_schema(USER_SCHEMA).await?;
    replicator.add_schema(USER_SCHEMA).await?;
    owner.add_encrypted_index("User", "name").await?;
    replicator.add_encrypted_index("User", "name").await?;

    set_se_key(&owner, SHARED_SE_KEY)?;
    set_se_key(&replicator, SHARED_SE_KEY)?;

    let p2p_owner = owner.p2p().cloned().context("owner missing p2p system")?;
    let p2p_replicator = replicator
        .p2p()
        .cloned()
        .context("replicator missing p2p system")?;

    let replicator_peer = p2p_replicator
        .ops()
        .local_peer_id()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let replicator_addr = wait_for_iroh_addr(&p2p_replicator).await?;

    p2p_owner
        .ops()
        .connect_peer(&replicator_addr)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    wait_for_connected_peer(&p2p_owner, &replicator_peer).await?;

    p2p_owner
        .ops()
        .add_collections(vec!["User".to_string()])
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    p2p_replicator
        .ops()
        .add_collections(vec!["User".to_string()])
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    p2p_owner
        .ops()
        .add_replicator(
            vec!["User".to_string()],
            Some(&replicator_addr),
            Default::default(),
            Vec::new(),
            None,
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    let created = owner
        .execute(r#"mutation { add_User(input: {name: "John", age: 21, city: "NYC"}) { _docID } }"#)
        .await;
    if created.has_errors() {
        bail!("add_User returned errors: {:?}", created.errors);
    }
    let doc_id = created
        .data
        .as_ref()
        .and_then(|d| d.get("add_User"))
        .and_then(|v| v.as_array())
        .and_then(|items| items.first())
        .and_then(|item| item.get("_docID"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .context("add_User response missing _docID")?;

    wait_for_se_query(&owner, "User", r#"{name: {_eq: "John"}}"#, &doc_id).await?;

    p2p_owner.shutdown().await;
    p2p_replicator.shutdown().await;
    owner.database.close().await?;
    replicator.database.close().await?;
    Ok(())
}

#[cfg(feature = "iroh")]
async fn wait_for_iroh_addr(system: &embedded::ManagedP2PSystem) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addrs = system
            .ops()
            .listen_addresses()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        if let Some(addr) = addrs
            .into_iter()
            .find(|addr| addr.contains("/p2p/") || addr.starts_with("endpoint"))
        {
            return Ok(addr);
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for direct iroh listen address");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn set_se_key(node: &EmbeddedNode<EmbeddedStore>, key: [u8; 32]) -> Result<()> {
    let p2p = node.p2p().context("node missing p2p system")?;
    p2p.set_replicator_push_options(ReplicatorPushOptions {
        se_encryption_key: Some(Zeroizing::new(key.to_vec())),
        se_identity_pubkey: None,
    })
    .map_err(|e| anyhow::anyhow!(e))
}

async fn wait_for_shareable_addr(
    system: &embedded::ManagedP2PSystem,
    peer_id: &str,
) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(addr) = system
            .ops()
            .shareable_address()
            .await
            .map_err(|e| anyhow::anyhow!(e))?
        {
            return Ok(addr);
        }
        // libp2p listen addresses lack the /p2p/<peer_id> suffix; append it
        // (mirrors the HTTP /p2p/info handler). Skip the unspecified 0.0.0.0
        // bind addr in favour of a concrete loopback address.
        let addrs = system
            .ops()
            .listen_addresses()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        if let Some(addr) = addrs
            .into_iter()
            .filter(|a| a.starts_with('/') && !a.contains("/0.0.0.0/"))
            .max_by_key(|a| a.contains("127.0.0.1"))
        {
            return Ok(format!("{addr}/p2p/{peer_id}"));
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for a shareable libp2p address");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_connected_peer(system: &embedded::ManagedP2PSystem, peer_id: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let peers = system
            .ops()
            .connected_peers()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        if peers.iter().any(|peer| peer.contains(peer_id)) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for peer connection to {peer_id}");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_se_query(
    node: &EmbeddedNode<EmbeddedStore>,
    collection: &str,
    filter: &str,
    expected_doc_id: &str,
) -> Result<()> {
    let query = format!("query {{ encrypted_{collection}(filter: {filter}) {{ docIDs }} }}");
    let key = format!("encrypted_{collection}");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = node.execute(&query).await;
        if response.has_errors() {
            bail!("encrypted query returned errors: {:?}", response.errors);
        }
        let found = response
            .data
            .as_ref()
            .and_then(|d| d.get(&key))
            .and_then(|rows| rows.as_array())
            .map(|rows| {
                rows.iter().any(|row| {
                    row.get("docIDs")
                        .and_then(|ids| ids.as_array())
                        .map(|ids| ids.iter().any(|v| v.as_str() == Some(expected_doc_id)))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);
        if found {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("SE query did not return doc {expected_doc_id} within timeout");
        }
        sleep(Duration::from_millis(250)).await;
    }
}
