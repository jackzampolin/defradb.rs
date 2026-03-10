#![cfg(feature = "iroh")]

use std::net::{IpAddr, Ipv4Addr};

use anyhow::{bail, Context, Result};
use embedded::{IrohConfig, NodeBuilder};
use tokio::time::{sleep, Duration, Instant};

const BOOK_SDL: &str = "type Book { title: String }";
const REPLICATED_TITLE: &str = "Replicated over iroh";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_embedded_iroh_nodes_connect_and_replicate() -> Result<()> {
    let node_a = NodeBuilder::default()
        .with_iroh(test_iroh_config())
        .build()
        .await?;
    let node_b = NodeBuilder::default()
        .with_iroh(test_iroh_config())
        .build()
        .await?;

    node_a.add_schema(BOOK_SDL).await?;
    node_b.add_schema(BOOK_SDL).await?;

    let p2p_b = node_b.p2p().cloned().context("node_b missing p2p system")?;
    let p2p_a = node_a.p2p().cloned().context("node_a missing p2p system")?;

    let peer_a = p2p_a
        .ops()
        .local_peer_id()
        .await
        .map_err(|error| anyhow::anyhow!(error))?;
    let direct_addr_a = wait_for_direct_iroh_addr(&p2p_a).await?;
    let endpoint_addr = format!("{peer_a}@{direct_addr_a}");

    let create_response = node_a
        .execute(
            r#"mutation { add_Book(input: {title: "Replicated over iroh"}) { _docID title } }"#,
        )
        .await;
    ensure_success(&create_response, "add_Book")?;
    let doc_id = extract_created_doc_id(&create_response)?;

    p2p_b
        .ops()
        .connect_peer(&endpoint_addr)
        .await
        .map_err(|error| anyhow::anyhow!(error))?;
    wait_for_connected_peer(&p2p_b, &peer_a).await?;

    p2p_b
        .ops()
        .sync_documents("Book", vec![doc_id])
        .await
        .map_err(|error| anyhow::anyhow!(error))?;

    wait_for_book_title(&node_b, REPLICATED_TITLE).await?;

    p2p_a.shutdown().await;
    p2p_b.shutdown().await;
    node_a.database.close().await?;
    node_b.database.close().await?;

    Ok(())
}

fn test_iroh_config() -> IrohConfig {
    IrohConfig {
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        bind_port: Some(0),
        relay_url: None,
        discovery: false,
        secret_key_path: None,
    }
}

async fn wait_for_direct_iroh_addr(system: &embedded::ManagedP2PSystem) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addrs = system
            .ops()
            .listen_addresses()
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        if let Some(addr) = addrs.into_iter().find(|addr| !addr.starts_with("iroh://")) {
            return Ok(addr);
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for direct iroh listen address");
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
            .map_err(|error| anyhow::anyhow!(error))?;
        if peers.iter().any(|peer| peer.contains(peer_id)) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for peer connection to {peer_id}");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_book_title(
    node: &embedded::EmbeddedNode<embedded::EmbeddedStore>,
    title: &str,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let response = node.execute("query { Book { _docID title } }").await;
        ensure_success(&response, "Book query")?;

        if response_contains_title(&response, title) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for replicated title '{title}'");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

fn ensure_success(response: &query::QueryResponse, operation: &str) -> Result<()> {
    if response.has_errors() {
        bail!("{operation} returned errors: {:?}", response.errors);
    }
    Ok(())
}

fn response_contains_title(response: &query::QueryResponse, title: &str) -> bool {
    response
        .data
        .as_ref()
        .and_then(|data| data.get("Book"))
        .and_then(|books| books.as_array())
        .map(|books| {
            books
                .iter()
                .any(|book| book.get("title").and_then(|value| value.as_str()) == Some(title))
        })
        .unwrap_or(false)
}

fn extract_created_doc_id(response: &query::QueryResponse) -> Result<String> {
    response
        .data
        .as_ref()
        .and_then(|data| data.get("add_Book"))
        .and_then(|value| value.as_array())
        .and_then(|items| items.first())
        .and_then(|item| item.get("_docID"))
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("add_Book response missing _docID"))
}
