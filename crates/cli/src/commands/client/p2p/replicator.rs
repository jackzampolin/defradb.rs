//! P2P replicator command implementations

use clap::{Args, Subcommand};
use defra_http::router::ExplicitReplayCapabilityInput;

use crate::commands::client::http_client::HttpClient;
use crate::commands::client::{raw_identity_from_key_bytes, validate_identifier, ClientContext};
use crate::error::{Error, Result};

/// Replicator management subcommands
#[derive(Args, Debug)]
pub struct P2pReplicatorArgs {
    #[command(subcommand)]
    pub command: P2pReplicatorCommand,
}

/// Replicator subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum P2pReplicatorCommand {
    /// Add replicator(s) and start synchronization
    Add(P2pReplicatorAddArgs),
    /// Delete replicator(s) and stop synchronization
    Delete(P2pReplicatorDeleteArgs),
    /// List all replicators
    List(P2pReplicatorListArgs),
}

/// Arguments for replicator list command
#[derive(Args, Debug)]
pub struct P2pReplicatorListArgs {}

/// Arguments for replicator add command
#[derive(Args, Debug)]
pub struct P2pReplicatorAddArgs {
    /// Collection(s) to replicate (comma-separated or multiple --collection)
    #[arg(long, short = 'c', required = true, value_delimiter = ',')]
    pub collection: Vec<String>,

    /// Peer address(es) to replicate with
    #[arg(value_name = "addresses")]
    pub addresses: Vec<String>,
}

/// Arguments for replicator delete command
#[derive(Args, Debug)]
pub struct P2pReplicatorDeleteArgs {
    /// Collection(s) to stop replicating (comma-separated or multiple --collection)
    #[arg(long, short = 'c', required = true, value_delimiter = ',')]
    pub collection: Vec<String>,

    /// Peer ID to stop replicating with
    #[arg(value_name = "peerID")]
    pub peer_id: Option<String>,
}

impl P2pReplicatorArgs {
    /// Execute the replicator subcommand
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pReplicatorCommand::Add(args) => args.execute(ctx).await,
            P2pReplicatorCommand::Delete(args) => args.execute(ctx).await,
            P2pReplicatorCommand::List(args) => args.execute(ctx).await,
        }
    }
}

impl P2pReplicatorListArgs {
    /// Execute the replicator list command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let replicators = client.p2p_replicator_list().await?;
        println!("{}", serde_json::to_string_pretty(&replicators)?);
        Ok(())
    }
}

impl P2pReplicatorAddArgs {
    /// Execute the replicator add command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        for col in &self.collection {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let address = self.addresses.first().map(|s| s.as_str());
        let explicit_replay_capabilities =
            build_explicit_replay_capabilities(&client, ctx, &self.collection, address).await?;

        client
            .p2p_replicator_add(&self.collection, address, &explicit_replay_capabilities)
            .await?;
        println!(
            "Set replicator for collections: {}",
            self.collection.join(", ")
        );
        Ok(())
    }
}

async fn build_explicit_replay_capabilities(
    client: &HttpClient,
    ctx: &ClientContext,
    collections: &[String],
    address: Option<&str>,
) -> Result<Vec<ExplicitReplayCapabilityInput>> {
    let Some(identity_key_bytes) = ctx.identity_key_bytes.as_deref() else {
        return Ok(Vec::new());
    };
    let Some(address) = address else {
        return Ok(Vec::new());
    };

    let local_info = client.p2p_info().await?;
    let source_peer_id = local_info
        .first()
        .ok_or_else(|| Error::Server("local node has no P2P listen address".to_string()))
        .and_then(|addr| extract_public_peer_id(addr.as_str()))?;
    let target_peer_id = extract_public_peer_id(address)?;
    let identity = raw_identity_from_key_bytes("replicator identity", identity_key_bytes)?;
    let collections_by_name = client.collection_versions().await?;

    collections
        .iter()
        .map(|name| {
            let collection_id = collections_by_name
                .iter()
                .find(|collection| collection.name == *name && collection.is_active)
                .or_else(|| {
                    collections_by_name
                        .iter()
                        .find(|collection| collection.name == *name)
                })
                .map(|collection| collection.collection_id.clone())
                .ok_or_else(|| Error::CollectionNotFound(name.clone()))?;
            let capability = p2p::generate_explicit_replay_capability(
                &identity,
                &source_peer_id,
                &target_peer_id,
                &collection_id,
                p2p::DEFAULT_EXPLICIT_REPLAY_CAPABILITY_TTL,
            )
            .map_err(|e| Error::Server(e.to_string()))?;
            Ok(ExplicitReplayCapabilityInput {
                collection_id,
                capability,
            })
        })
        .collect()
}

fn extract_public_peer_id(addr: &str) -> Result<String> {
    if let Ok(parsed) = p2p::parse_multiaddr_with_peer_id(addr) {
        return Ok(parsed.peer_id.to_string());
    }

    #[cfg(feature = "iroh")]
    {
        return p2p::iroh::parse_public_peer_addr(addr)
            .map(|(peer_id, _)| peer_id.to_string())
            .map_err(|e| Error::Server(e.to_string()));
    }

    #[cfg(not(feature = "iroh"))]
    {
        Err(Error::Server(
            p2p::parse_multiaddr_with_peer_id(addr)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "invalid peer address".to_string()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::extract_public_peer_id;

    #[test]
    fn extract_public_peer_id_accepts_libp2p_multiaddr() {
        let peer_id = "12D3KooWM111111111111111111111111111111111111111111111";
        let addr = format!("/ip4/127.0.0.1/tcp/9000/p2p/{peer_id}");

        assert_eq!(extract_public_peer_id(&addr).unwrap(), peer_id);
    }

    #[cfg(feature = "iroh")]
    #[test]
    fn extract_public_peer_id_accepts_iroh_public_addr() {
        let peer_id = "ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6";
        let addr = format!("127.0.0.1:9000/p2p/{peer_id}");

        assert_eq!(extract_public_peer_id(&addr).unwrap(), peer_id);
    }
}

impl P2pReplicatorDeleteArgs {
    /// Execute the replicator delete command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        for col in &self.collection {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client
            .p2p_replicator_delete(&self.collection, self.peer_id.as_deref())
            .await?;
        println!(
            "Removed replicator for collections: {}",
            self.collection.join(", ")
        );
        Ok(())
    }
}
