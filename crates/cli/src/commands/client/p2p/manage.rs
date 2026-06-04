//! P2P manage command implementations.
//!
//! Drives a P2P-only peer (target) via a reachable controller node's HTTP
//! relay. The caller mints an actor JWT (`aud` = target peer-id) from its own
//! identity; the controller node relays it to the target, which authorizes the
//! operation via NAC.

use clap::{Args, Subcommand};
use defra_http::router::{RemoteManageDocRef, RemoteManageOp, RemoteManageQueryOp};

use crate::commands::client::http_client::HttpClient;
use crate::commands::client::http_client::{mint_manage_token, ManageQueryResultResponse};
use crate::commands::client::{generate_auth_token, validate_identifier, ClientContext};
use crate::error::{Error, Result};

/// Manage a remote P2P-only peer via this node's HTTP relay
#[derive(Args, Debug)]
pub struct P2pManageArgs {
    #[command(subcommand)]
    pub command: P2pManageCommand,
}

/// P2P manage subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum P2pManageCommand {
    /// Manage the target peer's P2P collections
    Collection(P2pManageCollectionArgs),
    /// Manage the target peer's replicators
    Replicator(P2pManageReplicatorArgs),
    /// Manage the target peer's P2P documents
    Document(P2pManageDocumentArgs),
    /// Manage the target peer's peer connections
    Peer(P2pManagePeerArgs),
}

impl P2pManageArgs {
    /// Execute the manage subcommand
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pManageCommand::Collection(args) => args.execute(ctx).await,
            P2pManageCommand::Replicator(args) => args.execute(ctx).await,
            P2pManageCommand::Document(args) => args.execute(ctx).await,
            P2pManageCommand::Peer(args) => args.execute(ctx).await,
        }
    }
}

/// Common arguments identifying the target peer and the caller's identity.
#[derive(Args, Debug)]
pub struct ManageTarget {
    /// Multiaddr of the target P2P-only peer (must contain `/p2p/<peer-id>`)
    #[arg(long, value_name = "multiaddr")]
    pub target: String,

    /// Caller's identity private key (hex). This single identity drives BOTH
    /// the actor token the target authorizes (`aud` = target peer-id) AND the
    /// bearer that authenticates the request to the controller node
    /// (`aud` = controller url). Falls back to the global --identity /
    /// --identity-name if omitted.
    #[arg(long, value_name = "hex")]
    pub identity: Option<String>,
}

/// Resolved authentication material for a manage operation, all derived from a
/// single caller identity.
struct ManageAuth {
    /// Actor JWT the target peer authorizes (`aud` = target peer-id).
    actor_token: String,
    /// Bearer that authenticates the relay request to the controller node
    /// (`aud` = controller url). `None` only when no identity is available.
    bearer: Option<String>,
}

impl ManageTarget {
    /// Resolve the caller's identity once and derive every token needed for a
    /// manage operation.
    ///
    /// The caller is a single identity: it authenticates to the controller node
    /// (bearer, `aud` = controller url) AND is the actor the target authorizes
    /// (actor token, `aud` = target peer-id). Both come from the same resolved
    /// key.
    fn resolve(&self, ctx: &ClientContext) -> Result<ManageAuth> {
        let key_hex = if let Some(ref identity_hex) = self.identity {
            // Validate it is hex up-front for a clear error message.
            hex::decode(identity_hex)
                .map_err(|e| Error::InvalidIdentity(format!("invalid hex: {e}")))?;
            identity_hex.clone()
        } else if let Some(ref bytes) = ctx.identity_key_bytes {
            hex::encode(bytes)
        } else {
            return Err(Error::MissingInput(
                "an identity is required: pass --identity <hex> or the global --identity/--identity-name".to_string(),
            ));
        };

        let target_peer_id = extract_peer_id(&self.target)?;
        let actor_token = mint_manage_token(&key_hex, &target_peer_id)?;

        let bearer = if self.identity.is_some() {
            // The local override also authenticates the request to the
            // controller node, mirroring the global bearer (aud = url).
            Some(generate_auth_token(&key_hex, &ctx.url)?)
        } else {
            // The global identity already minted the controller bearer.
            ctx.auth_token.clone()
        };

        Ok(ManageAuth {
            actor_token,
            bearer,
        })
    }
}

/// Extract the peer-id substring from a target multiaddr.
fn extract_peer_id(addr: &str) -> Result<String> {
    if let Ok(parsed) = p2p::parse_multiaddr_with_peer_id(addr) {
        return Ok(parsed.peer_id.to_string());
    }

    #[cfg(feature = "iroh")]
    {
        p2p::iroh::parse_public_peer_addr(addr)
            .map(|(peer_id, _)| peer_id.to_string())
            .map_err(|e| Error::Server(e.to_string()))
    }

    #[cfg(not(feature = "iroh"))]
    {
        Err(Error::Server(
            p2p::parse_multiaddr_with_peer_id(addr)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "invalid target multiaddr".to_string()),
        ))
    }
}

fn client(ctx: &ClientContext, bearer: Option<String>) -> Result<HttpClient> {
    Ok(HttpClient::new(&ctx.url)?
        .with_auth_token(bearer)
        .with_verbose(ctx.verbose))
}

fn print_query_result(result: &ManageQueryResultResponse) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(result)?);
    Ok(())
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/// Manage the target peer's P2P collections
#[derive(Args, Debug)]
pub struct P2pManageCollectionArgs {
    #[command(subcommand)]
    pub command: P2pManageCollectionCommand,
}

#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum P2pManageCollectionCommand {
    /// Add collection(s) to the target peer's P2P sync
    Add(ManageCollectionMutateArgs),
    /// Remove collection(s) from the target peer's P2P sync
    Remove(ManageCollectionMutateArgs),
    /// List the target peer's P2P collections
    List(ManageTarget),
}

#[derive(Args, Debug)]
pub struct ManageCollectionMutateArgs {
    #[command(flatten)]
    pub target: ManageTarget,

    /// Collection ID(s)
    #[arg(value_name = "COLLECTION_IDS", required = true)]
    pub collection_ids: Vec<String>,
}

impl P2pManageCollectionArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pManageCollectionCommand::Add(args) => {
                args.execute(ctx, /* add = */ true).await
            }
            P2pManageCollectionCommand::Remove(args) => {
                args.execute(ctx, /* add = */ false).await
            }
            P2pManageCollectionCommand::List(target) => {
                let auth = target.resolve(ctx)?;
                let client = client(ctx, auth.bearer)?;
                let result = client
                    .p2p_manage_query(
                        &target.target,
                        &auth.actor_token,
                        RemoteManageQueryOp::CollectionList,
                    )
                    .await?;
                print_query_result(&result)
            }
        }
    }
}

impl ManageCollectionMutateArgs {
    async fn execute(&self, ctx: &ClientContext, add: bool) -> Result<()> {
        for col in &self.collection_ids {
            validate_identifier(col)?;
        }
        let auth = self.target.resolve(ctx)?;
        let op = if add {
            RemoteManageOp::CollectionAdd {
                collection_ids: self.collection_ids.clone(),
            }
        } else {
            RemoteManageOp::CollectionRemove {
                collection_ids: self.collection_ids.clone(),
            }
        };
        let client = client(ctx, auth.bearer)?;
        client
            .p2p_manage(&self.target.target, &auth.actor_token, op)
            .await?;
        println!(
            "{} collection(s) on target peer: {}",
            if add { "Added" } else { "Removed" },
            self.collection_ids.join(", ")
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Replicator
// ---------------------------------------------------------------------------

/// Manage the target peer's replicators
#[derive(Args, Debug)]
pub struct P2pManageReplicatorArgs {
    #[command(subcommand)]
    pub command: P2pManageReplicatorCommand,
}

#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum P2pManageReplicatorCommand {
    /// Add a replicator to the target peer
    Add(ManageReplicatorMutateArgs),
    /// Delete a replicator from the target peer
    Delete(ManageReplicatorMutateArgs),
    /// List the target peer's replicators
    List(ManageTarget),
}

#[derive(Args, Debug)]
pub struct ManageReplicatorMutateArgs {
    #[command(flatten)]
    pub target: ManageTarget,

    /// Replicator peer address (multiaddr). At most one is permitted.
    #[arg(long, value_name = "multiaddr")]
    pub address: String,

    /// Collection ID(s) to replicate
    #[arg(value_name = "COLLECTION_IDS", required = true)]
    pub collection_ids: Vec<String>,
}

impl P2pManageReplicatorArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pManageReplicatorCommand::Add(args) => args.execute(ctx, /* add = */ true).await,
            P2pManageReplicatorCommand::Delete(args) => {
                args.execute(ctx, /* add = */ false).await
            }
            P2pManageReplicatorCommand::List(target) => {
                let auth = target.resolve(ctx)?;
                let client = client(ctx, auth.bearer)?;
                let result = client
                    .p2p_manage_query(
                        &target.target,
                        &auth.actor_token,
                        RemoteManageQueryOp::ReplicatorList,
                    )
                    .await?;
                print_query_result(&result)
            }
        }
    }
}

impl ManageReplicatorMutateArgs {
    async fn execute(&self, ctx: &ClientContext, add: bool) -> Result<()> {
        for col in &self.collection_ids {
            validate_identifier(col)?;
        }
        let auth = self.target.resolve(ctx)?;
        // The server rejects more than one address; pass exactly one.
        let addresses = vec![self.address.clone()];
        let op = if add {
            RemoteManageOp::ReplicatorAdd {
                addresses,
                collection_ids: self.collection_ids.clone(),
            }
        } else {
            RemoteManageOp::ReplicatorDelete {
                addresses,
                collection_ids: self.collection_ids.clone(),
            }
        };
        let client = client(ctx, auth.bearer)?;
        client
            .p2p_manage(&self.target.target, &auth.actor_token, op)
            .await?;
        println!(
            "{} replicator on target peer for collection(s): {}",
            if add { "Added" } else { "Deleted" },
            self.collection_ids.join(", ")
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Document
// ---------------------------------------------------------------------------

/// Manage the target peer's P2P documents
#[derive(Args, Debug)]
pub struct P2pManageDocumentArgs {
    #[command(subcommand)]
    pub command: P2pManageDocumentCommand,
}

#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum P2pManageDocumentCommand {
    /// Add document(s) to the target peer's P2P sync
    Add(ManageDocumentMutateArgs),
    /// Remove document(s) from the target peer's P2P sync
    Remove(ManageDocumentMutateArgs),
}

#[derive(Args, Debug)]
pub struct ManageDocumentMutateArgs {
    #[command(flatten)]
    pub target: ManageTarget,

    /// Collection the documents belong to
    #[arg(long, value_name = "collection")]
    pub collection: String,

    /// Document ID(s)
    #[arg(value_name = "DOC_IDS", required = true)]
    pub doc_ids: Vec<String>,
}

impl P2pManageDocumentArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pManageDocumentCommand::Add(args) => args.execute(ctx, /* add = */ true).await,
            P2pManageDocumentCommand::Remove(args) => args.execute(ctx, /* add = */ false).await,
        }
    }
}

impl ManageDocumentMutateArgs {
    async fn execute(&self, ctx: &ClientContext, add: bool) -> Result<()> {
        validate_identifier(&self.collection)?;
        let auth = self.target.resolve(ctx)?;
        let docs: Vec<RemoteManageDocRef> = self
            .doc_ids
            .iter()
            .map(|doc_id| RemoteManageDocRef {
                collection: self.collection.clone(),
                doc_id: doc_id.clone(),
            })
            .collect();
        let op = if add {
            RemoteManageOp::DocumentAdd { docs }
        } else {
            RemoteManageOp::DocumentRemove { docs }
        };
        let client = client(ctx, auth.bearer)?;
        client
            .p2p_manage(&self.target.target, &auth.actor_token, op)
            .await?;
        println!(
            "{} document(s) on target peer in collection {}: {}",
            if add { "Added" } else { "Removed" },
            self.collection,
            self.doc_ids.join(", ")
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Peer
// ---------------------------------------------------------------------------

/// Manage the target peer's peer connections
#[derive(Args, Debug)]
pub struct P2pManagePeerArgs {
    #[command(subcommand)]
    pub command: P2pManagePeerCommand,
}

#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum P2pManagePeerCommand {
    /// Instruct the target peer to connect to another peer
    Connect(ManagePeerConnectArgs),
}

#[derive(Args, Debug)]
pub struct ManagePeerConnectArgs {
    #[command(flatten)]
    pub target: ManageTarget,

    /// Address of the peer the target should connect to
    #[arg(long, value_name = "multiaddr")]
    pub address: String,
}

impl P2pManagePeerArgs {
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            P2pManagePeerCommand::Connect(args) => args.execute(ctx).await,
        }
    }
}

impl ManagePeerConnectArgs {
    async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let auth = self.target.resolve(ctx)?;
        let op = RemoteManageOp::PeerConnect {
            address: self.address.clone(),
        };
        let client = client(ctx, auth.bearer)?;
        client
            .p2p_manage(&self.target.target, &auth.actor_token, op)
            .await?;
        println!("Instructed target peer to connect to {}", self.address);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_peer_id, ManageTarget};
    use crate::commands::client::ClientContext;

    #[test]
    fn extract_peer_id_from_libp2p_multiaddr() {
        let peer_id = libp2p::PeerId::random().to_string();
        let addr = format!("/ip4/127.0.0.1/tcp/9000/p2p/{peer_id}");
        assert_eq!(extract_peer_id(&addr).unwrap(), peer_id);
    }

    #[test]
    fn extract_peer_id_rejects_address_without_peer_id() {
        assert!(extract_peer_id("/ip4/127.0.0.1/tcp/9000").is_err());
    }

    fn ctx_without_global_identity() -> ClientContext {
        ClientContext {
            url: "http://localhost:9181".to_string(),
            auth_token: None,
            identity_key_bytes: None,
            tx_id: None,
            development: false,
            verbose: false,
        }
    }

    fn random_target() -> String {
        let peer_id = libp2p::PeerId::random().to_string();
        format!("/ip4/127.0.0.1/tcp/9000/p2p/{peer_id}")
    }

    /// A local `--identity` must authenticate the controller request too: with
    /// no global identity it still produces a controller bearer.
    #[test]
    fn local_identity_produces_controller_bearer() {
        use crypto::keys::Key;
        let key_hex = crypto::generate_ed25519().unwrap().to_hex_string();
        let target = ManageTarget {
            target: random_target(),
            identity: Some(key_hex),
        };

        let auth = target.resolve(&ctx_without_global_identity()).unwrap();

        let bearer = auth.bearer.expect("local identity must mint a bearer");
        assert!(!bearer.is_empty());
        assert!(!auth.actor_token.is_empty());
    }

    /// Without any identity (local or global) resolution fails with a clear error.
    #[test]
    fn no_identity_is_rejected() {
        let target = ManageTarget {
            target: random_target(),
            identity: None,
        };
        assert!(target.resolve(&ctx_without_global_identity()).is_err());
    }
}
