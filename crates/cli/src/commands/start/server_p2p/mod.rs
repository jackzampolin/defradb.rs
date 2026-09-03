//! P2P initialization helpers for Node startup.

use std::sync::Arc;

use super::node::{Node, P2PTasks};
use crate::config::{AcpDocumentType, Config, TransportType};
#[cfg(not(feature = "iroh"))]
use crate::error::Error;
use crate::error::Result;

#[cfg(feature = "iroh")]
mod iroh;
#[path = "libp2p.rs"]
mod libp2p_setup;

type WireDocumentAcp = Option<Box<dyn FnOnce(Arc<dyn acp::DocumentACP>)>>;
type WireKms = Option<Box<dyn FnOnce(Arc<dyn kms::KmsService>) + Send>>;

pub(super) struct P2PSetup {
    pub(super) host_handle: Option<p2p::P2PHostHandle>,
    pub(super) p2p_tasks: Option<P2PTasks>,
    pub(super) mutator: Arc<dyn query::mutator::DocMutator>,
    pub(super) http_adapter: Option<Arc<dyn defra_http::router::P2POperations>>,
    pub(super) wire_merge_acp: WireDocumentAcp,
    pub(super) wire_doc_pusher_acp: WireDocumentAcp,
    /// Hook for forwarding committed `/tx` writes to P2P peers. `Some` when the
    /// P2P stack is up; `None` for the non-P2P fallback path.
    pub(super) txn_broadcaster: Option<Arc<dyn db::event::emission::TxnBroadcaster>>,
    /// Type-erased KMS transport for this node's P2P system. server.rs adds it
    /// to the DefraKms transports list and installs the serve handler. `None`
    /// on the non-P2P fallback path.
    pub(super) kms_transport: Option<Arc<dyn kms::KeyTransport>>,
    /// Defers wiring the late-built KMS into the inner merge handler (mirrors
    /// `wire_merge_acp`). NAC/document_acp aren't available when the P2P system
    /// is created, so the KMS is built later in server.rs.
    pub(super) wire_kms: WireKms,
    /// This node's transport-level peer id (stringified). server.rs binds it
    /// into the KMS so served ECIES replies carry the correct AAD peer id.
    pub(super) local_peer_id: String,
    /// SE remote query transport (owner-queries-replicator, #976). `Some` on the
    /// libp2p path when an SE key is present; `None` for iroh (the SE-query
    /// send path is libp2p-only) and the non-P2P fallback.
    pub(super) se_transport: Option<Arc<dyn query::SeQueryTransport>>,
    /// Inbound management-channel serve deps, read lazily by the event loop and
    /// populated by server.rs once the controller (`P2POperations`) and NAC
    /// manager are built. `None` on the non-P2P fallback path; the event loop
    /// drops manage requests until populated.
    pub(super) manage_hooks: Option<defra_p2p_adapter::manage::hooks::ManageHooksCell>,
    /// The `P2POperations` controller (the `http_adapter`) bound into
    /// `manage_hooks` after the NAC manager exists. `None` on the fallback path.
    pub(super) manage_controller: Option<Arc<dyn defra_http::router::P2POperations>>,
    /// Requester-side manage correlators (mutating + query). Event-loop clones
    /// deliver inbound replies; these clones feed the requester API (Task 6.3)
    /// and are bound into `manage_hooks` so both agree on message_id
    /// correlation. `None` on the fallback path.
    pub(super) manage_correlator: Option<p2p::ManageCorrelator>,
    pub(super) manage_query_correlator: Option<p2p::ManageQueryCorrelator>,
    /// Outbound management requester (Task 7a): relays management requests to
    /// P2P-only peers on behalf of an HTTP caller. Built over the same concrete
    /// transport the SE requester / serve loop use, sharing the requester-side
    /// manage correlators. server.rs threads it into `AppState` via
    /// `with_manage`. `None` on the non-P2P fallback path.
    pub(super) manage_requester: Option<Arc<dyn defra_http::router::ManageRequester>>,
}

impl Node {
    pub(super) async fn setup_p2p(
        store: Arc<storage::DynStore>,
        database: Arc<db::DB<storage::DynStore>>,
        event_bus: Arc<dyn events::Bus>,
        config: &Config,
        peer_keypair: Option<p2p::Keypair>,
        node_identity: Option<Arc<identity::RawIdentity>>,
        se_key: Option<[u8; 32]>,
    ) -> Result<P2PSetup> {
        if config.net.p2p_disabled {
            return Ok(Self::p2p_disabled(database));
        }

        if config.net.transport == TransportType::Iroh {
            #[cfg(feature = "iroh")]
            {
                return Self::setup_iroh_p2p(
                    store,
                    database,
                    event_bus,
                    config,
                    peer_keypair,
                    node_identity,
                    se_key,
                )
                .await;
            }
            #[cfg(not(feature = "iroh"))]
            {
                let _ = (
                    store,
                    database,
                    event_bus,
                    peer_keypair,
                    node_identity,
                    se_key,
                );
                return Err(Error::InvalidTransport(
                    "iroh transport not enabled. Rebuild with --features iroh".into(),
                ));
            }
        }

        Self::setup_libp2p_p2p(
            store,
            database,
            event_bus,
            config,
            peer_keypair,
            node_identity,
            se_key,
        )
        .await
    }

    fn p2p_disabled(database: Arc<db::DB<storage::DynStore>>) -> P2PSetup {
        P2PSetup {
            host_handle: None,
            p2p_tasks: None,
            mutator: Arc::new(db::AutoCommitMutator::new(database)),
            http_adapter: None,
            wire_merge_acp: None,
            wire_doc_pusher_acp: None,
            txn_broadcaster: None,
            kms_transport: None,
            wire_kms: None,
            local_peer_id: String::new(),
            se_transport: None,
            manage_hooks: None,
            manage_controller: None,
            manage_correlator: None,
            manage_query_correlator: None,
            manage_requester: None,
        }
    }

    fn access_mode(config: &Config) -> p2p::bitswap::AccessMode {
        if config.acp.document_type != AcpDocumentType::None {
            p2p::bitswap::AccessMode::Controlled
        } else {
            p2p::bitswap::AccessMode::Open
        }
    }

    fn sync_config(config: &Config) -> p2p::sync::SyncConfig {
        p2p::sync::SyncConfig {
            rate_limit_burst: config.net.p2p_rate_limit_burst,
            rate_limit_rate: config.net.p2p_rate_limit_rate,
            max_doc_sync_request_doc_ids: config.net.p2p_max_doc_sync_request_doc_ids,
            max_pending_dags: config.net.p2p_max_pending_dags,
            push_queue_capacity: config.net.p2p_push_queue_capacity,
            push_queue_byte_capacity: config.net.p2p_push_queue_byte_capacity,
            max_active_pushes_per_peer: config.net.p2p_max_active_pushes_per_peer,
            ..Default::default()
        }
    }
}
