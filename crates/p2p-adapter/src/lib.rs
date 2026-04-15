//! Shared P2P adapters implementing the HTTP P2P operation surface.

#[cfg(feature = "iroh")]
mod iroh;
#[cfg(feature = "libp2p")]
mod libp2p;
#[cfg(feature = "libp2p")]
mod libp2p_doc_pusher;
#[cfg(feature = "iroh")]
mod transport_doc_pusher;
#[cfg(feature = "iroh")]
mod transport_version_syncer;
#[cfg(feature = "libp2p")]
mod version_syncer;

#[cfg(feature = "iroh")]
pub use iroh::IrohP2PAdapter;
#[cfg(feature = "libp2p")]
pub use libp2p::{CollectionLookup, P2PAdapter, VersionSyncer};
#[cfg(feature = "libp2p")]
pub use libp2p_doc_pusher::{DbDocPusher, DocPusher};
#[cfg(feature = "iroh")]
pub use transport_doc_pusher::{DbTransportDocPusher, TransportDocPusher};
#[cfg(feature = "iroh")]
pub use transport_version_syncer::{DbTransportVersionSyncer, TransportVersionSyncer};
#[cfg(feature = "libp2p")]
pub use version_syncer::DbVersionSyncer;

pub use defra_http::router::{
    ExplicitReplayCapabilityInput, P2PError, P2POperations, P2PResult, P2pDocumentInfo,
    P2pDocumentRequest, ReplicatorInfo,
};

/// Optional inputs used when pushing existing documents to replicators.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicatorPushOptions {
    pub se_encryption_key: Option<Vec<u8>>,
    pub se_identity_pubkey: Option<Vec<u8>>,
}

pub(crate) trait P2PErrorExt {
    fn invalid_input(message: impl Into<String>) -> Self;
    fn not_found(message: impl Into<String>) -> Self;
    fn unsupported(message: impl Into<String>) -> Self;
    fn transport(message: impl Into<String>) -> Self;
    fn persistence(message: impl Into<String>) -> Self;
    fn internal(message: impl Into<String>) -> Self;
}

impl P2PErrorExt for P2PError {
    fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput(message.into())
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported(message.into())
    }

    fn transport(message: impl Into<String>) -> Self {
        Self::Transport(message.into())
    }

    fn persistence(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}
