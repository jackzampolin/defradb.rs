//! Shared P2P adapters implementing the HTTP P2P operation surface.

use std::sync::{Arc, RwLock};

use zeroize::Zeroizing;

#[cfg(feature = "iroh")]
mod iroh;
#[cfg(feature = "libp2p")]
mod libp2p;
#[cfg(feature = "libp2p")]
mod libp2p_doc_pusher;
pub mod manage;
mod replicator_status;
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
pub use replicator_status::{load_persisted_replicators, set_persisted_replicator_status};
#[cfg(feature = "iroh")]
pub use transport_doc_pusher::{DbTransportDocPusher, TransportDocPusher};
#[cfg(feature = "iroh")]
pub use transport_version_syncer::{DbTransportVersionSyncer, TransportVersionSyncer};
#[cfg(feature = "libp2p")]
pub use version_syncer::DbVersionSyncer;

pub use defra_http::router::{
    ExplicitReplayCapabilityInput, P2PError, P2POperations, P2PResult, P2pDocumentInfo,
    P2pDocumentRequest, ReplicationFilter, ReplicationFilters, ReplicatorInfo,
};

/// Convert a p2p `ReplicatorInfo` into the HTTP-facing `ReplicatorInfo`.
pub(crate) fn to_http_replicator_info(info: p2p::ReplicatorInfo) -> ReplicatorInfo {
    let address = info.addresses_str().first().map(|addr| addr.to_string());
    let status = Some(info.status.into());
    let last_status_change = Some(info.last_status_change_go_string());
    ReplicatorInfo {
        id: Some(info.peer_id_str().to_string()),
        collections: info.collections,
        address,
        status,
        last_status_change,
        filters: info
            .filters
            .into_iter()
            .filter_map(|(collection, filter)| Some((collection, p2p_filter_to_http(&filter)?)))
            .collect(),
    }
}

/// Convert a `p2p::ReplicationFilter` to the HTTP wire representation.
///
/// Simple single-field `_eq` predicates use the legacy `Field`/`Value` shape so
/// older clients can still read them. All other predicates (multi-field, `_in`,
/// `_gt`, etc.) are encoded via the `Conditions` field.
pub(crate) fn p2p_filter_to_http(
    filter: &p2p::ReplicationFilter,
) -> Option<defra_http::router::ReplicationFilter> {
    match filter {
        p2p::ReplicationFilter::Predicate(conds) => {
            if conds.len() == 1 {
                let (field, condition) = conds.iter().next()?;
                if let Some(value) = condition.as_object().and_then(|op| op.get("_eq")).cloned() {
                    return Some(defra_http::router::ReplicationFilter::eq(
                        field.clone(),
                        value,
                    ));
                }
            }
            Some(defra_http::router::ReplicationFilter::predicate(
                conds.clone(),
            ))
        }
        p2p::ReplicationFilter::Acp { .. } | p2p::ReplicationFilter::All(_) => {
            tracing::warn!(
                "replication filter variant not representable over HTTP yet; omitted from replicator listing"
            );
            None
        }
    }
}

/// Optional inputs used when pushing existing documents to replicators.
#[derive(Debug, Clone, Default)]
pub struct ReplicatorPushOptions {
    pub se_encryption_key: Option<Zeroizing<Vec<u8>>>,
    pub se_identity_pubkey: Option<Vec<u8>>,
}

impl PartialEq for ReplicatorPushOptions {
    fn eq(&self, other: &Self) -> bool {
        self.se_encryption_key.as_ref().map(|key| key.as_slice())
            == other.se_encryption_key.as_ref().map(|key| key.as_slice())
            && self.se_identity_pubkey == other.se_identity_pubkey
    }
}

impl Eq for ReplicatorPushOptions {}

#[derive(Debug, Clone, Default)]
pub struct ReplicatorPushOptionsState {
    inner: Arc<RwLock<ReplicatorPushOptions>>,
}

impl ReplicatorPushOptionsState {
    pub fn new(options: ReplicatorPushOptions) -> Self {
        Self {
            inner: Arc::new(RwLock::new(options)),
        }
    }

    pub fn load(&self) -> ReplicatorPushOptions {
        self.inner
            .read()
            .map(|options| options.clone())
            .unwrap_or_default()
    }

    pub fn store(&self, options: ReplicatorPushOptions) -> Result<(), String> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| "replicator push options lock poisoned".to_string())?;
        *guard = options;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::{ReplicatorPushOptions, ReplicatorPushOptionsState};
    use zeroize::Zeroizing;

    #[test]
    fn replicator_push_options_state_stores_latest_snapshot() {
        let state = ReplicatorPushOptionsState::default();

        state
            .store(ReplicatorPushOptions {
                se_encryption_key: Some(Zeroizing::new(vec![7; 32])),
                se_identity_pubkey: Some(b"did:key:zTest".to_vec()),
            })
            .unwrap();

        assert_eq!(
            state.load(),
            ReplicatorPushOptions {
                se_encryption_key: Some(Zeroizing::new(vec![7; 32])),
                se_identity_pubkey: Some(b"did:key:zTest".to_vec()),
            }
        );
    }
}
