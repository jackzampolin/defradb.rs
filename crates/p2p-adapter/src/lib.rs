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

/// Resolve replicator-delete collection arguments from names to CIDs.
///
/// The push registry is keyed by collection CID (see `add_replicator`, which stores
/// CIDs), so a name passed to delete must be resolved to its CID to match. Lenient:
/// an unresolved string is kept as-is, so a CID (or already-resolved id) passed
/// directly still works. An empty input is returned untouched (full-delete path).
pub fn resolve_remove_collections<F>(collections: Vec<String>, resolve: F) -> Vec<String>
where
    F: Fn(&str) -> Option<String>,
{
    if collections.is_empty() {
        return collections;
    }
    collections
        .into_iter()
        .map(|c| resolve(&c).unwrap_or(c))
        .collect()
}

/// Overlay persisted peer metadata onto live replicator authorization state.
///
/// The live registry/transport is authoritative for which peers and collections
/// are currently replicators. Peerstore rows are persisted metadata and may lag
/// live authorization changes, so they must not introduce peers or collections.
pub fn merge_live_replicators_with_persisted_metadata(
    live: Vec<p2p::ReplicatorInfo>,
    persisted: Option<Vec<p2p::ReplicatorInfo>>,
) -> Vec<p2p::ReplicatorInfo> {
    let Some(persisted) = persisted else {
        return live;
    };

    let persisted_by_peer: std::collections::BTreeMap<String, p2p::ReplicatorInfo> = persisted
        .into_iter()
        .map(|info| (info.peer_id_str().to_string(), info))
        .collect();

    live.into_iter()
        .map(|mut info| {
            let Some(persisted_info) = persisted_by_peer.get(info.peer_id_str()) else {
                return info;
            };

            for address in &persisted_info.addresses {
                if !info.addresses.iter().any(|existing| existing == address) {
                    info.addresses.push(address.clone());
                }
            }
            info.status = persisted_info.status;
            info.last_status_change = persisted_info.last_status_change;
            info
        })
        .collect()
}

#[cfg(test)]
mod resolve_remove_collections_tests {
    use super::{merge_live_replicators_with_persisted_metadata, resolve_remove_collections};
    use std::collections::HashMap;

    fn resolver(map: HashMap<&'static str, &'static str>) -> impl Fn(&str) -> Option<String> {
        move |name| map.get(name).map(|cid| cid.to_string())
    }

    #[test]
    fn resolves_name_to_cid() {
        let map = HashMap::from([("AgentDoc", "bafyCID")]);
        let out = resolve_remove_collections(vec!["AgentDoc".to_string()], resolver(map));
        assert_eq!(out, vec!["bafyCID".to_string()]);
    }

    #[test]
    fn keeps_unresolved_string_lenient() {
        let map = HashMap::new();
        let out = resolve_remove_collections(vec!["bafyAlreadyCID".to_string()], resolver(map));
        assert_eq!(out, vec!["bafyAlreadyCID".to_string()]);
    }

    #[test]
    fn empty_is_untouched_full_delete() {
        let map = HashMap::from([("AgentDoc", "bafyCID")]);
        let out = resolve_remove_collections(Vec::new(), resolver(map));
        assert!(out.is_empty());
    }

    #[test]
    fn live_replicator_state_is_authoritative_over_persisted_collections() {
        let peer = "peer-1".to_string();
        let live = vec![p2p::ReplicatorInfo::from_raw(
            peer.clone(),
            vec!["allowed".to_string()],
            vec!["live-addr".to_string()],
        )];

        let mut persisted = p2p::ReplicatorInfo::from_raw(
            peer.clone(),
            vec!["allowed".to_string(), "revoked".to_string()],
            vec!["persisted-addr".to_string()],
        );
        persisted.set_status_if_changed_now(p2p::ReplicatorStatus::Inactive);

        let persisted_only = p2p::ReplicatorInfo::from_raw(
            "stale-peer".to_string(),
            vec!["stale".to_string()],
            vec!["stale-addr".to_string()],
        );

        let merged = merge_live_replicators_with_persisted_metadata(
            live,
            Some(vec![persisted.clone(), persisted_only]),
        );

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].peer_id_str(), peer);
        assert_eq!(merged[0].collections, vec!["allowed".to_string()]);
        assert_eq!(
            merged[0].addresses,
            vec!["live-addr".to_string(), "persisted-addr".to_string()]
        );
        assert_eq!(merged[0].status, p2p::ReplicatorStatus::Inactive);
        assert_eq!(merged[0].last_status_change, persisted.last_status_change);
    }
}

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
