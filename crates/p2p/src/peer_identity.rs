//! Peer ACP identity resolution for serve-boundary checks.

use async_trait::async_trait;

use crate::transport::PeerId;

#[cfg(feature = "iroh-transport")]
const IROH_IDENTITY_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(5 * 60);
#[cfg(feature = "iroh-transport")]
const IROH_IDENTITY_CACHE_CAPACITY: usize = 1024;

#[async_trait]
pub trait PeerIdentityResolver: Send + Sync {
    async fn resolve(&self, peer_id: &PeerId) -> Option<identity::Did>;
}

#[cfg(feature = "libp2p-transport")]
#[derive(Clone)]
pub struct HandlePeerIdentityResolver {
    handle: crate::P2PHostHandle,
}

#[cfg(feature = "libp2p-transport")]
impl HandlePeerIdentityResolver {
    pub fn new(handle: crate::P2PHostHandle) -> Self {
        Self { handle }
    }
}

#[cfg(feature = "libp2p-transport")]
#[async_trait]
impl PeerIdentityResolver for HandlePeerIdentityResolver {
    async fn resolve(&self, peer_id: &PeerId) -> Option<identity::Did> {
        let peer_id = peer_id.as_str().parse::<libp2p::PeerId>().ok()?;
        self.handle.get_peer_identity(peer_id).await.ok().flatten()
    }
}

/// Resolves a Defra DID by challenging the authenticated Iroh QUIC endpoint.
///
/// The remote endpoint signs a token whose audience is this transport's
/// endpoint ID. This keeps the untrusted PushLog origin field out of the CAR
/// authorization decision while preserving Go's peer-identity semantics.
#[cfg(feature = "iroh-transport")]
#[derive(Clone)]
pub struct IrohPeerIdentityResolver {
    transport: crate::iroh::IrohTransport,
    state: IrohPeerIdentityState,
}

#[cfg(feature = "iroh-transport")]
struct CachedIrohPeerIdentity {
    did: identity::Did,
    verified_at: std::time::Instant,
}

#[cfg(feature = "iroh-transport")]
type IrohIdentityFlight = std::sync::Arc<tokio::sync::OnceCell<Option<identity::Did>>>;

#[cfg(feature = "iroh-transport")]
type IrohIdentityFlights =
    std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<PeerId, IrohIdentityFlight>>>;

/// Bounded positive cache matching the established libp2p peer-identity
/// behavior without retaining stale endpoint-to-DID bindings indefinitely.
#[cfg(feature = "iroh-transport")]
struct IrohPeerIdentityCache {
    entries: lru::LruCache<PeerId, CachedIrohPeerIdentity>,
    ttl: std::time::Duration,
}

#[cfg(feature = "iroh-transport")]
#[derive(Clone)]
struct IrohPeerIdentityState {
    cache: std::sync::Arc<parking_lot::Mutex<IrohPeerIdentityCache>>,
    in_flight: IrohIdentityFlights,
}

#[cfg(feature = "iroh-transport")]
impl IrohPeerIdentityState {
    fn new() -> Self {
        Self {
            cache: std::sync::Arc::new(parking_lot::Mutex::new(IrohPeerIdentityCache::new(
                IROH_IDENTITY_CACHE_TTL,
                IROH_IDENTITY_CACHE_CAPACITY,
            ))),
            in_flight: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    async fn resolve_with<F, Fut>(&self, peer_id: &PeerId, resolve: F) -> Option<identity::Did>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Option<identity::Did>>,
    {
        if let Some(did) = self.cache.lock().get(peer_id, std::time::Instant::now()) {
            tracing::trace!(%peer_id, "using cached authenticated Iroh peer identity");
            return Some(did);
        }

        let flight = {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(flight) = in_flight.get(peer_id) {
                std::sync::Arc::clone(flight)
            } else {
                if in_flight.len() >= IROH_IDENTITY_CACHE_CAPACITY {
                    tracing::debug!(%peer_id, "Iroh peer identity single-flight capacity reached");
                    return None;
                }
                let flight = std::sync::Arc::new(tokio::sync::OnceCell::new());
                in_flight.insert(peer_id.clone(), std::sync::Arc::clone(&flight));
                flight
            }
        };

        // Every concurrent request for one authenticated endpoint observes the
        // same challenge result. A negative result is shared only by current
        // waiters and is removed immediately; it is never cached for retries.
        let result = flight.get_or_init(resolve).await.clone();
        if let Some(did) = result.as_ref() {
            self.cache
                .lock()
                .insert(peer_id.clone(), did.clone(), std::time::Instant::now());
        }
        let mut in_flight = self.in_flight.lock().await;
        if in_flight
            .get(peer_id)
            .is_some_and(|current| std::sync::Arc::ptr_eq(current, &flight))
        {
            in_flight.remove(peer_id);
        }
        result
    }
}

#[cfg(feature = "iroh-transport")]
impl IrohPeerIdentityCache {
    fn new(ttl: std::time::Duration, capacity: usize) -> Self {
        Self {
            entries: lru::LruCache::new(
                std::num::NonZeroUsize::new(capacity).expect("identity cache capacity is non-zero"),
            ),
            ttl,
        }
    }

    fn get(&mut self, peer_id: &PeerId, now: std::time::Instant) -> Option<identity::Did> {
        let entry = self.entries.peek(peer_id)?;
        if now.duration_since(entry.verified_at) >= self.ttl {
            self.entries.pop(peer_id);
            return None;
        }
        Some(entry.did.clone())
    }

    fn insert(&mut self, peer_id: PeerId, did: identity::Did, now: std::time::Instant) {
        self.entries.put(
            peer_id,
            CachedIrohPeerIdentity {
                did,
                verified_at: now,
            },
        );
    }
}

#[cfg(feature = "iroh-transport")]
impl IrohPeerIdentityResolver {
    pub fn new(transport: crate::iroh::IrohTransport) -> Self {
        Self {
            transport,
            state: IrohPeerIdentityState::new(),
        }
    }
}

#[cfg(feature = "iroh-transport")]
#[async_trait]
impl PeerIdentityResolver for IrohPeerIdentityResolver {
    async fn resolve(&self, peer_id: &PeerId) -> Option<identity::Did> {
        self.state
            .resolve_with(peer_id, || async {
                match self.transport.get_peer_identity(peer_id).await {
                    Ok(identity) => identity,
                    Err(error) => {
                        tracing::debug!(%peer_id, %error, "failed to resolve Iroh peer identity");
                        None
                    }
                }
            })
            .await
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AnonymousResolver;

#[async_trait]
impl PeerIdentityResolver for AnonymousResolver {
    async fn resolve(&self, _peer_id: &PeerId) -> Option<identity::Did> {
        None
    }
}

#[cfg(all(test, feature = "iroh-transport"))]
mod tests {
    use super::*;

    fn did(label: &str) -> identity::Did {
        identity::Did::new(format!("did:key:{label}")).unwrap()
    }

    #[test]
    fn iroh_identity_cache_is_positive_bounded_and_expiring() {
        let start = std::time::Instant::now();
        let ttl = std::time::Duration::from_secs(10);
        let mut cache = IrohPeerIdentityCache::new(ttl, 2);
        let peer_a = PeerId::new("peer-a".to_string());
        let peer_b = PeerId::new("peer-b".to_string());
        let peer_c = PeerId::new("peer-c".to_string());

        cache.insert(peer_a.clone(), did("a"), start);
        cache.insert(
            peer_b.clone(),
            did("b"),
            start + std::time::Duration::from_secs(1),
        );
        assert_eq!(cache.get(&peer_a, start).unwrap(), did("a"));

        cache.insert(
            peer_c.clone(),
            did("c"),
            start + std::time::Duration::from_secs(2),
        );
        assert!(cache
            .get(&peer_a, start + std::time::Duration::from_secs(2))
            .is_none());
        assert_eq!(
            cache
                .get(&peer_b, start + std::time::Duration::from_secs(2))
                .unwrap(),
            did("b")
        );
        assert_eq!(
            cache
                .get(&peer_c, start + std::time::Duration::from_secs(2))
                .unwrap(),
            did("c")
        );
        assert!(cache
            .get(&peer_b, start + std::time::Duration::from_secs(11))
            .is_none());
    }

    #[tokio::test]
    async fn iroh_identity_resolution_is_single_flight_per_peer() {
        let state = IrohPeerIdentityState::new();
        let peer = PeerId::new("peer-a".to_string());
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(17));
        let mut tasks = tokio::task::JoinSet::new();

        for _ in 0..16 {
            let state = state.clone();
            let peer = peer.clone();
            let calls = std::sync::Arc::clone(&calls);
            let barrier = std::sync::Arc::clone(&barrier);
            tasks.spawn(async move {
                barrier.wait().await;
                state
                    .resolve_with(&peer, || {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        async {
                            tokio::task::yield_now().await;
                            Some(did("single-flight"))
                        }
                    })
                    .await
            });
        }
        barrier.wait().await;

        while let Some(result) = tasks.join_next().await {
            assert_eq!(result.unwrap(), Some(did("single-flight")));
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
