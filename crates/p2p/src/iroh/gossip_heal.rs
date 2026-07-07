//! Gossip send-path healing (#1092).
//!
//! iroh-gossip can wedge a peer in `PeerState::Active` with a dead send loop:
//! a stream-level write error kills the send half of its connection loop while
//! the QUIC connection stays alive (iroh keep-alives), so the connection task
//! never finishes and gossip never prunes the peer. Every subsequent send then
//! warns "connection task send loop terminated" forever, and even our
//! `join_peers` rejoin on reconnect is blackholed through the same dead
//! channel.
//!
//! The heal: dial the peer on `GOSSIP_ALPN` ourselves and hand the connection
//! to `Gossip::handle_connection`, which atomically replaces the (possibly
//! dead) active send path — iroh-gossip's own duplicate-connection handling —
//! then re-join the peer into all subscribed topics. This runs:
//!
//! - event-driven on every 0→1 peer connection (accept and dial), so a
//!   returning peer heals immediately, and
//! - on a periodic sweep with per-peer exponential backoff on dial failure,
//!   which cures the silent half-dead state (no observable signal exists at
//!   this layer for a dead send loop behind a live QUIC connection).
//!
//! After `max_attempts` consecutive failures the sweep gives up and force-
//! closes the peer's RPC connections so `PeerDisconnected` fires and the peer
//! is dropped until the next discovery/dial recreates it through the 0→1 path.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use iroh::endpoint::Connection;
use iroh::EndpointId;
use tracing::{debug, warn};

use super::endpoint::{
    join_peer_to_subscription_senders, snapshot_subscription_senders, track_task,
    EndpointResources, SubscriptionSenders, TopicSubscription,
};
use super::endpoint_rpc::{close_peer_connections, connect_with_direct_addr_fallback};
use super::peer_map::endpoint_id_to_peer_id;

/// Configuration for gossip send-path healing.
#[derive(Debug, Clone)]
pub struct GossipHealConfig {
    /// Cadence of the unconditional per-peer gossip path refresh. Zero
    /// disables healing entirely (no sweep, no 0→1 refresh).
    pub refresh_interval: Duration,
    /// First retry delay after a failed refresh dial; doubles per attempt.
    pub backoff_base: Duration,
    /// Upper bound on the retry delay.
    pub backoff_cap: Duration,
    /// Consecutive failed attempts before giving up on the peer.
    pub max_attempts: u32,
}

impl Default for GossipHealConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(60),
            backoff_base: Duration::from_secs(2),
            backoff_cap: Duration::from_secs(60),
            max_attempts: 5,
        }
    }
}

impl GossipHealConfig {
    /// Defaults with the refresh interval overridable via
    /// `DEFRA_P2P_GOSSIP_HEAL_INTERVAL_SECS` (0 disables healing).
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Ok(value) = std::env::var("DEFRA_P2P_GOSSIP_HEAL_INTERVAL_SECS") {
            if let Ok(secs) = value.parse::<u64>() {
                config.refresh_interval = Duration::from_secs(secs);
            }
        }
        config
    }

    pub fn enabled(&self) -> bool {
        !self.refresh_interval.is_zero()
    }

    /// Sweep tick granularity: fine enough to service backoff retries
    /// promptly, never finer than 100ms or coarser than the refresh interval.
    pub(super) fn tick_period(&self) -> Duration {
        if !self.enabled() {
            return Duration::from_secs(3600);
        }
        self.backoff_base
            .clamp(Duration::from_millis(100), self.refresh_interval)
    }
}

/// Treat an in-flight refresh as lost after this long (dial timeouts bound a
/// refresh to well under this), so a stuck flag cannot block healing forever.
const IN_FLIGHT_STALE: Duration = Duration::from_secs(60);

/// How long a superseded refresh connection is kept open before we close it.
/// Both sides must have adopted the replacement as their active gossip
/// connection first; closing the still-active one would make gossip interpret
/// it as a peer disconnect and churn the topic mesh. We must close eventually:
/// iroh keep-alives prevent idle cleanup, so unclosed superseded connections
/// leak one per refresh.
const SUPERSEDED_CLOSE_GRACE: Duration = Duration::from_secs(10);

struct PeerHeal {
    attempts: u32,
    next_due: Instant,
    in_flight_since: Option<Instant>,
}

/// Pure per-peer refresh schedule with exponential backoff. Time is injected
/// so the state machine is deterministic under test.
struct HealSchedule {
    config: GossipHealConfig,
    peers: HashMap<EndpointId, PeerHeal>,
}

impl HealSchedule {
    fn new(config: GossipHealConfig) -> Self {
        Self {
            config,
            peers: HashMap::new(),
        }
    }

    fn backoff_delay(&self, attempts: u32) -> Duration {
        let exp = attempts.saturating_sub(1).min(16);
        self.config
            .backoff_base
            .saturating_mul(1u32 << exp)
            .min(self.config.backoff_cap)
    }

    /// Peers whose refresh is due. Prunes departed peers, starts tracking new
    /// ones (first refresh one interval after they appear — the 0→1 heal
    /// already covered their connect), and marks returned peers in flight.
    fn due_peers(&mut self, connected: &[EndpointId], now: Instant) -> Vec<EndpointId> {
        self.peers.retain(|id, _| connected.contains(id));
        let mut due = Vec::new();
        for id in connected {
            let entry = self.peers.entry(*id).or_insert_with(|| PeerHeal {
                attempts: 0,
                next_due: now + self.config.refresh_interval,
                in_flight_since: None,
            });
            if let Some(since) = entry.in_flight_since {
                if now.duration_since(since) < IN_FLIGHT_STALE {
                    continue;
                }
            }
            if entry.next_due <= now {
                entry.in_flight_since = Some(now);
                due.push(*id);
            }
        }
        due
    }

    /// A fresh 0→1 connection: reset the schedule and mark the connect-time
    /// refresh in flight so the sweep does not double-dial.
    fn note_connected(&mut self, id: EndpointId, now: Instant) {
        self.peers.insert(
            id,
            PeerHeal {
                attempts: 0,
                next_due: now + self.config.refresh_interval,
                in_flight_since: Some(now),
            },
        );
    }

    fn record_success(&mut self, id: EndpointId, now: Instant) {
        self.peers.insert(
            id,
            PeerHeal {
                attempts: 0,
                next_due: now + self.config.refresh_interval,
                in_flight_since: None,
            },
        );
    }

    /// Returns `true` when attempts are exhausted (give up on the peer).
    fn record_failure(&mut self, id: EndpointId, now: Instant) -> bool {
        let entry = self.peers.entry(id).or_insert(PeerHeal {
            attempts: 0,
            next_due: now,
            in_flight_since: None,
        });
        entry.in_flight_since = None;
        entry.attempts += 1;
        let attempts = entry.attempts;
        if attempts >= self.config.max_attempts {
            self.peers.remove(&id);
            return true;
        }
        let delay = self.backoff_delay(attempts);
        if let Some(entry) = self.peers.get_mut(&id) {
            entry.next_due = now + delay;
        }
        false
    }
}

/// Shared healer: refresh schedule plus the gossip connections we injected,
/// retained so each refresh can close the connection it supersedes.
pub(super) struct GossipHealer {
    config: GossipHealConfig,
    schedule: parking_lot::Mutex<HealSchedule>,
    conns: parking_lot::Mutex<HashMap<EndpointId, Connection>>,
}

impl GossipHealer {
    pub(super) fn new(config: GossipHealConfig) -> Self {
        Self {
            schedule: parking_lot::Mutex::new(HealSchedule::new(config.clone())),
            conns: parking_lot::Mutex::new(HashMap::new()),
            config,
        }
    }

    pub(super) fn config(&self) -> &GossipHealConfig {
        &self.config
    }

    fn due_peers(&self, connected: &[EndpointId], now: Instant) -> Vec<EndpointId> {
        self.schedule.lock().due_peers(connected, now)
    }

    fn note_connected(&self, id: EndpointId, now: Instant) {
        self.schedule.lock().note_connected(id, now);
    }

    fn record_success(&self, id: EndpointId, now: Instant) {
        self.schedule.lock().record_success(id, now);
    }

    fn record_failure(&self, id: EndpointId, now: Instant) -> bool {
        self.schedule.lock().record_failure(id, now)
    }

    fn store_conn(&self, id: EndpointId, conn: Connection) -> Option<Connection> {
        self.conns.lock().insert(id, conn)
    }

    pub(super) fn take_conn(&self, id: &EndpointId) -> Option<Connection> {
        self.conns.lock().remove(id)
    }

    /// Injected gossip connections whose peer is no longer connected.
    fn take_departed_conns(&self, connected: &[EndpointId]) -> Vec<Connection> {
        let mut conns = self.conns.lock();
        let departed: Vec<EndpointId> = conns
            .keys()
            .filter(|id| !connected.contains(id))
            .copied()
            .collect();
        departed
            .into_iter()
            .filter_map(|id| conns.remove(&id))
            .collect()
    }
}

#[derive(Clone, Copy, PartialEq)]
enum HealContext {
    PeerConnected,
    Sweep,
}

/// Event-driven heal on a 0→1 peer connection (accept or dial): refresh the
/// gossip path and rejoin the peer into all subscribed topics. With healing
/// disabled, falls back to the plain rejoin.
pub(super) fn spawn_peer_connected_heal(
    res: &EndpointResources,
    senders: &SubscriptionSenders,
    endpoint_id: EndpointId,
) {
    if senders.is_empty() {
        return;
    }
    let spawned_tasks = res.spawned_tasks.clone();
    if !res.healer.config().enabled() {
        let senders = senders.clone();
        let task = tokio::spawn(async move {
            join_peer_to_subscription_senders(&senders, endpoint_id).await;
        });
        track_task(&spawned_tasks, task);
        return;
    }
    res.healer.note_connected(endpoint_id, Instant::now());
    let res = res.clone();
    let senders = senders.clone();
    let task = tokio::spawn(async move {
        refresh_peer(&res, &senders, endpoint_id, HealContext::PeerConnected).await;
    });
    track_task(&spawned_tasks, task);
}

/// Periodic sweep: refresh due peers and drop injected connections to
/// departed peers.
pub(super) fn sweep(res: &EndpointResources, subscriptions: &HashMap<String, TopicSubscription>) {
    let connected: Vec<EndpointId> = res.peer_map.lock().endpoint_ids().collect();
    for conn in res.healer.take_departed_conns(&connected) {
        conn.close(0u32.into(), b"gossip-heal");
    }

    let senders = snapshot_subscription_senders(subscriptions);
    if senders.is_empty() {
        return;
    }
    for endpoint_id in res.healer.due_peers(&connected, Instant::now()) {
        let task_res = res.clone();
        let senders = senders.clone();
        let task = tokio::spawn(async move {
            refresh_peer(&task_res, &senders, endpoint_id, HealContext::Sweep).await;
        });
        track_task(&res.spawned_tasks, task);
    }
}

async fn refresh_peer(
    res: &EndpointResources,
    senders: &SubscriptionSenders,
    endpoint_id: EndpointId,
    ctx: HealContext,
) {
    match dial_and_inject(res, endpoint_id).await {
        Ok(()) => {
            join_peer_to_subscription_senders(senders, endpoint_id).await;
            res.healer.record_success(endpoint_id, Instant::now());
            debug!(peer = %endpoint_id, "gossip path refreshed");
        }
        Err(error) => {
            if ctx == HealContext::PeerConnected {
                // Preserve the pre-heal rejoin: gossip's own dialer may still
                // reach the peer (e.g. via addresses only it has learned).
                join_peer_to_subscription_senders(senders, endpoint_id).await;
            }
            let gave_up = res.healer.record_failure(endpoint_id, Instant::now());
            if gave_up && ctx == HealContext::Sweep {
                // Only force-close when we successfully dialed this peer's
                // gossip path before (a tracked injected connection exists):
                // dialability regressed, so the remaining RPC connections are
                // almost certainly stale too. A peer we could never dial back
                // (e.g. NAT'd inbound, no relay) keeps its healthy inbound
                // connections; healing just cools down until re-tracked.
                if let Some(conn) = res.healer.take_conn(&endpoint_id) {
                    warn!(
                        peer = %endpoint_id,
                        error = %error,
                        "gossip path heal exhausted; dropping peer connections until rediscovery"
                    );
                    conn.close(0u32.into(), b"gossip-heal");
                    close_peer_connections(&res.peer_map, &res.connection_cache, &endpoint_id);
                } else {
                    debug!(
                        peer = %endpoint_id,
                        error = %error,
                        "gossip path heal exhausted for undialable peer; cooling down"
                    );
                }
            } else {
                debug!(
                    peer = %endpoint_id,
                    error = %error,
                    "gossip path refresh failed; retrying with backoff"
                );
            }
        }
    }
}

/// Dial `GOSSIP_ALPN` and hand the connection to gossip, replacing the active
/// send path for the peer. Closes the connection this one supersedes after a
/// grace period.
async fn dial_and_inject(
    res: &EndpointResources,
    endpoint_id: EndpointId,
) -> crate::error::Result<()> {
    let peer_id = endpoint_id_to_peer_id(&endpoint_id);
    let direct_addr = {
        let map = res.peer_map.lock();
        map.get(&endpoint_id).and_then(|info| info.remote_addr)
    };
    let conn = connect_with_direct_addr_fallback(
        &res.endpoint,
        &peer_id,
        iroh_gossip::net::GOSSIP_ALPN,
        direct_addr,
    )
    .await?;
    res.gossip
        .handle_connection(conn.clone())
        .await
        .map_err(|e| crate::error::Error::Transport(format!("gossip handle_connection: {}", e)))?;

    if let Some(previous) = res.healer.store_conn(endpoint_id, conn) {
        let task = tokio::spawn(async move {
            tokio::time::sleep(SUPERSEDED_CLOSE_GRACE).await;
            previous.close(0u32.into(), b"gossip-refresh");
        });
        track_task(&res.spawned_tasks, task);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_id() -> EndpointId {
        iroh::SecretKey::generate().public()
    }

    fn schedule() -> HealSchedule {
        HealSchedule::new(GossipHealConfig {
            refresh_interval: Duration::from_secs(60),
            backoff_base: Duration::from_secs(2),
            backoff_cap: Duration::from_secs(30),
            max_attempts: 3,
        })
    }

    #[test]
    fn new_peer_is_due_one_interval_after_appearing() {
        let mut s = schedule();
        let id = test_id();
        let t0 = Instant::now();

        assert!(s.due_peers(&[id], t0).is_empty());
        assert!(s.due_peers(&[id], t0 + Duration::from_secs(59)).is_empty());
        assert_eq!(s.due_peers(&[id], t0 + Duration::from_secs(60)), vec![id]);
    }

    #[test]
    fn in_flight_peer_is_not_redialed_until_stale() {
        let mut s = schedule();
        let id = test_id();
        let t0 = Instant::now();
        let due_at = t0 + Duration::from_secs(60);

        assert!(s.due_peers(&[id], t0).is_empty());
        assert_eq!(s.due_peers(&[id], due_at), vec![id]);
        assert!(s
            .due_peers(&[id], due_at + Duration::from_secs(1))
            .is_empty());
        assert_eq!(s.due_peers(&[id], due_at + IN_FLIGHT_STALE), vec![id]);
    }

    #[test]
    fn failure_backoff_doubles_and_caps() {
        let mut s = schedule();
        let id = test_id();
        let t0 = Instant::now();
        s.due_peers(&[id], t0);

        assert!(!s.record_failure(id, t0));
        assert!(s.due_peers(&[id], t0 + Duration::from_secs(1)).is_empty());
        assert_eq!(s.due_peers(&[id], t0 + Duration::from_secs(2)), vec![id]);

        let t1 = t0 + Duration::from_secs(2);
        assert!(!s.record_failure(id, t1));
        assert!(s.due_peers(&[id], t1 + Duration::from_secs(3)).is_empty());
        assert_eq!(s.due_peers(&[id], t1 + Duration::from_secs(4)), vec![id]);
    }

    #[test]
    fn backoff_delay_is_capped() {
        let s = schedule();
        assert_eq!(s.backoff_delay(1), Duration::from_secs(2));
        assert_eq!(s.backoff_delay(2), Duration::from_secs(4));
        assert_eq!(s.backoff_delay(4), Duration::from_secs(16));
        assert_eq!(s.backoff_delay(5), Duration::from_secs(30));
        assert_eq!(s.backoff_delay(60), Duration::from_secs(30));
    }

    #[test]
    fn gives_up_after_max_attempts_and_retracks_after_interval() {
        let mut s = schedule();
        let id = test_id();
        let t0 = Instant::now();
        s.due_peers(&[id], t0);

        assert!(!s.record_failure(id, t0));
        assert!(!s.record_failure(id, t0));
        assert!(s.record_failure(id, t0), "third failure must give up");
        assert!(!s.peers.contains_key(&id), "given-up peer is untracked");

        // If the peer somehow stays connected, tracking restarts from scratch.
        assert!(s.due_peers(&[id], t0 + Duration::from_secs(1)).is_empty());
        assert_eq!(s.due_peers(&[id], t0 + Duration::from_secs(61)), vec![id]);
    }

    #[test]
    fn success_resets_attempts() {
        let mut s = schedule();
        let id = test_id();
        let t0 = Instant::now();
        s.due_peers(&[id], t0);

        assert!(!s.record_failure(id, t0));
        assert!(!s.record_failure(id, t0));
        s.record_success(id, t0);
        assert!(!s.record_failure(id, t0), "attempts restart after success");
        assert!(!s.record_failure(id, t0));
    }

    #[test]
    fn success_schedules_next_refresh_one_interval_out() {
        let mut s = schedule();
        let id = test_id();
        let t0 = Instant::now();
        s.record_success(id, t0);

        assert!(s.due_peers(&[id], t0 + Duration::from_secs(59)).is_empty());
        assert_eq!(s.due_peers(&[id], t0 + Duration::from_secs(60)), vec![id]);
    }

    #[test]
    fn departed_peers_are_pruned() {
        let mut s = schedule();
        let id = test_id();
        let t0 = Instant::now();
        s.due_peers(&[id], t0);
        assert!(s.peers.contains_key(&id));

        s.due_peers(&[], t0 + Duration::from_secs(1));
        assert!(!s.peers.contains_key(&id));
    }

    #[test]
    fn note_connected_marks_in_flight_and_resets() {
        let mut s = schedule();
        let id = test_id();
        let t0 = Instant::now();
        s.due_peers(&[id], t0);
        s.record_failure(id, t0);

        s.note_connected(id, t0);
        assert!(
            s.due_peers(&[id], t0 + Duration::from_secs(2)).is_empty(),
            "connect-time refresh is in flight; sweep must not double-dial"
        );
        s.record_success(id, t0 + Duration::from_secs(1));
        assert_eq!(s.peers.get(&id).unwrap().attempts, 0);
    }

    #[test]
    fn disabled_config_has_idle_tick() {
        let config = GossipHealConfig {
            refresh_interval: Duration::ZERO,
            ..Default::default()
        };
        assert!(!config.enabled());
        assert_eq!(config.tick_period(), Duration::from_secs(3600));

        let enabled = GossipHealConfig::default();
        assert!(enabled.enabled());
        assert_eq!(enabled.tick_period(), Duration::from_secs(2));
    }
}
