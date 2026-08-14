//! Active connection pruning for libp2p hosts.
//!
//! Two policies share one registry: a global high-water prune that sheds whole
//! peers when the host holds too many connections, and a per-peer rule that
//! keeps a single connection to each peer. Both mark their victims `closing` in
//! the same map, so neither re-selects what the other already gave up.

use std::collections::HashMap;
use std::time::Duration;

use libp2p::{swarm::ConnectionId, PeerId};
use tokio::time::Instant;

/// How a connection reaches its peer.
///
/// Ordered so `Direct` outranks `Relayed`: a circuit-relayed connection is only
/// ever a fallback, and a direct one that arrives later must replace it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ConnectionPath {
    Relayed,
    Direct,
}

#[derive(Debug)]
pub(super) struct ActiveConnectionManager {
    low_water: usize,
    high_water: usize,
    grace_period: Duration,
    connections: HashMap<ConnectionId, ManagedConnection>,
}

#[derive(Debug)]
pub(super) struct PruneCandidate {
    pub connection_id: ConnectionId,
    pub peer_id: PeerId,
    pub age: Duration,
}

#[derive(Debug)]
struct ManagedConnection {
    peer_id: PeerId,
    path: ConnectionPath,
    dialed_by_us: bool,
    established_at: Instant,
    closing: bool,
}

impl ActiveConnectionManager {
    pub(super) fn new(low_water: u32, high_water: u32, grace_period: Duration) -> Self {
        let low_water = low_water as usize;
        let high_water = high_water.max(low_water as u32) as usize;

        Self {
            low_water,
            high_water,
            grace_period,
            connections: HashMap::new(),
        }
    }

    pub(super) fn on_established(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        path: ConnectionPath,
        dialed_by_us: bool,
        now: Instant,
    ) {
        self.connections.insert(
            connection_id,
            ManagedConnection {
                peer_id,
                path,
                dialed_by_us,
                established_at: now,
                closing: false,
            },
        );
    }

    pub(super) fn on_closed(&mut self, connection_id: ConnectionId) {
        self.connections.remove(&connection_id);
    }

    /// Connections to `peer_id` that must close so exactly one survives.
    ///
    /// A second connection to the same peer is what breaks gossipsub against Go
    /// (#1449): rust-libp2p gives every connection its own handler draining one
    /// shared per-peer send queue, so each opens its own `/meshsub` substream,
    /// while go-libp2p-pubsub keeps a single inbound stream per peer and resets
    /// the rest. The survivors then ping-pong until rust-libp2p disables the
    /// handler, silencing this node's gossip to that peer for good.
    ///
    /// The survivor is the connection on the best available path, so a direct
    /// connection always displaces a relayed one — the relayed-to-direct upgrade
    /// keeps the direct link and sheds the relay, never the reverse.
    ///
    /// Within a path the winner is the one dialed by the lower of the two peer
    /// ids. Both ends compute that from the same two ids and the same direction,
    /// so they always drop the same connection. Choosing locally instead — by
    /// age, say — lets two peers that dialed each other simultaneously each drop
    /// what the other kept and sever the link outright.
    pub(super) fn redundant_connections(
        &mut self,
        local_peer_id: PeerId,
        peer_id: PeerId,
    ) -> Vec<ConnectionId> {
        let keep_our_dial = local_peer_id < peer_id;
        let mut live: Vec<(ConnectionId, ConnectionPath, bool, Instant)> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.peer_id == peer_id && !connection.closing)
            .map(|(id, connection)| {
                (
                    *id,
                    connection.path,
                    connection.dialed_by_us == keep_our_dial,
                    connection.established_at,
                )
            })
            .collect();
        if live.len() < 2 {
            return Vec::new();
        }

        live.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)).then(a.3.cmp(&b.3)));
        let redundant: Vec<ConnectionId> = live.into_iter().skip(1).map(|(id, ..)| id).collect();
        for connection_id in &redundant {
            if let Some(connection) = self.connections.get_mut(connection_id) {
                connection.closing = true;
            }
        }
        redundant
    }

    pub(super) fn prune_candidates(&mut self, now: Instant) -> Vec<PruneCandidate> {
        let active_count = self
            .connections
            .values()
            .filter(|connection| !connection.closing)
            .count();

        if active_count <= self.high_water {
            return Vec::new();
        }

        let target_count = self.low_water.min(self.high_water);
        let needed = active_count.saturating_sub(target_count);
        if needed == 0 {
            return Vec::new();
        }

        let mut peers = HashMap::<PeerId, (Instant, Vec<ConnectionId>)>::new();
        for (connection_id, connection) in &self.connections {
            let peer = peers
                .entry(connection.peer_id)
                .or_insert_with(|| (connection.established_at, Vec::new()));
            peer.0 = peer.0.min(connection.established_at);
            if !connection.closing {
                peer.1.push(*connection_id);
            }
        }

        let mut eligible = peers
            .into_iter()
            .filter(|(_, (first_seen, connections))| {
                !connections.is_empty() && now.duration_since(*first_seen) >= self.grace_period
            })
            .collect::<Vec<_>>();
        eligible.sort_by_key(|(_, (first_seen, _))| *first_seen);

        let mut selected = Vec::with_capacity(needed);
        for (peer_id, (_, connection_ids)) in eligible {
            if selected.len() >= needed {
                break;
            }
            for connection_id in connection_ids {
                let Some(connection) = self.connections.get_mut(&connection_id) else {
                    continue;
                };
                connection.closing = true;
                selected.push(PruneCandidate {
                    connection_id,
                    peer_id,
                    age: now.duration_since(connection.established_at),
                });
            }
        }
        selected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection_id(id: usize) -> ConnectionId {
        ConnectionId::new_unchecked(id)
    }

    fn direct(
        manager: &mut ActiveConnectionManager,
        id: usize,
        peer_id: PeerId,
        established_at: Instant,
    ) {
        manager.on_established(
            connection_id(id),
            peer_id,
            ConnectionPath::Direct,
            true,
            established_at,
        );
    }

    /// A `(low, high)` peer-id pair, so tests can state which side dials.
    fn ordered_peers() -> (PeerId, PeerId) {
        let (a, b) = (PeerId::random(), PeerId::random());
        if a < b {
            (a, b)
        } else {
            (b, a)
        }
    }

    #[test]
    fn below_high_water_does_not_prune() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let now = Instant::now();

        for id in 0..4 {
            direct(&mut manager, id, PeerId::random(), now);
        }

        assert!(manager
            .prune_candidates(now + Duration::from_secs(30))
            .is_empty());
    }

    #[test]
    fn grace_period_protects_new_connections() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let now = Instant::now();

        for id in 0..5 {
            direct(&mut manager, id, PeerId::random(), now);
        }

        assert!(manager
            .prune_candidates(now + Duration::from_secs(10))
            .is_empty());
    }

    #[test]
    fn prunes_oldest_connections_toward_low_water() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let now = Instant::now();

        for id in 0..5 {
            direct(
                &mut manager,
                id,
                PeerId::random(),
                now + Duration::from_secs(id as u64),
            );
        }

        let candidates = manager.prune_candidates(now + Duration::from_secs(30));

        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.connection_id)
                .collect::<Vec<_>>(),
            vec![connection_id(0), connection_id(1), connection_id(2)]
        );
    }

    #[test]
    fn prunes_all_connections_for_oldest_peer() {
        let mut manager = ActiveConnectionManager::new(2, 3, Duration::from_secs(20));
        let now = Instant::now();
        let oldest_peer = PeerId::random();

        direct(&mut manager, 0, oldest_peer, now);
        direct(&mut manager, 1, oldest_peer, now + Duration::from_secs(25));
        direct(
            &mut manager,
            2,
            PeerId::random(),
            now + Duration::from_secs(1),
        );
        direct(
            &mut manager,
            3,
            PeerId::random(),
            now + Duration::from_secs(2),
        );

        let mut candidates = manager
            .prune_candidates(now + Duration::from_secs(30))
            .into_iter()
            .map(|candidate| candidate.connection_id)
            .collect::<Vec<_>>();
        candidates.sort();

        assert_eq!(candidates, vec![connection_id(0), connection_id(1)]);
    }

    #[test]
    fn closing_candidates_are_not_repeated() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let now = Instant::now();

        for id in 0..5 {
            direct(&mut manager, id, PeerId::random(), now);
        }

        assert_eq!(
            manager
                .prune_candidates(now + Duration::from_secs(30))
                .len(),
            3
        );
        assert!(manager
            .prune_candidates(now + Duration::from_secs(31))
            .is_empty());
    }

    #[test]
    fn single_connection_is_never_redundant() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let (local, remote) = ordered_peers();

        direct(&mut manager, 0, remote, Instant::now());

        assert!(manager.redundant_connections(local, remote).is_empty());
    }

    /// #1449: the second connection to a peer is what breaks gossipsub against
    /// Go. The lower peer id owns the surviving dial.
    #[test]
    fn duplicate_direct_connection_keeps_the_dial_from_the_lower_peer_id() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let now = Instant::now();
        let (local, remote) = ordered_peers();

        manager.on_established(connection_id(0), remote, ConnectionPath::Direct, true, now);
        manager.on_established(
            connection_id(1),
            remote,
            ConnectionPath::Direct,
            false,
            now + Duration::from_secs(1),
        );

        assert_eq!(
            manager.redundant_connections(local, remote),
            vec![connection_id(1)]
        );
    }

    /// The same simultaneous connect seen from both nodes. Connection 0 is the
    /// dial the low peer made, connection 1 the dial the high peer made, so each
    /// node records them with opposite `dialed_by_us`. Both must drop 1 — if
    /// they disagreed, each would close what the other kept and sever the link.
    #[test]
    fn both_ends_drop_the_same_connection() {
        let (low, high) = ordered_peers();
        let now = Instant::now();

        let mut at_low = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        at_low.on_established(connection_id(0), high, ConnectionPath::Direct, true, now);
        at_low.on_established(
            connection_id(1),
            high,
            ConnectionPath::Direct,
            false,
            now + Duration::from_secs(1),
        );

        let mut at_high = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        at_high.on_established(connection_id(0), low, ConnectionPath::Direct, false, now);
        at_high.on_established(
            connection_id(1),
            low,
            ConnectionPath::Direct,
            true,
            now + Duration::from_secs(1),
        );

        assert_eq!(
            at_low.redundant_connections(low, high),
            vec![connection_id(1)]
        );
        assert_eq!(
            at_high.redundant_connections(high, low),
            vec![connection_id(1)]
        );
    }

    /// The relayed-to-direct upgrade: a direct connection arriving after a
    /// relayed one must survive, and the relay is what gets dropped.
    #[test]
    fn direct_connection_supersedes_an_older_relayed_one() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let now = Instant::now();
        let (local, remote) = ordered_peers();

        manager.on_established(connection_id(0), remote, ConnectionPath::Relayed, true, now);
        manager.on_established(
            connection_id(1),
            remote,
            ConnectionPath::Direct,
            false,
            now + Duration::from_secs(5),
        );

        assert_eq!(
            manager.redundant_connections(local, remote),
            vec![connection_id(0)]
        );
    }

    /// The mirror: a relay arriving while a direct connection is live is the
    /// redundant one, so a late relay cannot displace a working direct link.
    #[test]
    fn relayed_connection_never_supersedes_a_direct_one() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let now = Instant::now();
        let (local, remote) = ordered_peers();

        manager.on_established(connection_id(0), remote, ConnectionPath::Direct, false, now);
        manager.on_established(
            connection_id(1),
            remote,
            ConnectionPath::Relayed,
            true,
            now + Duration::from_secs(5),
        );

        assert_eq!(
            manager.redundant_connections(local, remote),
            vec![connection_id(1)]
        );
    }

    #[test]
    fn redundant_connections_are_not_reported_twice() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let now = Instant::now();
        let (local, remote) = ordered_peers();

        direct(&mut manager, 0, remote, now);
        direct(&mut manager, 1, remote, now + Duration::from_secs(1));

        assert_eq!(manager.redundant_connections(local, remote).len(), 1);
        assert!(manager.redundant_connections(local, remote).is_empty());
    }

    /// Per-peer dedup and the high-water prune share one registry, so a
    /// connection one policy already gave up is invisible to the other.
    #[test]
    fn watermark_prune_skips_connections_dedup_already_closed() {
        let mut manager = ActiveConnectionManager::new(1, 1, Duration::from_secs(20));
        let now = Instant::now();
        let (local, remote) = ordered_peers();

        direct(&mut manager, 0, remote, now);
        direct(&mut manager, 1, remote, now + Duration::from_secs(1));
        direct(
            &mut manager,
            2,
            PeerId::random(),
            now + Duration::from_secs(2),
        );
        assert_eq!(
            manager.redundant_connections(local, remote),
            vec![connection_id(1)]
        );

        let candidates = manager
            .prune_candidates(now + Duration::from_secs(30))
            .into_iter()
            .map(|candidate| candidate.connection_id)
            .collect::<Vec<_>>();

        assert_eq!(candidates, vec![connection_id(0)]);
    }

    /// The other half of composing: dedup lowers the active count, so a host
    /// that was only over its high-water mark because of duplicates stops
    /// shedding whole peers.
    #[test]
    fn dedup_relieves_high_water_pressure() {
        let mut manager = ActiveConnectionManager::new(1, 2, Duration::from_secs(20));
        let now = Instant::now();
        let (local, remote) = ordered_peers();

        direct(&mut manager, 0, remote, now);
        direct(&mut manager, 1, remote, now + Duration::from_secs(1));
        direct(
            &mut manager,
            2,
            PeerId::random(),
            now + Duration::from_secs(2),
        );
        assert_eq!(manager.redundant_connections(local, remote).len(), 1);

        assert!(manager
            .prune_candidates(now + Duration::from_secs(30))
            .is_empty());
    }
}
