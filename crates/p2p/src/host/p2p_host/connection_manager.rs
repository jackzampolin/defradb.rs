//! Active connection pruning for libp2p hosts.

use std::collections::HashMap;
use std::time::Duration;

use libp2p::{swarm::ConnectionId, PeerId};
use tokio::time::Instant;

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
        now: Instant,
    ) {
        self.connections.insert(
            connection_id,
            ManagedConnection {
                peer_id,
                established_at: now,
                closing: false,
            },
        );
    }

    pub(super) fn on_closed(&mut self, connection_id: ConnectionId) {
        self.connections.remove(&connection_id);
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

    #[test]
    fn below_high_water_does_not_prune() {
        let mut manager = ActiveConnectionManager::new(2, 4, Duration::from_secs(20));
        let now = Instant::now();

        for id in 0..4 {
            manager.on_established(connection_id(id), PeerId::random(), now);
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
            manager.on_established(connection_id(id), PeerId::random(), now);
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
            manager.on_established(
                connection_id(id),
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

        manager.on_established(connection_id(0), oldest_peer, now);
        manager.on_established(connection_id(1), oldest_peer, now + Duration::from_secs(25));
        manager.on_established(
            connection_id(2),
            PeerId::random(),
            now + Duration::from_secs(1),
        );
        manager.on_established(
            connection_id(3),
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
            manager.on_established(connection_id(id), PeerId::random(), now);
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
}
