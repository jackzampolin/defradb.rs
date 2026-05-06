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

        let mut eligible = self
            .connections
            .iter()
            .filter_map(|(connection_id, connection)| {
                let age = now.duration_since(connection.established_at);
                (!connection.closing && age >= self.grace_period).then_some((
                    *connection_id,
                    connection.peer_id,
                    connection.established_at,
                    age,
                ))
            })
            .collect::<Vec<_>>();

        eligible.sort_by_key(|(_, _, established_at, _)| *established_at);

        eligible
            .into_iter()
            .take(needed)
            .filter_map(|(connection_id, peer_id, _, age)| {
                let connection = self.connections.get_mut(&connection_id)?;
                connection.closing = true;
                Some(PruneCandidate {
                    connection_id,
                    peer_id,
                    age,
                })
            })
            .collect()
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
