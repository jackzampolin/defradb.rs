use db::merge::broadcast_mutator::broadcast::broadcast_retry_delay_ms;

#[test]
fn insufficient_peers_without_connections_does_not_retry() {
    let delay = broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 0, 1);
    assert_eq!(delay, None);
}

#[test]
fn insufficient_peers_with_two_connections_uses_peer_aware_backoff() {
    let delay = broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 2, 3);
    assert_eq!(delay, Some(1600));
}

#[test]
fn insufficient_peers_with_one_connection_waits_longer_than_many_peers() {
    let sparse_delay =
        broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 1, 3).unwrap();
    let many_peer_delay =
        broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 8, 3).unwrap();

    assert!(sparse_delay > many_peer_delay);
    assert_eq!(many_peer_delay, 800);
}

#[test]
fn insufficient_peers_retry_delay_is_capped() {
    let delay = broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 1, 10);
    assert_eq!(delay, Some(10_000));
}

#[test]
fn non_retryable_broadcast_errors_fail_fast() {
    let delay = broadcast_retry_delay_ms("gossipsub publish error: MessageTooLarge", 2, 1);
    assert_eq!(delay, None);
}
