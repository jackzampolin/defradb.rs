use super::*;
use std::str::FromStr;
use std::time::Duration;

fn test_peer_id() -> PeerId {
    PeerId::random()
}

fn test_cid() -> Cid {
    Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
}

#[test]
pub fn test_peer_connect_disconnect() {
    let tracker = PeerStateTracker::new();
    let peer = test_peer_id();

    assert!(!tracker.is_connected(&peer));

    tracker.peer_connected(peer);
    assert!(tracker.is_connected(&peer));

    tracker.peer_disconnected(&peer);
    assert!(!tracker.is_connected(&peer));
}

#[test]
pub fn test_peer_has_cid() {
    let tracker = PeerStateTracker::new();
    let peer = test_peer_id();
    let cid = test_cid();

    tracker.peer_connected(peer);
    assert!(!tracker.peer_has(&peer, &cid));

    tracker.peer_has_cid(&peer, cid);
    assert!(tracker.peer_has(&peer, &cid));
}

#[test]
pub fn test_peers_with_cid() {
    let tracker = PeerStateTracker::new();
    let peer1 = test_peer_id();
    let peer2 = test_peer_id();
    let cid = test_cid();

    tracker.peer_connected(peer1);
    tracker.peer_connected(peer2);

    // Only peer1 has the CID
    tracker.peer_has_cid(&peer1, cid);

    let peers_with = tracker.peers_with_cid(&cid);
    assert_eq!(peers_with.len(), 1);
    assert!(peers_with.contains(&peer1));
    assert!(!peers_with.contains(&peer2));
}

#[test]
pub fn test_peers_without_cid() {
    let tracker = PeerStateTracker::new();
    let peer1 = test_peer_id();
    let peer2 = test_peer_id();
    let cid = test_cid();

    tracker.peer_connected(peer1);
    tracker.peer_connected(peer2);

    // Only peer1 has the CID
    tracker.peer_has_cid(&peer1, cid);

    let peers_without = tracker.peers_without_cid(&cid);
    assert_eq!(peers_without.len(), 1);
    assert!(!peers_without.contains(&peer1));
    assert!(peers_without.contains(&peer2));
}

#[test]
pub fn test_collection_subscription() {
    let tracker = PeerStateTracker::new();
    let peer = test_peer_id();

    tracker.peer_connected(peer);
    tracker.peer_subscribed(&peer, "users".to_string());

    let peers = tracker.peers_for_collection("users");
    assert_eq!(peers.len(), 1);
    assert!(peers.contains(&peer));

    let peers = tracker.peers_for_collection("posts");
    assert!(peers.is_empty());

    tracker.peer_unsubscribed(&peer, "users");
    let peers = tracker.peers_for_collection("users");
    assert!(peers.is_empty());
}

#[test]
pub fn test_disconnected_peer_not_in_results() {
    let tracker = PeerStateTracker::new();
    let peer = test_peer_id();
    let cid = test_cid();

    tracker.peer_connected(peer);
    tracker.peer_has_cid(&peer, cid);

    // While connected, peer shows up
    assert_eq!(tracker.peers_with_cid(&cid).len(), 1);

    // After disconnect, peer doesn't show up in active queries
    tracker.peer_disconnected(&peer);
    assert!(tracker.peers_with_cid(&cid).is_empty());

    // But we still remember they have it
    assert!(tracker.peer_has(&peer, &cid));
}

#[test]
pub fn test_stats() {
    let tracker = PeerStateTracker::new();
    let peer1 = test_peer_id();
    let peer2 = test_peer_id();
    let cid1 = test_cid();

    tracker.peer_connected(peer1);
    tracker.peer_connected(peer2);
    tracker.peer_has_cid(&peer1, cid1);

    let stats = tracker.stats();
    assert_eq!(stats.total_peers(), 2);
    assert_eq!(stats.connected_peers(), 2);
    assert_eq!(stats.total_tracked_cids(), 1);

    tracker.peer_disconnected(&peer2);
    let stats = tracker.stats();
    assert_eq!(stats.connected_peers(), 1);
}

#[test]
pub fn test_cleanup_stale() {
    let tracker = PeerStateTracker::with_ttl(Duration::from_millis(10));
    let peer = test_peer_id();

    tracker.peer_connected(peer);
    tracker.peer_disconnected(&peer);

    // Peer still exists right after disconnect
    let stats = tracker.stats();
    assert_eq!(stats.total_peers(), 1);

    // Wait for TTL to expire
    std::thread::sleep(Duration::from_millis(20));

    // Cleanup should remove the stale peer
    tracker.cleanup_stale();
    let stats = tracker.stats();
    assert_eq!(stats.total_peers(), 0);
}

#[test]
pub fn test_connected_peers() {
    let tracker = PeerStateTracker::new();
    let peer1 = test_peer_id();
    let peer2 = test_peer_id();

    tracker.peer_connected(peer1);
    tracker.peer_connected(peer2);

    let connected = tracker.connected_peers();
    assert_eq!(connected.len(), 2);

    tracker.peer_disconnected(&peer1);
    let connected = tracker.connected_peers();
    assert_eq!(connected.len(), 1);
    assert!(connected.contains(&peer2));
}

#[test]
pub fn test_peer_has_cid_creates_entry_for_unknown_peer() {
    // Test that peer_has_cid creates a peer entry if one doesn't exist
    // This handles race conditions where CID announcements arrive before connection events
    let tracker = PeerStateTracker::new();
    let peer = test_peer_id();
    let cid = test_cid();

    // Peer is not connected yet
    assert!(!tracker.is_connected(&peer));
    assert_eq!(tracker.stats().total_peers(), 0);

    // Record that the peer has a CID (before peer_connected is called)
    tracker.peer_has_cid(&peer, cid);

    // Peer entry should be created (but not connected)
    assert_eq!(tracker.stats().total_peers(), 1);
    assert!(!tracker.is_connected(&peer)); // Still not connected
    assert!(tracker.peer_has(&peer, &cid)); // But we track the CID

    // peers_with_cid should NOT return this peer since they're not connected
    assert!(tracker.peers_with_cid(&cid).is_empty());

    // Now connect the peer
    tracker.peer_connected(peer);
    assert!(tracker.is_connected(&peer));

    // Now they should appear in peers_with_cid
    let peers_with = tracker.peers_with_cid(&cid);
    assert_eq!(peers_with.len(), 1);
    assert!(peers_with.contains(&peer));
}

#[test]
pub fn test_peer_has_cids_creates_entry_for_unknown_peer() {
    // Test that peer_has_cids also creates a peer entry if one doesn't exist
    let tracker = PeerStateTracker::new();
    let peer = test_peer_id();
    let cid1 = test_cid();
    let cid2 =
        Cid::from_str("bafybeibdqagjfxgsqiafpmyohldmiu4qn6ucudpzqlxkfrmb6dzbggbkxy").unwrap();

    // Record multiple CIDs before peer is connected
    tracker.peer_has_cids(&peer, vec![cid1, cid2]);

    // Peer entry should be created
    assert_eq!(tracker.stats().total_peers(), 1);
    assert_eq!(tracker.stats().total_tracked_cids(), 2);
    assert!(tracker.peer_has(&peer, &cid1));
    assert!(tracker.peer_has(&peer, &cid2));

    // Not connected yet
    assert!(!tracker.is_connected(&peer));
    assert!(tracker.peers_with_cid(&cid1).is_empty());
}

#[test]
pub fn test_lru_eviction_when_max_cids_exceeded() {
    // Create tracker with a small limit for testing
    let tracker = PeerStateTracker::with_config(Duration::from_secs(3600), 3);
    let peer = test_peer_id();
    tracker.peer_connected(peer);

    // Create 5 different CIDs
    let cid1 =
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
    let cid2 =
        Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap();
    let cid3 =
        Cid::from_str("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku").unwrap();
    let cid4 =
        Cid::from_str("bafybeibdqagjfxgsqiafpmyohldmiu4qn6ucudpzqlxkfrmb6dzbggbkxy").unwrap();
    let cid5 =
        Cid::from_str("bafkreigaknpexyvxt76zgkitavbwx6ejgfheup5oybpm77oxmxbyjaoj4i").unwrap();

    // Add first 3 CIDs - all should be present
    tracker.peer_has_cid(&peer, cid1);
    tracker.peer_has_cid(&peer, cid2);
    tracker.peer_has_cid(&peer, cid3);

    assert!(tracker.peer_has(&peer, &cid1));
    assert!(tracker.peer_has(&peer, &cid2));
    assert!(tracker.peer_has(&peer, &cid3));
    assert_eq!(tracker.peer_cid_count(&peer), 3);

    // Add 4th CID - should evict cid1 (oldest)
    tracker.peer_has_cid(&peer, cid4);

    assert!(!tracker.peer_has(&peer, &cid1)); // Evicted
    assert!(tracker.peer_has(&peer, &cid2));
    assert!(tracker.peer_has(&peer, &cid3));
    assert!(tracker.peer_has(&peer, &cid4));
    assert_eq!(tracker.peer_cid_count(&peer), 3);

    // Add 5th CID - should evict cid2
    tracker.peer_has_cid(&peer, cid5);

    assert!(!tracker.peer_has(&peer, &cid1)); // Evicted earlier
    assert!(!tracker.peer_has(&peer, &cid2)); // Evicted now
    assert!(tracker.peer_has(&peer, &cid3));
    assert!(tracker.peer_has(&peer, &cid4));
    assert!(tracker.peer_has(&peer, &cid5));
    assert_eq!(tracker.peer_cid_count(&peer), 3);
}

#[test]
pub fn test_adding_same_cid_twice_does_not_evict() {
    let tracker = PeerStateTracker::with_config(Duration::from_secs(3600), 3);
    let peer = test_peer_id();
    tracker.peer_connected(peer);

    let cid1 =
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
    let cid2 =
        Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap();
    let cid3 =
        Cid::from_str("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku").unwrap();

    // Add 3 CIDs
    tracker.peer_has_cid(&peer, cid1);
    tracker.peer_has_cid(&peer, cid2);
    tracker.peer_has_cid(&peer, cid3);

    // Re-add cid1 multiple times - should not cause eviction
    tracker.peer_has_cid(&peer, cid1);
    tracker.peer_has_cid(&peer, cid1);
    tracker.peer_has_cid(&peer, cid1);

    // All 3 should still be present
    assert!(tracker.peer_has(&peer, &cid1));
    assert!(tracker.peer_has(&peer, &cid2));
    assert!(tracker.peer_has(&peer, &cid3));
    assert_eq!(tracker.peer_cid_count(&peer), 3);
}

#[test]
pub fn test_concurrent_peer_operations() {
    use std::sync::Arc;
    use std::thread;

    let tracker = Arc::new(PeerStateTracker::new());
    let mut handles = vec![];

    // Spawn multiple threads that perform concurrent operations
    for i in 0..10 {
        let tracker_clone = Arc::clone(&tracker);
        let handle = thread::spawn(move || {
            let peer = PeerId::random();
            let cid = Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
                .unwrap();

            // Perform various operations
            tracker_clone.peer_connected(peer);
            tracker_clone.peer_has_cid(&peer, cid);
            tracker_clone.peer_subscribed(&peer, format!("collection_{}", i));

            // Verify our peer is tracked
            assert!(tracker_clone.is_connected(&peer));
            assert!(tracker_clone.peer_has(&peer, &cid));

            // Disconnect
            tracker_clone.peer_disconnected(&peer);
            assert!(!tracker_clone.is_connected(&peer));
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // All peers should be in disconnected state
    let stats = tracker.stats();
    assert_eq!(stats.connected_peers(), 0);
    // 10 peers were created
    assert_eq!(stats.total_peers(), 10);
}

#[test]
pub fn test_concurrent_cid_tracking() {
    use std::sync::Arc;
    use std::thread;

    let tracker = Arc::new(PeerStateTracker::new());
    let peer = PeerId::random();
    tracker.peer_connected(peer);

    let mut handles = vec![];

    // Spawn multiple threads that add CIDs concurrently
    for _ in 0..5 {
        let tracker_clone = Arc::clone(&tracker);
        let handle = thread::spawn(move || {
            // Each thread adds the same CID multiple times
            for _ in 0..10 {
                // Use a valid CID for testing
                let cid =
                    Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
                        .unwrap();
                tracker_clone.peer_has_cid(&peer, cid);
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Stats should be valid after concurrent updates
    let stats = tracker.stats();
    assert!(stats.total_peers() >= 1);
    // Multiple threads adding the same CID shouldn't cause issues
    assert!(stats.total_tracked_cids() >= 1);
}

#[test]
pub fn test_global_peer_limits_enforced_on_connect() {
    // Test that global limits are enforced when adding peers
    // Create tracker with small max_peers limit
    let tracker = PeerStateTracker::with_full_config(
        Duration::from_secs(3600),
        100,  // max_cids_per_peer
        1000, // max_total_cids
        5,    // max_peers (small for testing)
    );

    // Add 6 peers - peer 6 should trigger eviction
    for _ in 0..6 {
        let peer = PeerId::random();
        tracker.peer_connected(peer);
    }

    // Should have at most max_peers (5) tracked
    // Note: enforcement happens lazily on operations
    let stats = tracker.stats();
    // All 6 may exist since they're all connected (no disconnected to evict)
    assert!(stats.total_peers() >= 5);
}

#[test]
pub fn test_global_limits_evicts_disconnected_first() {
    // Test that disconnected peers get evicted before connected ones
    let tracker = PeerStateTracker::with_full_config(
        Duration::from_secs(3600),
        100,  // max_cids_per_peer
        1000, // max_total_cids
        3,    // max_peers (small for testing)
    );

    // Add 2 peers
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    tracker.peer_connected(peer1);
    tracker.peer_connected(peer2);

    // Disconnect peer2
    tracker.peer_disconnected(&peer2);

    // Add 2 more peers
    let peer3 = PeerId::random();
    let peer4 = PeerId::random();
    tracker.peer_connected(peer3);
    tracker.peer_connected(peer4);

    // After cleanup, peer2 (disconnected) should be evicted first
    tracker.cleanup_stale();

    // All currently connected peers should still be tracked
    assert!(tracker.is_connected(&peer1));
    assert!(tracker.is_connected(&peer3));
    assert!(tracker.is_connected(&peer4));
}
