use regex::Regex;

/// A named log pattern for event-driven test verification.
pub struct NamedPattern {
    pub name: &'static str,
    pub regex: Regex,
}

/// Standard log patterns emitted by Rust DefraDB nodes.
pub fn rust_patterns() -> Vec<NamedPattern> {
    vec![
        NamedPattern {
            name: "peer_connected",
            regex: Regex::new(r"Peer connected: ").unwrap(),
        },
        NamedPattern {
            name: "peer_disconnected",
            regex: Regex::new(r"Peer disconnected: ").unwrap(),
        },
        NamedPattern {
            name: "replication_started",
            regex: Regex::new(r"Starting replication loop").unwrap(),
        },
        NamedPattern {
            name: "p2p_listening",
            regex: Regex::new(r"Now listening on: ").unwrap(),
        },
    ]
}

/// Standard log patterns emitted by Go DefraDB nodes.
///
/// Go DefraDB uses different log strings from its go-p2p library.
pub fn go_patterns() -> Vec<NamedPattern> {
    vec![
        NamedPattern {
            name: "peer_connected",
            regex: Regex::new(r"Peer connected|PeerConnect").unwrap(),
        },
        NamedPattern {
            name: "peer_disconnected",
            regex: Regex::new(r"Peer disconnected|PeerDisconnect").unwrap(),
        },
        NamedPattern {
            name: "replication_started",
            regex: Regex::new(r"Starting replication loop").unwrap(),
        },
        NamedPattern {
            name: "p2p_listening",
            regex: Regex::new(r"Created LibP2P host").unwrap(),
        },
    ]
}
