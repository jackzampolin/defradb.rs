use regex::Regex;

/// A named log pattern for event-driven test verification.
pub struct NamedPattern {
    pub name: &'static str,
    pub regex: Regex,
}

/// Standard log patterns emitted by DefraDB nodes (Go and Rust converged).
pub fn node_patterns() -> Vec<NamedPattern> {
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

/// Log patterns emitted by Rust DefraDB nodes using iroh transport.
pub fn iroh_patterns() -> Vec<NamedPattern> {
    vec![
        NamedPattern {
            name: "peer_connected",
            regex: Regex::new(r"Peer connected \(iroh\)").unwrap(),
        },
        NamedPattern {
            name: "peer_disconnected",
            regex: Regex::new(r"Peer disconnected \(iroh\)").unwrap(),
        },
        NamedPattern {
            name: "replication_started",
            regex: Regex::new(r"Starting.*replication loop").unwrap(),
        },
        NamedPattern {
            name: "p2p_listening",
            regex: Regex::new(r"Iroh transport initialized").unwrap(),
        },
    ]
}
