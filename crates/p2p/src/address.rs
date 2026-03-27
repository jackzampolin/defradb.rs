/// Parsed multiaddr containing peer ID and transport address.
#[derive(Debug, Clone)]
pub struct ParsedMultiaddr {
    pub peer_id: libp2p::PeerId,
    pub transport_addr: libp2p::Multiaddr,
}

/// Parse a full multiaddr string that includes a peer ID.
///
/// Expects format like: `/ip4/127.0.0.1/tcp/9171/p2p/12D3KooW...`
///
/// Returns the peer ID and the transport address (without /p2p component).
pub fn parse_multiaddr_with_peer_id(addr_str: &str) -> crate::error::Result<ParsedMultiaddr> {
    use crate::error::Error;

    let full_addr: libp2p::Multiaddr = addr_str
        .parse()
        .map_err(|e: libp2p::multiaddr::Error| Error::InvalidMultiaddr(e.to_string()))?;

    let peer_id = full_addr
        .iter()
        .find_map(|p| match p {
            libp2p::multiaddr::Protocol::P2p(id) => Some(id),
            _ => None,
        })
        .ok_or_else(|| Error::MissingPeerIdInMultiaddr {
            addr: addr_str.to_string(),
        })?;

    let transport_addr: libp2p::Multiaddr = full_addr
        .iter()
        .filter(|p| !matches!(p, libp2p::multiaddr::Protocol::P2p(_)))
        .collect();

    Ok(ParsedMultiaddr {
        peer_id,
        transport_addr,
    })
}
