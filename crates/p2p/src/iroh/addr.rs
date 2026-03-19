//! Helpers for formatting and parsing public iroh peer addresses.

use std::net::SocketAddr;
use std::str::FromStr;

use iroh::{EndpointAddr, RelayUrl};
use iroh_tickets::endpoint::EndpointTicket;

use crate::error::{Error, Result};
use crate::transport::{PeerAddr, PeerId};

use super::peer_map::parse_endpoint_id;

/// Convert an [`EndpointAddr`] into a shareable ticket string.
pub fn endpoint_ticket_string(endpoint_addr: &EndpointAddr) -> String {
    EndpointTicket::from(endpoint_addr.clone()).to_string()
}

/// Returns true if the string parses as an iroh endpoint ticket.
pub fn is_ticket_string(value: &str) -> bool {
    EndpointTicket::from_str(value.trim()).is_ok()
}

/// Parse a public iroh address into a peer id and dial hints.
///
/// Accepted forms:
/// - `endpoint...` ticket strings
/// - `iroh://<endpoint-id>`
/// - `<endpoint-id>`
/// - `<endpoint-id>@<host>:<port>`
/// - `<host>:<port>/p2p/<endpoint-id>`
pub fn parse_public_peer_addr(addr: &str) -> Result<(PeerId, Vec<PeerAddr>)> {
    let trimmed = addr.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidPeerId("empty iroh address".to_string()));
    }

    if let Ok(ticket) = EndpointTicket::from_str(trimmed) {
        let endpoint_addr = ticket.endpoint_addr();
        let peer_id = PeerId::new(endpoint_addr.id.to_string());
        let addrs = endpoint_addr
            .addrs
            .iter()
            .map(|addr| match addr {
                iroh::TransportAddr::Relay(relay_url) => PeerAddr::new(relay_url.to_string()),
                iroh::TransportAddr::Ip(socket_addr) => PeerAddr::new(socket_addr.to_string()),
                _ => PeerAddr::new(String::new()),
            })
            .filter(|addr| !addr.as_str().is_empty())
            .collect();
        return Ok((peer_id, addrs));
    }

    if let Some((endpoint_id, host_port)) = trimmed.split_once('@') {
        return Ok((
            PeerId::new(normalize_endpoint_id(endpoint_id)),
            vec![PeerAddr::new(host_port.to_string())],
        ));
    }

    if let Some(pos) = trimmed.rfind("/p2p/") {
        let addr_part = &trimmed[..pos];
        let id_part = &trimmed[pos + 5..];
        return Ok((
            PeerId::new(normalize_endpoint_id(id_part)),
            vec![PeerAddr::new(addr_part.to_string())],
        ));
    }

    Ok((PeerId::new(normalize_endpoint_id(trimmed)), Vec::new()))
}

/// Render raw iroh endpoint listen addresses into stable, connectable public strings.
///
/// The returned list prefers directly dialable addresses first so existing callers can
/// connect with the first entry. A bare endpoint ID is still included last as a stable
/// identity-only reference.
pub fn format_public_listen_addrs(peer_id: &PeerId, raw_addrs: &[PeerAddr]) -> Vec<String> {
    let mut direct = Vec::new();
    let mut tickets = Vec::new();

    for addr in raw_addrs {
        let raw = addr.as_str().trim();
        if raw.is_empty() || raw == format!("iroh://{}", peer_id) {
            continue;
        }

        let candidate = if is_ticket_string(raw) {
            raw.to_string()
        } else {
            format!("{}/p2p/{}", raw, peer_id)
        };

        let target = if is_ticket_string(raw) {
            &mut tickets
        } else {
            &mut direct
        };

        if !target.contains(&candidate) {
            target.push(candidate);
        }
    }

    let mut formatted = direct;
    for ticket in tickets {
        if !formatted.contains(&ticket) {
            formatted.push(ticket);
        }
    }
    if !formatted.contains(&peer_id.to_string()) {
        formatted.push(peer_id.to_string());
    }

    formatted
}

/// Build an [`EndpointAddr`] from a peer id and a list of dial hints.
pub fn endpoint_addr_from_parts(peer_id: &PeerId, addrs: &[PeerAddr]) -> Result<EndpointAddr> {
    let endpoint_id = parse_endpoint_id(peer_id)?;
    let mut endpoint_addr = EndpointAddr::new(endpoint_id);

    for addr in addrs {
        let raw = addr.as_str().trim();
        if raw.is_empty() {
            continue;
        }

        if let Ok(ticket) = EndpointTicket::from_str(raw) {
            let ticket_addr = ticket.endpoint_addr();
            if ticket_addr.id != endpoint_id {
                return Err(Error::InvalidPeerId(format!(
                    "ticket endpoint id {} does not match peer id {}",
                    ticket_addr.id, peer_id
                )));
            }
            endpoint_addr = endpoint_addr.with_addrs(ticket_addr.addrs.iter().cloned());
            continue;
        }

        if let Ok(socket_addr) = raw.parse::<SocketAddr>() {
            endpoint_addr = endpoint_addr.with_ip_addr(socket_addr);
            continue;
        }

        if let Ok(relay_url) = raw.parse::<RelayUrl>() {
            endpoint_addr = endpoint_addr.with_relay_url(relay_url);
            continue;
        }

        return Err(Error::Dial(format!("unsupported iroh dial hint: {}", raw)));
    }

    Ok(endpoint_addr)
}

fn normalize_endpoint_id(raw: &str) -> String {
    raw.trim()
        .strip_prefix("iroh://")
        .unwrap_or(raw.trim())
        .to_string()
}

#[cfg(test)]
mod tests {
    use iroh::EndpointAddr;
    use iroh::EndpointId;

    use super::*;

    fn endpoint_id() -> EndpointId {
        "ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6"
            .parse()
            .unwrap()
    }

    #[test]
    fn parse_ticket_expands_transport_addrs() {
        let ticket = EndpointTicket::from(
            EndpointAddr::new(endpoint_id())
                .with_relay_url("https://relay.example.com".parse().unwrap())
                .with_ip_addr("127.0.0.1:4242".parse().unwrap()),
        )
        .to_string();

        let (peer_id, addrs) = parse_public_peer_addr(&ticket).unwrap();
        assert_eq!(peer_id.as_str(), endpoint_id().to_string());
        assert!(addrs.iter().any(|addr| addr.as_str() == "127.0.0.1:4242"));
        assert!(addrs
            .iter()
            .any(|addr| addr.as_str() == "https://relay.example.com/"));
    }

    #[test]
    fn parse_legacy_iroh_formats() {
        let endpoint = endpoint_id().to_string();
        let (peer_id, addrs) = parse_public_peer_addr(&format!("iroh://{}", endpoint)).unwrap();
        assert_eq!(peer_id.as_str(), endpoint);
        assert!(addrs.is_empty());

        let (peer_id, addrs) =
            parse_public_peer_addr(&format!("{endpoint}@127.0.0.1:7777")).unwrap();
        assert_eq!(peer_id.as_str(), endpoint);
        assert_eq!(addrs, vec![PeerAddr::new("127.0.0.1:7777".to_string())]);
    }

    #[test]
    fn format_public_addresses_keeps_legacy_id_and_ticket() {
        let peer_id = PeerId::new(endpoint_id().to_string());
        let ticket = EndpointTicket::from(
            EndpointAddr::new(endpoint_id()).with_ip_addr("127.0.0.1:9999".parse().unwrap()),
        )
        .to_string();
        let formatted = format_public_listen_addrs(
            &peer_id,
            &[
                PeerAddr::new(format!("iroh://{}", peer_id)),
                PeerAddr::new(ticket.clone()),
                PeerAddr::new("127.0.0.1:9999".to_string()),
            ],
        );

        assert_eq!(formatted[0], format!("127.0.0.1:9999/p2p/{peer_id}"));
        assert!(formatted.contains(&ticket));
        assert!(formatted.contains(&peer_id.to_string()));
    }
}
