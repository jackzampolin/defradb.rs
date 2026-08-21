//! Configuration helpers for the iroh endpoint.

use iroh::endpoint::BindOpts;
use iroh::SecretKey;
use std::sync::Arc;

use super::config::{IrohDiscoveryConfig, IrohRelayModeConfig};
use super::gossip_heal::GossipHealConfig;

// iroh 1.0 silently ignores custom values below its internal default plus one.
const MIN_CONCURRENT_MULTIPATH_PATHS: u32 = 9;

/// Configuration for creating an `IrohEndpoint`.
pub struct IrohEndpointConfig {
    pub secret_key: SecretKey,
    /// Optional Defra identity used for the Go-compatible peer identity
    /// challenge. The QUIC endpoint authenticates the requester; the returned
    /// token binds this DID to that requester's endpoint ID.
    pub node_identity: Option<Arc<identity::RawIdentity>>,
    /// Relay behavior for this endpoint.
    pub relay_mode: IrohRelayModeConfig,
    /// Address publishing / lookup behavior for this endpoint.
    pub discovery: IrohDiscoveryConfig,
    /// UDP port for the QUIC listener. `None` = ephemeral (OS-assigned).
    pub bind_port: Option<u16>,
    /// Bind to a specific IP address. When set, IROH only listens on this
    /// interface — prevents advertising unreachable LAN addresses to peers
    /// on different networks. None = 0.0.0.0 (all interfaces).
    pub bind_addr: Option<std::net::IpAddr>,
    /// Maximum QUIC paths that may be open concurrently for one connection.
    /// `None` keeps iroh's default; custom values must be at least 9.
    pub max_concurrent_multipath_paths: Option<u32>,
    /// Gossip send-path healing (#1092).
    pub gossip_heal: GossipHealConfig,
}

impl Default for IrohEndpointConfig {
    fn default() -> Self {
        Self {
            secret_key: SecretKey::generate(),
            node_identity: None,
            relay_mode: IrohRelayModeConfig::default(),
            discovery: IrohDiscoveryConfig::default(),
            bind_port: None,
            bind_addr: None,
            max_concurrent_multipath_paths: None,
            gossip_heal: GossipHealConfig::default(),
        }
    }
}

pub(super) fn apply_multipath_config(
    builder: iroh::endpoint::Builder,
    max_concurrent: Option<u32>,
) -> crate::error::Result<iroh::endpoint::Builder> {
    let Some(max_concurrent) = max_concurrent else {
        return Ok(builder);
    };

    if max_concurrent < MIN_CONCURRENT_MULTIPATH_PATHS {
        return Err(crate::error::Error::Transport(format!(
            "iroh max concurrent multipath paths must be at least {}, got {}",
            MIN_CONCURRENT_MULTIPATH_PATHS, max_concurrent
        )));
    }

    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .max_concurrent_multipath_paths(max_concurrent)
        .build();
    tracing::info!(max_concurrent, "configured iroh multipath path limit");
    Ok(builder.transport_config(transport_config))
}

pub(super) fn relay_mode_from_config(
    config: &IrohRelayModeConfig,
) -> crate::error::Result<iroh::RelayMode> {
    match config {
        IrohRelayModeConfig::Default => Ok(iroh::endpoint::default_relay_mode()),
        IrohRelayModeConfig::Disabled => Ok(iroh::RelayMode::Disabled),
        IrohRelayModeConfig::Custom(urls) => {
            let relay_map = iroh::RelayMap::try_from_iter(urls.iter().map(String::as_str))
                .map_err(|e| {
                    crate::error::Error::Transport(format!("invalid relay URL list: {}", e))
                })?;
            Ok(iroh::RelayMode::Custom(relay_map))
        }
    }
}

pub(super) fn apply_discovery_config(
    mut builder: iroh::endpoint::Builder,
    config: &IrohDiscoveryConfig,
) -> crate::error::Result<iroh::endpoint::Builder> {
    use iroh::address_lookup::{DnsAddressLookup, PkarrPublisher};

    builder = match config {
        IrohDiscoveryConfig::N0 => builder
            .address_lookup(PkarrPublisher::n0_dns())
            .address_lookup(DnsAddressLookup::n0_dns()),
        IrohDiscoveryConfig::Disabled => builder.clear_address_lookup(),
        IrohDiscoveryConfig::CustomDns {
            origin_domain,
            pkarr_relay_url,
        } => {
            let pkarr_relay = pkarr_relay_url.parse().map_err(|e| {
                crate::error::Error::Transport(format!(
                    "invalid pkarr relay URL '{}': {}",
                    pkarr_relay_url, e
                ))
            })?;
            builder
                .address_lookup(PkarrPublisher::builder(pkarr_relay))
                .address_lookup(DnsAddressLookup::builder(origin_domain.clone()))
        }
    };

    Ok(builder)
}

pub(super) fn apply_bind_config(
    mut builder: iroh::endpoint::Builder,
    bind_addr: Option<std::net::IpAddr>,
    bind_port: Option<u16>,
) -> crate::error::Result<iroh::endpoint::Builder> {
    let bind_error =
        |error| crate::error::Error::Transport(format!("invalid bind addr: {}", error));

    match (bind_addr, bind_port) {
        (Some(ip), port) => {
            builder = builder
                .bind_addr(std::net::SocketAddr::new(ip, port.unwrap_or(0)))
                .map_err(bind_error)?;
        }
        (None, Some(port)) => {
            builder = builder.clear_ip_transports();
            builder = builder
                .bind_addr(std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    port,
                ))
                .map_err(bind_error)?;
            builder = builder
                .bind_addr_with_opts(
                    std::net::SocketAddr::new(
                        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                        port,
                    ),
                    BindOpts::default().set_is_required(false),
                )
                .map_err(bind_error)?;
        }
        (None, None) => {}
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multipath_limit_rejects_values_iroh_would_ignore() {
        let builder = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal);
        let result =
            apply_multipath_config(builder, Some(super::MIN_CONCURRENT_MULTIPATH_PATHS - 1));

        assert!(matches!(
            result,
            Err(crate::error::Error::Transport(message))
                if message.contains("must be at least 9")
        ));
    }

    #[test]
    fn multipath_limit_accepts_iroh_minimum() {
        let builder = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal);

        assert!(
            apply_multipath_config(builder, Some(super::MIN_CONCURRENT_MULTIPATH_PATHS),).is_ok()
        );
    }
}
