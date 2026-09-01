use std::ffi::c_char;
#[cfg(feature = "iroh")]
use std::path::PathBuf;

use crate::ffi_entry;
use crate::node::build_node_state;
use crate::state::NODES;
use crate::types::{c_str_to_string, NewNodeResult, NodeInitOptions};

/// Create a new DefraDB node with P2P enabled.
///
/// # Safety
///
/// Any non-null C string pointers inside `options` and `listen_addr` must be valid
/// null-terminated UTF-8 strings for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn new_node_with_p2p(
    options: NodeInitOptions,
    listen_addr: *const c_char,
) -> NewNodeResult {
    ffi_entry! {
        let rt = match crate::runtime::RUNTIME.get() {
            Some(rt) => rt,
            None => return NewNodeResult::error("runtime not initialized - call defra_init() first"),
        };

        let listen_addr_str = c_str_to_string(listen_addr).unwrap_or_default();
        let transport = match resolve_transport(&options, &listen_addr_str) {
            Ok(transport) => transport,
            Err(error) => return NewNodeResult::error(error),
        };

        let result = rt
            .block_on(async { build_node_state(options, transport).await })
            .map(|state| NODES.insert(state));

        match result {
            Ok(handle) => NewNodeResult::success(handle),
            Err(error) => NewNodeResult::error(error),
        }
    }
}

fn resolve_transport(
    options: &NodeInitOptions,
    _listen_addr: &str,
) -> Result<embedded::TransportConfig, String> {
    let transport = unsafe { c_str_to_string(options.p2p_transport) }
        .unwrap_or_else(|| "libp2p".to_string())
        .to_lowercase();

    match transport.as_str() {
        "" | "libp2p" => {
            #[cfg(feature = "libp2p")]
            {
                if _listen_addr.is_empty() {
                    return Err("listen_addr is required for libp2p transport".to_string());
                }
                Ok(embedded::TransportConfig::Libp2p(embedded::Libp2pConfig {
                    listen_addr: _listen_addr.to_string(),
                }))
            }
            #[cfg(not(feature = "libp2p"))]
            {
                Err(
                    "this build does not include the libp2p transport; rebuild with the libp2p feature"
                        .to_string(),
                )
            }
        }
        "iroh" => {
            #[cfg(feature = "iroh")]
            {
                let mut config = embedded::IrohConfig {
                    secret_key_path: unsafe { c_str_to_string(options.iroh_key_path) }
                        .map(PathBuf::from),
                    ..Default::default()
                };

                let relay_mode = unsafe { c_str_to_string(options.iroh_relay_mode) };
                let relay_url = unsafe { c_str_to_string(options.iroh_relay_url) };
                let relay_urls_json = unsafe { c_str_to_string(options.iroh_relay_urls_json) };
                let relay_urls = match relay_urls_json {
                    Some(raw) if !raw.trim().is_empty() => {
                        serde_json::from_str::<Vec<String>>(&raw)
                            .map_err(|error| format!("invalid iroh_relay_urls_json: {}", error))?
                    }
                    _ => Vec::new(),
                };

                config.relay_mode = match relay_mode.as_deref() {
                    Some("disabled") => p2p::iroh::IrohRelayModeConfig::Disabled,
                    Some("default") => p2p::iroh::IrohRelayModeConfig::Default,
                    Some("custom") => {
                        let mut urls = relay_urls;
                        if let Some(url) = relay_url {
                            urls.push(url);
                        }
                        if urls.is_empty() {
                            return Err("iroh_relay_mode=custom requires at least one relay URL"
                                .to_string());
                        }
                        p2p::iroh::IrohRelayModeConfig::Custom(urls)
                    }
                    Some(other) => {
                        return Err(format!(
                            "unsupported iroh_relay_mode '{}'. Supported: default, disabled, custom",
                            other
                        ));
                    }
                    None => {
                        let mut urls = relay_urls;
                        if let Some(url) = relay_url {
                            urls.push(url);
                        }
                        if urls.is_empty() {
                            p2p::iroh::IrohRelayModeConfig::Default
                        } else {
                            p2p::iroh::IrohRelayModeConfig::Custom(urls)
                        }
                    }
                };

                let discovery_origin =
                    unsafe { c_str_to_string(options.iroh_discovery_origin_domain) };
                let pkarr_relay_url = unsafe { c_str_to_string(options.iroh_pkarr_relay_url) };
                config.discovery = match (
                    options.iroh_discovery != 0,
                    discovery_origin,
                    pkarr_relay_url,
                ) {
                    (_, Some(origin_domain), Some(pkarr_relay_url)) => {
                        p2p::iroh::IrohDiscoveryConfig::CustomDns {
                            origin_domain,
                            pkarr_relay_url,
                        }
                    }
                    (_, Some(_), None) | (_, None, Some(_)) => {
                        return Err(
                            "custom iroh discovery requires both iroh_discovery_origin_domain and iroh_pkarr_relay_url"
                                .to_string(),
                        );
                    }
                    (false, None, None) => p2p::iroh::IrohDiscoveryConfig::Disabled,
                    (true, None, None) => p2p::iroh::IrohDiscoveryConfig::N0,
                };

                if !options.iroh_bind_addr.is_null() {
                    let bind_addr = unsafe { c_str_to_string(options.iroh_bind_addr) }
                        .ok_or_else(|| "iroh_bind_addr is not valid UTF-8".to_string())?;
                    config.bind_addr = Some(bind_addr.parse().map_err(|error| {
                        format!("invalid iroh_bind_addr '{}': {}", bind_addr, error)
                    })?);
                }
                if options.iroh_bind_port != 0 {
                    config.bind_port = Some(options.iroh_bind_port);
                }

                if !_listen_addr.trim().is_empty() {
                    let socket_addr: std::net::SocketAddr =
                        _listen_addr.parse().map_err(|error| {
                            format!("invalid iroh listen address '{}': {}", _listen_addr, error)
                        })?;
                    config.bind_addr = Some(socket_addr.ip());
                    config.bind_port = Some(socket_addr.port());
                }

                Ok(embedded::TransportConfig::Iroh(config))
            }
            #[cfg(not(feature = "iroh"))]
            {
                Err("iroh transport not enabled. Rebuild with --features iroh".to_string())
            }
        }
        other => Err(format!(
            "unsupported p2p transport '{}'. Supported: libp2p, iroh",
            other
        )),
    }
}
