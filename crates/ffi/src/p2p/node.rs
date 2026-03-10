use std::ffi::c_char;
#[cfg(feature = "iroh")]
use std::path::PathBuf;

use crate::ffi_entry;
use crate::node::build_node_state;
use crate::state::NODES;
use crate::types::{c_str_to_string, NewNodeResult, NodeInitOptions};

/// Create a new DefraDB node with P2P enabled.
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
    listen_addr: &str,
) -> Result<embedded::TransportConfig, String> {
    let transport = unsafe { c_str_to_string(options.p2p_transport) }
        .unwrap_or_else(|| "libp2p".to_string())
        .to_lowercase();

    match transport.as_str() {
        "" | "libp2p" => {
            if listen_addr.is_empty() {
                return Err("listen_addr is required for libp2p transport".to_string());
            }
            Ok(embedded::TransportConfig::Libp2p(embedded::Libp2pConfig {
                listen_addr: listen_addr.to_string(),
            }))
        }
        "iroh" => {
            #[cfg(feature = "iroh")]
            {
                let mut config = embedded::IrohConfig {
                    relay_url: unsafe { c_str_to_string(options.iroh_relay_url) },
                    discovery: options.iroh_discovery != 0,
                    secret_key_path: unsafe { c_str_to_string(options.iroh_key_path) }
                        .map(PathBuf::from),
                    ..Default::default()
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

                if !listen_addr.trim().is_empty() {
                    let socket_addr: std::net::SocketAddr =
                        listen_addr.parse().map_err(|error| {
                            format!("invalid iroh listen address '{}': {}", listen_addr, error)
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
