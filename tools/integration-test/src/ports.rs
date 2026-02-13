use std::net::TcpListener;

use anyhow::{Context, Result};

/// Ports assigned to a single node.
pub struct NodePorts {
    pub http: u16,
    pub p2p: u16,
}

/// Allocate `n` unique ephemeral ports using bind-hold-release.
///
/// Binds all ports simultaneously before releasing any, preventing
/// two calls from getting the same port.
pub fn allocate_ports(n: usize) -> Result<Vec<u16>> {
    let listeners: Vec<TcpListener> = (0..n)
        .map(|i| {
            TcpListener::bind("127.0.0.1:0")
                .with_context(|| format!("failed to bind ephemeral port {}/{}", i + 1, n))
        })
        .collect::<Result<_>>()?;

    let ports = listeners
        .iter()
        .map(|l| l.local_addr().map(|a| a.port()))
        .collect::<std::io::Result<Vec<u16>>>()
        .context("failed to get local address")?;

    // All listeners drop here, releasing ports simultaneously
    Ok(ports)
}

/// Allocate port pairs (http, p2p) for `n` nodes.
pub fn allocate_node_ports(n: usize) -> Result<Vec<NodePorts>> {
    let ports = allocate_ports(n * 2)?;
    Ok(ports
        .chunks(2)
        .map(|pair| NodePorts {
            http: pair[0],
            p2p: pair[1],
        })
        .collect())
}

/// Ports assigned to a Source Hub node.
pub struct SourceHubPorts {
    pub lcd: u16,
    pub comet_rpc: u16,
    pub grpc: u16,
    pub p2p: u16,
}

/// Allocate ports for a single Source Hub instance.
pub fn allocate_source_hub_ports() -> Result<SourceHubPorts> {
    let ports = allocate_ports(4)?;
    Ok(SourceHubPorts {
        lcd: ports[0],
        comet_rpc: ports[1],
        grpc: ports[2],
        p2p: ports[3],
    })
}
