//! Public configuration types for Defra's iroh transport.

/// Relay configuration for an iroh endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum IrohRelayModeConfig {
    /// Use iroh's default relay behavior.
    #[default]
    Default,
    /// Disable relay-assisted connectivity entirely.
    Disabled,
    /// Use a custom set of relay URLs.
    Custom(Vec<String>),
}

/// Address lookup / discovery configuration for an iroh endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum IrohDiscoveryConfig {
    /// Use iroh's default Number 0 discovery stack (pkarr publisher + DNS lookup).
    #[default]
    N0,
    /// Disable address lookup and publishing.
    Disabled,
    /// Use a custom DNS origin and pkarr relay for publishing / lookup.
    CustomDns {
        origin_domain: String,
        pkarr_relay_url: String,
    },
}
