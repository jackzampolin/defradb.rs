//! DAG sync configuration.

use std::time::Duration;

use crate::error::Result;

/// Configuration for DAG sync operations.
#[derive(Debug, Clone)]
pub struct DagSyncConfig {
    /// Timeout for fetching a single block via Bitswap.
    block_fetch_timeout: Duration,
}

impl DagSyncConfig {
    /// Create a new DagSyncConfig with validation.
    ///
    /// # Arguments
    ///
    /// * `block_fetch_timeout` - Timeout for fetching blocks (must be > 0)
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidConfig` if `block_fetch_timeout` is zero.
    pub fn new(block_fetch_timeout: Duration) -> Result<Self> {
        if block_fetch_timeout.is_zero() {
            return Err(crate::error::Error::InvalidConfig(
                "block_fetch_timeout must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            block_fetch_timeout,
        })
    }

    /// Get the block fetch timeout.
    pub fn block_fetch_timeout(&self) -> Duration {
        self.block_fetch_timeout
    }

    /// Builder method to set block fetch timeout.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidConfig` if `timeout` is zero.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(crate::error::Error::InvalidConfig(
                "timeout must be greater than zero".to_string(),
            ));
        }
        self.block_fetch_timeout = timeout;
        Ok(self)
    }
}

impl Default for DagSyncConfig {
    fn default() -> Self {
        Self {
            block_fetch_timeout: Duration::from_secs(30),
        }
    }
}
