//! DAG sync configuration.

use std::num::NonZeroUsize;
use std::time::Duration;

use crate::error::Result;

/// Configuration for DAG sync operations.
#[derive(Debug, Clone)]
pub struct DagSyncConfig {
    /// Timeout for fetching a single block via Bitswap.
    block_fetch_timeout: Duration,

    /// Maximum depth to recursively sync (None = unlimited).
    max_depth: Option<NonZeroUsize>,

    /// Maximum concurrent block fetches (guaranteed non-zero).
    max_concurrent_fetches: NonZeroUsize,
}

impl DagSyncConfig {
    /// Create a new DagSyncConfig with validation.
    ///
    /// # Arguments
    ///
    /// * `block_fetch_timeout` - Timeout for fetching blocks (must be > 0)
    /// * `max_depth` - Maximum sync depth (None = unlimited)
    /// * `max_concurrent_fetches` - Max concurrent fetches (guaranteed non-zero)
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidConfig` if `block_fetch_timeout` is zero.
    pub fn new(
        block_fetch_timeout: Duration,
        max_depth: Option<NonZeroUsize>,
        max_concurrent_fetches: NonZeroUsize,
    ) -> Result<Self> {
        if block_fetch_timeout.is_zero() {
            return Err(crate::error::Error::InvalidConfig(
                "block_fetch_timeout must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            block_fetch_timeout,
            max_depth,
            max_concurrent_fetches,
        })
    }

    /// Get the block fetch timeout.
    pub fn block_fetch_timeout(&self) -> Duration {
        self.block_fetch_timeout
    }

    /// Get the maximum sync depth (None = unlimited).
    pub fn max_depth(&self) -> Option<NonZeroUsize> {
        self.max_depth
    }

    /// Get the maximum concurrent fetches.
    pub fn max_concurrent_fetches(&self) -> NonZeroUsize {
        self.max_concurrent_fetches
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

    /// Builder method to set max depth.
    pub fn with_max_depth(mut self, depth: Option<NonZeroUsize>) -> Self {
        self.max_depth = depth;
        self
    }

    /// Builder method to set max concurrent fetches.
    pub fn with_max_concurrent_fetches(mut self, count: NonZeroUsize) -> Self {
        self.max_concurrent_fetches = count;
        self
    }
}

impl Default for DagSyncConfig {
    fn default() -> Self {
        Self {
            block_fetch_timeout: Duration::from_secs(30),
            max_depth: None, // Unlimited
            // SAFETY: 16 is non-zero
            max_concurrent_fetches: NonZeroUsize::new(16).unwrap(),
        }
    }
}
