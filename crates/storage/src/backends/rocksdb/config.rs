use std::time::Duration;

use crate::backends::shared::DurabilityMode;

/// Default close timeout in seconds.
pub const DEFAULT_CLOSE_TIMEOUT_SECS: u64 = 5;

/// Default block cache size: 512 MiB.
const DEFAULT_BLOCK_CACHE_SIZE: usize = 512 * 1024 * 1024;

/// Default write buffer size: 64 MiB.
const DEFAULT_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// Default max write buffer number (memtables before stall).
const DEFAULT_MAX_WRITE_BUFFER_NUMBER: i32 = 4;

/// Default max background compactions.
const DEFAULT_MAX_BACKGROUND_COMPACTIONS: i32 = 4;

/// Default max background flushes.
const DEFAULT_MAX_BACKGROUND_FLUSHES: i32 = 2;

/// Default L0 slowdown writes trigger.
const DEFAULT_L0_SLOWDOWN_WRITES_TRIGGER: i32 = 20;

/// Default L0 stop writes trigger.
const DEFAULT_L0_STOP_WRITES_TRIGGER: i32 = 36;

/// Configuration options for RocksDbStore.
#[derive(Debug, Clone)]
pub struct RocksDbStoreOptions {
    block_cache_size: usize,
    write_buffer_size: usize,
    max_write_buffer_number: i32,
    max_background_compactions: i32,
    max_background_flushes: i32,
    l0_slowdown_writes_trigger: i32,
    l0_stop_writes_trigger: i32,
    enable_blob_files: bool,
    min_blob_size: u64,
    close_timeout: Duration,
    durability: DurabilityMode,
}

impl Default for RocksDbStoreOptions {
    fn default() -> Self {
        Self {
            block_cache_size: DEFAULT_BLOCK_CACHE_SIZE,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            max_write_buffer_number: DEFAULT_MAX_WRITE_BUFFER_NUMBER,
            max_background_compactions: DEFAULT_MAX_BACKGROUND_COMPACTIONS,
            max_background_flushes: DEFAULT_MAX_BACKGROUND_FLUSHES,
            l0_slowdown_writes_trigger: DEFAULT_L0_SLOWDOWN_WRITES_TRIGGER,
            l0_stop_writes_trigger: DEFAULT_L0_STOP_WRITES_TRIGGER,
            enable_blob_files: false,
            min_blob_size: 256,
            close_timeout: Duration::from_secs(DEFAULT_CLOSE_TIMEOUT_SECS),
            durability: DurabilityMode::Eventual,
        }
    }
}

impl RocksDbStoreOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_block_cache_size(mut self, bytes: usize) -> Self {
        self.block_cache_size = bytes;
        self
    }

    pub fn block_cache_size(&self) -> usize {
        self.block_cache_size
    }

    pub fn with_write_buffer_size(mut self, bytes: usize) -> Self {
        self.write_buffer_size = bytes;
        self
    }

    pub fn write_buffer_size(&self) -> usize {
        self.write_buffer_size
    }

    pub fn with_max_write_buffer_number(mut self, n: i32) -> Self {
        self.max_write_buffer_number = n;
        self
    }

    pub fn max_write_buffer_number(&self) -> i32 {
        self.max_write_buffer_number
    }

    pub fn with_max_background_compactions(mut self, n: i32) -> Self {
        self.max_background_compactions = n;
        self
    }

    pub fn max_background_compactions(&self) -> i32 {
        self.max_background_compactions
    }

    pub fn with_max_background_flushes(mut self, n: i32) -> Self {
        self.max_background_flushes = n;
        self
    }

    pub fn max_background_flushes(&self) -> i32 {
        self.max_background_flushes
    }

    pub fn with_l0_slowdown_writes_trigger(mut self, n: i32) -> Self {
        self.l0_slowdown_writes_trigger = n;
        self
    }

    pub fn l0_slowdown_writes_trigger(&self) -> i32 {
        self.l0_slowdown_writes_trigger
    }

    pub fn with_l0_stop_writes_trigger(mut self, n: i32) -> Self {
        self.l0_stop_writes_trigger = n;
        self
    }

    pub fn l0_stop_writes_trigger(&self) -> i32 {
        self.l0_stop_writes_trigger
    }

    pub fn with_enable_blob_files(mut self, enabled: bool) -> Self {
        self.enable_blob_files = enabled;
        self
    }

    pub fn enable_blob_files(&self) -> bool {
        self.enable_blob_files
    }

    pub fn with_min_blob_size(mut self, bytes: u64) -> Self {
        self.min_blob_size = bytes;
        self
    }

    pub fn min_blob_size(&self) -> u64 {
        self.min_blob_size
    }

    pub fn with_close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    pub fn close_timeout(&self) -> Duration {
        self.close_timeout
    }

    pub fn with_durability(mut self, mode: DurabilityMode) -> Self {
        self.durability = mode;
        self
    }

    pub fn durability(&self) -> DurabilityMode {
        self.durability
    }
}
