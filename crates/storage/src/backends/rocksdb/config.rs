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

/// Default target file size base: 64 MiB.
const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = 64 * 1024 * 1024;

/// Default max bytes for level base: 256 MiB.
const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 256 * 1024 * 1024;

/// Default block size: 16 KiB (reduces index size vs 4KB default).
const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;

/// Compression type for RocksDB SST files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Snappy,
    Zstd,
    Lz4,
}

/// Compaction style for RocksDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStyle {
    Level,
    Universal,
}

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
    target_file_size_base: u64,
    max_bytes_for_level_base: u64,
    block_size: usize,
    compression: CompressionType,
    compaction_style: CompactionStyle,
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
            target_file_size_base: DEFAULT_TARGET_FILE_SIZE_BASE,
            max_bytes_for_level_base: DEFAULT_MAX_BYTES_FOR_LEVEL_BASE,
            block_size: DEFAULT_BLOCK_SIZE,
            compression: CompressionType::Lz4,
            compaction_style: CompactionStyle::Level,
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

    pub fn with_target_file_size_base(mut self, bytes: u64) -> Self {
        self.target_file_size_base = bytes;
        self
    }

    pub fn target_file_size_base(&self) -> u64 {
        self.target_file_size_base
    }

    pub fn with_max_bytes_for_level_base(mut self, bytes: u64) -> Self {
        self.max_bytes_for_level_base = bytes;
        self
    }

    pub fn max_bytes_for_level_base(&self) -> u64 {
        self.max_bytes_for_level_base
    }

    pub fn with_block_size(mut self, bytes: usize) -> Self {
        self.block_size = bytes;
        self
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    pub fn compression(&self) -> CompressionType {
        self.compression
    }

    pub fn with_compaction_style(mut self, style: CompactionStyle) -> Self {
        self.compaction_style = style;
        self
    }

    pub fn compaction_style(&self) -> CompactionStyle {
        self.compaction_style
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

    /// Build options from environment variables, falling back to defaults.
    ///
    /// | Env Var | Field | Default |
    /// |---------|-------|---------|
    /// | `ROCKS_BLOCK_CACHE_MB` | block_cache_size | 512 |
    /// | `ROCKS_WRITE_BUFFER_MB` | write_buffer_size | 64 |
    /// | `ROCKS_MAX_WRITE_BUFFERS` | max_write_buffer_number | 4 |
    /// | `ROCKS_COMPACTIONS` | max_background_compactions | 4 |
    /// | `ROCKS_FLUSHES` | max_background_flushes | 2 |
    /// | `ROCKS_L0_SLOWDOWN` | l0_slowdown_writes_trigger | 20 |
    /// | `ROCKS_L0_STOP` | l0_stop_writes_trigger | 36 |
    /// | `ROCKS_TARGET_FILE_MB` | target_file_size_base | 64 |
    /// | `ROCKS_LEVEL_BASE_MB` | max_bytes_for_level_base | 256 |
    /// | `ROCKS_BLOCK_SIZE_KB` | block_size | 4 |
    /// | `ROCKS_COMPRESSION` | compression | snappy |
    /// | `ROCKS_COMPACTION_STYLE` | compaction_style | level |
    /// | `ROCKS_BLOB_FILES` | enable_blob_files | false |
    /// | `ROCKS_MIN_BLOB_SIZE` | min_blob_size | 256 |
    pub fn from_env() -> Self {
        let mut opts = Self::default();

        if let Some(v) = env_usize("ROCKS_BLOCK_CACHE_MB") {
            opts.block_cache_size = v * 1024 * 1024;
        }
        if let Some(v) = env_usize("ROCKS_WRITE_BUFFER_MB") {
            opts.write_buffer_size = v * 1024 * 1024;
        }
        if let Some(v) = env_i32("ROCKS_MAX_WRITE_BUFFERS") {
            opts.max_write_buffer_number = v;
        }
        if let Some(v) = env_i32("ROCKS_COMPACTIONS") {
            opts.max_background_compactions = v;
        }
        if let Some(v) = env_i32("ROCKS_FLUSHES") {
            opts.max_background_flushes = v;
        }
        if let Some(v) = env_i32("ROCKS_L0_SLOWDOWN") {
            opts.l0_slowdown_writes_trigger = v;
        }
        if let Some(v) = env_i32("ROCKS_L0_STOP") {
            opts.l0_stop_writes_trigger = v;
        }
        if let Some(v) = env_u64("ROCKS_TARGET_FILE_MB") {
            opts.target_file_size_base = v * 1024 * 1024;
        }
        if let Some(v) = env_u64("ROCKS_LEVEL_BASE_MB") {
            opts.max_bytes_for_level_base = v * 1024 * 1024;
        }
        if let Some(v) = env_usize("ROCKS_BLOCK_SIZE_KB") {
            opts.block_size = v * 1024;
        }
        if let Ok(v) = std::env::var("ROCKS_COMPRESSION") {
            opts.compression = match v.to_lowercase().as_str() {
                "none" => CompressionType::None,
                "snappy" => CompressionType::Snappy,
                "zstd" => CompressionType::Zstd,
                "lz4" => CompressionType::Lz4,
                _ => CompressionType::Snappy,
            };
        }
        if let Ok(v) = std::env::var("ROCKS_COMPACTION_STYLE") {
            opts.compaction_style = match v.to_lowercase().as_str() {
                "level" => CompactionStyle::Level,
                "universal" => CompactionStyle::Universal,
                _ => CompactionStyle::Level,
            };
        }
        if let Ok(v) = std::env::var("ROCKS_BLOB_FILES") {
            opts.enable_blob_files = v == "1" || v.to_lowercase() == "true";
        }
        if let Some(v) = env_u64("ROCKS_MIN_BLOB_SIZE") {
            opts.min_blob_size = v;
        }

        opts
    }
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok()?.parse().ok()
}

fn env_i32(key: &str) -> Option<i32> {
    std::env::var(key).ok()?.parse().ok()
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse().ok()
}
