use std::time::Duration;

use crate::backends::shared::DurabilityMode;

const DEFAULT_CLOSE_TIMEOUT_SECS: u64 = 5;
const DEFAULT_BLOCK_CACHE_SIZE: usize = 512 * 1024 * 1024;
const DEFAULT_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;
const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;
const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;
const DEFAULT_LEVEL_BASE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_LEVEL_SIZE_MULTIPLIER: u64 = 10;
const DEFAULT_TARGET_FILE_SIZE: u64 = 64 * 1024 * 1024;

/// Configuration for the Lark storage backend.
#[derive(Debug, Clone)]
pub struct LarkStoreOptions {
    block_cache_size: usize,
    write_buffer_size: usize,
    block_size: usize,
    bloom_bits_per_key: usize,
    compression: bool,
    l0_compaction_trigger: usize,
    level_base_bytes: u64,
    level_size_multiplier: u64,
    target_file_size: u64,
    close_timeout: Duration,
    durability: DurabilityMode,
}

impl Default for LarkStoreOptions {
    fn default() -> Self {
        Self {
            block_cache_size: DEFAULT_BLOCK_CACHE_SIZE,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            block_size: DEFAULT_BLOCK_SIZE,
            bloom_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
            compression: true,
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            level_base_bytes: DEFAULT_LEVEL_BASE_BYTES,
            level_size_multiplier: DEFAULT_LEVEL_SIZE_MULTIPLIER,
            target_file_size: DEFAULT_TARGET_FILE_SIZE,
            close_timeout: Duration::from_secs(DEFAULT_CLOSE_TIMEOUT_SECS),
            durability: DurabilityMode::Eventual,
        }
    }
}

impl LarkStoreOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_block_cache_size(mut self, bytes: usize) -> Self {
        self.block_cache_size = bytes;
        self
    }

    pub fn with_write_buffer_size(mut self, bytes: usize) -> Self {
        self.write_buffer_size = bytes;
        self
    }

    pub fn with_block_size(mut self, bytes: usize) -> Self {
        self.block_size = bytes;
        self
    }

    pub fn with_bloom_bits_per_key(mut self, bits: usize) -> Self {
        self.bloom_bits_per_key = bits;
        self
    }

    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compression = enabled;
        self
    }

    pub fn with_l0_compaction_trigger(mut self, n: usize) -> Self {
        self.l0_compaction_trigger = n;
        self
    }

    pub fn with_level_base_bytes(mut self, bytes: u64) -> Self {
        self.level_base_bytes = bytes;
        self
    }

    pub fn with_level_size_multiplier(mut self, m: u64) -> Self {
        self.level_size_multiplier = m;
        self
    }

    pub fn with_target_file_size(mut self, bytes: u64) -> Self {
        self.target_file_size = bytes;
        self
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

    /// Build options from `LARK_*` environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let mut opts = Self::default();

        if let Some(v) = env_usize("LARK_BLOCK_CACHE_MB") {
            opts.block_cache_size = v * 1024 * 1024;
        }
        if let Some(v) = env_usize("LARK_WRITE_BUFFER_MB") {
            opts.write_buffer_size = v * 1024 * 1024;
        }
        if let Some(v) = env_usize("LARK_BLOCK_SIZE_KB") {
            opts.block_size = v * 1024;
        }
        if let Some(v) = env_usize("LARK_BLOOM_BITS") {
            opts.bloom_bits_per_key = v;
        }
        if let Ok(v) = std::env::var("LARK_COMPRESSION") {
            opts.compression = v != "0" && v.to_lowercase() != "false";
        }
        if let Some(v) = env_usize("LARK_L0_TRIGGER") {
            opts.l0_compaction_trigger = v;
        }
        if let Some(v) = env_u64("LARK_LEVEL_BASE_MB") {
            opts.level_base_bytes = v * 1024 * 1024;
        }
        if let Some(v) = env_u64("LARK_LEVEL_MULTIPLIER") {
            opts.level_size_multiplier = v;
        }
        if let Some(v) = env_u64("LARK_TARGET_FILE_MB") {
            opts.target_file_size = v * 1024 * 1024;
        }

        opts
    }

    /// Convert to `lark_kv::Options`.
    pub(crate) fn to_lark_options(&self) -> lark_kv::Options {
        lark_kv::Options {
            write_buffer_size: self.write_buffer_size,
            block_size: self.block_size,
            block_cache_size: self.block_cache_size,
            bloom_bits_per_key: self.bloom_bits_per_key,
            compression: if self.compression {
                lark_kv::CompressionType::Lz4
            } else {
                lark_kv::CompressionType::None
            },
            l0_compaction_trigger: self.l0_compaction_trigger,
            level_base_bytes: self.level_base_bytes,
            level_size_multiplier: self.level_size_multiplier,
            target_file_size: self.target_file_size,
            durability: match self.durability {
                DurabilityMode::Immediate => lark_kv::DurabilityMode::Immediate,
                DurabilityMode::Eventual => lark_kv::DurabilityMode::Eventual,
            },
            ..Default::default()
        }
    }
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok()?.parse().ok()
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse().ok()
}
