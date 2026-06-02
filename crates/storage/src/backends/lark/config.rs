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
const DEFAULT_BLOCK_CACHE_NUM_SHARD_BITS: u32 = 6;
const DEFAULT_STRICT_CAPACITY_LIMIT: bool = false;
const DEFAULT_LEVEL0_SLOWDOWN_WRITES_TRIGGER: usize = 20;
const DEFAULT_LEVEL0_STOP_WRITES_TRIGGER: usize = 36;
const DEFAULT_SOFT_PENDING_COMPACTION_BYTES_LIMIT: u64 = 64 * 1024 * 1024 * 1024;
const DEFAULT_HARD_PENDING_COMPACTION_BYTES_LIMIT: u64 = 256 * 1024 * 1024 * 1024;
const DEFAULT_MAX_WRITE_BUFFER_NUMBER: usize = 2;
const DEFAULT_MAX_BACKGROUND_COMPACTIONS: usize = 1;
const DEFAULT_EVICT_COMPACTION_DATA_FROM_PAGE_CACHE: bool = false;
const DEFAULT_PARTITIONED_INDEX: bool = false;
const DEFAULT_METADATA_BLOCK_SIZE: usize = 4 * 1024;
const DEFAULT_FIFO_MAX_TABLE_FILES_SIZE: u64 = 1024 * 1024 * 1024;
const DEFAULT_UNIVERSAL_SIZE_RATIO: u32 = 1;
const DEFAULT_UNIVERSAL_MIN_MERGE_WIDTH: u32 = 2;
const DEFAULT_UNIVERSAL_MAX_MERGE_WIDTH: u32 = u32::MAX;
const DEFAULT_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT: u32 = 200;

/// Compression codec for Lark SSTable data blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompressionType {
    /// Disable SSTable data-block compression.
    None,
    /// Use Snappy compression.
    Snappy,
    /// Use LZ4 compression.
    Lz4,
}

impl From<CompressionType> for lark_kv::CompressionType {
    fn from(value: CompressionType) -> Self {
        match value {
            CompressionType::None => lark_kv::CompressionType::None,
            CompressionType::Snappy => lark_kv::CompressionType::Snappy,
            CompressionType::Lz4 => lark_kv::CompressionType::Lz4,
        }
    }
}

/// Compaction strategy for the Lark backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompactionStyle {
    /// Standard leveled compaction.
    Level,
    /// FIFO compaction for retention-window style workloads.
    Fifo,
    /// Universal compaction for write-heavy workloads.
    Universal,
}

impl From<CompactionStyle> for lark_kv::CompactionStyle {
    fn from(value: CompactionStyle) -> Self {
        match value {
            CompactionStyle::Level => lark_kv::CompactionStyle::Level,
            CompactionStyle::Fifo => lark_kv::CompactionStyle::Fifo,
            CompactionStyle::Universal => lark_kv::CompactionStyle::Universal,
        }
    }
}

/// Configuration for the Lark storage backend.
#[derive(Debug, Clone)]
pub struct LarkStoreOptions {
    block_cache_size: usize,
    write_buffer_size: usize,
    block_size: usize,
    block_cache_num_shard_bits: u32,
    strict_capacity_limit: bool,
    bloom_bits_per_key: usize,
    compression: CompressionType,
    compression_per_level: Option<Vec<CompressionType>>,
    l0_compaction_trigger: usize,
    level0_slowdown_writes_trigger: usize,
    level0_stop_writes_trigger: usize,
    soft_pending_compaction_bytes_limit: u64,
    hard_pending_compaction_bytes_limit: u64,
    max_write_buffer_number: usize,
    level_base_bytes: u64,
    level_size_multiplier: u64,
    target_file_size: u64,
    max_background_compactions: usize,
    evict_compaction_data_from_page_cache: bool,
    partitioned_index: bool,
    metadata_block_size: usize,
    compaction_style: CompactionStyle,
    fifo_max_table_files_size: u64,
    universal_size_ratio: u32,
    universal_min_merge_width: u32,
    universal_max_merge_width: u32,
    universal_max_size_amplification_percent: u32,
    close_timeout: Duration,
    durability: DurabilityMode,
}

impl Default for LarkStoreOptions {
    fn default() -> Self {
        Self {
            block_cache_size: DEFAULT_BLOCK_CACHE_SIZE,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            block_size: DEFAULT_BLOCK_SIZE,
            block_cache_num_shard_bits: DEFAULT_BLOCK_CACHE_NUM_SHARD_BITS,
            strict_capacity_limit: DEFAULT_STRICT_CAPACITY_LIMIT,
            bloom_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
            compression: CompressionType::Lz4,
            compression_per_level: None,
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            level0_slowdown_writes_trigger: DEFAULT_LEVEL0_SLOWDOWN_WRITES_TRIGGER,
            level0_stop_writes_trigger: DEFAULT_LEVEL0_STOP_WRITES_TRIGGER,
            soft_pending_compaction_bytes_limit: DEFAULT_SOFT_PENDING_COMPACTION_BYTES_LIMIT,
            hard_pending_compaction_bytes_limit: DEFAULT_HARD_PENDING_COMPACTION_BYTES_LIMIT,
            max_write_buffer_number: DEFAULT_MAX_WRITE_BUFFER_NUMBER,
            level_base_bytes: DEFAULT_LEVEL_BASE_BYTES,
            level_size_multiplier: DEFAULT_LEVEL_SIZE_MULTIPLIER,
            target_file_size: DEFAULT_TARGET_FILE_SIZE,
            max_background_compactions: DEFAULT_MAX_BACKGROUND_COMPACTIONS,
            evict_compaction_data_from_page_cache: DEFAULT_EVICT_COMPACTION_DATA_FROM_PAGE_CACHE,
            partitioned_index: DEFAULT_PARTITIONED_INDEX,
            metadata_block_size: DEFAULT_METADATA_BLOCK_SIZE,
            compaction_style: CompactionStyle::Level,
            fifo_max_table_files_size: DEFAULT_FIFO_MAX_TABLE_FILES_SIZE,
            universal_size_ratio: DEFAULT_UNIVERSAL_SIZE_RATIO,
            universal_min_merge_width: DEFAULT_UNIVERSAL_MIN_MERGE_WIDTH,
            universal_max_merge_width: DEFAULT_UNIVERSAL_MAX_MERGE_WIDTH,
            universal_max_size_amplification_percent:
                DEFAULT_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT,
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

    pub fn with_block_cache_num_shard_bits(mut self, bits: u32) -> Self {
        self.block_cache_num_shard_bits = bits;
        self
    }

    pub fn with_strict_capacity_limit(mut self, enabled: bool) -> Self {
        self.strict_capacity_limit = enabled;
        self
    }

    pub fn with_bloom_bits_per_key(mut self, bits: usize) -> Self {
        self.bloom_bits_per_key = bits;
        self
    }

    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compression = if enabled {
            CompressionType::Lz4
        } else {
            CompressionType::None
        };
        self
    }

    pub fn with_compression_type(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_compression_per_level(mut self, compression: Vec<CompressionType>) -> Self {
        self.compression_per_level = Some(compression);
        self
    }

    pub fn with_l0_compaction_trigger(mut self, n: usize) -> Self {
        self.l0_compaction_trigger = n;
        self
    }

    pub fn with_level0_slowdown_writes_trigger(mut self, n: usize) -> Self {
        self.level0_slowdown_writes_trigger = n;
        self
    }

    pub fn with_level0_stop_writes_trigger(mut self, n: usize) -> Self {
        self.level0_stop_writes_trigger = n;
        self
    }

    pub fn with_soft_pending_compaction_bytes_limit(mut self, bytes: u64) -> Self {
        self.soft_pending_compaction_bytes_limit = bytes;
        self
    }

    pub fn with_hard_pending_compaction_bytes_limit(mut self, bytes: u64) -> Self {
        self.hard_pending_compaction_bytes_limit = bytes;
        self
    }

    pub fn with_max_write_buffer_number(mut self, n: usize) -> Self {
        self.max_write_buffer_number = n;
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

    pub fn with_max_background_compactions(mut self, n: usize) -> Self {
        self.max_background_compactions = n;
        self
    }

    pub fn with_evict_compaction_data_from_page_cache(mut self, enabled: bool) -> Self {
        self.evict_compaction_data_from_page_cache = enabled;
        self
    }

    pub fn with_partitioned_index(mut self, enabled: bool) -> Self {
        self.partitioned_index = enabled;
        self
    }

    pub fn with_metadata_block_size(mut self, bytes: usize) -> Self {
        self.metadata_block_size = bytes;
        self
    }

    pub fn with_compaction_style(mut self, style: CompactionStyle) -> Self {
        self.compaction_style = style;
        self
    }

    pub fn with_fifo_max_table_files_size(mut self, bytes: u64) -> Self {
        self.fifo_max_table_files_size = bytes;
        self
    }

    pub fn with_universal_size_ratio(mut self, ratio: u32) -> Self {
        self.universal_size_ratio = ratio;
        self
    }

    pub fn with_universal_min_merge_width(mut self, width: u32) -> Self {
        self.universal_min_merge_width = width;
        self
    }

    pub fn with_universal_max_merge_width(mut self, width: u32) -> Self {
        self.universal_max_merge_width = width;
        self
    }

    pub fn with_universal_max_size_amplification_percent(mut self, percent: u32) -> Self {
        self.universal_max_size_amplification_percent = percent;
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
    ///
    /// | Env Var | Field | Unit / Values |
    /// |---------|-------|---------------|
    /// | `LARK_BLOCK_CACHE_MB` | block cache size | MiB |
    /// | `LARK_WRITE_BUFFER_MB` | write buffer size | MiB |
    /// | `LARK_BLOCK_SIZE_KB` | SSTable block size | KiB |
    /// | `LARK_BLOCK_CACHE_SHARD_BITS` | block cache shard exponent | integer |
    /// | `LARK_STRICT_CAPACITY_LIMIT` | strict block-cache admission | bool |
    /// | `LARK_BLOOM_BITS` | Bloom bits per key | integer |
    /// | `LARK_COMPRESSION` | default compression | `none`, `snappy`, `lz4`, bool |
    /// | `LARK_COMPRESSION_PER_LEVEL` | per-level compression | comma-separated codecs |
    /// | `LARK_L0_TRIGGER` | L0 compaction trigger | file count |
    /// | `LARK_L0_SLOWDOWN_TRIGGER` | L0 write slowdown trigger | file count |
    /// | `LARK_L0_STOP_TRIGGER` | L0 write stop trigger | file count |
    /// | `LARK_SOFT_PENDING_COMPACTION_MB` | soft pending-compaction limit | MiB |
    /// | `LARK_HARD_PENDING_COMPACTION_MB` | hard pending-compaction limit | MiB |
    /// | `LARK_MAX_WRITE_BUFFER_NUMBER` | max write-buffer count | integer |
    /// | `LARK_LEVEL_BASE_MB` | level-1 target size | MiB |
    /// | `LARK_LEVEL_MULTIPLIER` | level size multiplier | integer |
    /// | `LARK_TARGET_FILE_MB` | compaction output target file size | MiB |
    /// | `LARK_MAX_BACKGROUND_COMPACTIONS` | compaction workers | integer |
    /// | `LARK_EVICT_COMPACTION_DATA` | page-cache eviction hint | bool |
    /// | `LARK_PARTITIONED_INDEX` | partitioned SSTable index | bool |
    /// | `LARK_METADATA_BLOCK_SIZE_KB` | partitioned-index block size | KiB |
    /// | `LARK_COMPACTION_STYLE` | compaction strategy | `level`, `fifo`, `universal` |
    /// | `LARK_FIFO_MAX_TABLE_FILES_MB` | FIFO SSTable retention cap | MiB |
    /// | `LARK_UNIVERSAL_SIZE_RATIO` | universal size ratio | integer |
    /// | `LARK_UNIVERSAL_MIN_MERGE_WIDTH` | universal min merge width | integer |
    /// | `LARK_UNIVERSAL_MAX_MERGE_WIDTH` | universal max merge width | integer |
    /// | `LARK_UNIVERSAL_MAX_SIZE_AMP_PERCENT` | universal size amplification cap | percent |
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
        if let Some(v) = env_u32("LARK_BLOCK_CACHE_SHARD_BITS") {
            opts.block_cache_num_shard_bits = v;
        }
        if let Some(v) = env_bool("LARK_STRICT_CAPACITY_LIMIT") {
            opts.strict_capacity_limit = v;
        }
        if let Some(v) = env_usize("LARK_BLOOM_BITS") {
            opts.bloom_bits_per_key = v;
        }
        if let Ok(v) = std::env::var("LARK_COMPRESSION") {
            if let Some(compression) = parse_compression(&v) {
                opts.compression = compression;
            }
        }
        if let Ok(v) = std::env::var("LARK_COMPRESSION_PER_LEVEL") {
            opts.compression_per_level = parse_compression_list(&v);
        }
        if let Some(v) = env_usize("LARK_L0_TRIGGER") {
            opts.l0_compaction_trigger = v;
        }
        if let Some(v) = env_usize("LARK_L0_SLOWDOWN_TRIGGER") {
            opts.level0_slowdown_writes_trigger = v;
        }
        if let Some(v) = env_usize("LARK_L0_STOP_TRIGGER") {
            opts.level0_stop_writes_trigger = v;
        }
        if let Some(v) = env_u64("LARK_SOFT_PENDING_COMPACTION_MB") {
            opts.soft_pending_compaction_bytes_limit = v * 1024 * 1024;
        }
        if let Some(v) = env_u64("LARK_HARD_PENDING_COMPACTION_MB") {
            opts.hard_pending_compaction_bytes_limit = v * 1024 * 1024;
        }
        if let Some(v) = env_usize("LARK_MAX_WRITE_BUFFER_NUMBER") {
            opts.max_write_buffer_number = v;
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
        if let Some(v) = env_usize("LARK_MAX_BACKGROUND_COMPACTIONS") {
            opts.max_background_compactions = v;
        }
        if let Some(v) = env_bool("LARK_EVICT_COMPACTION_DATA") {
            opts.evict_compaction_data_from_page_cache = v;
        }
        if let Some(v) = env_bool("LARK_PARTITIONED_INDEX") {
            opts.partitioned_index = v;
        }
        if let Some(v) = env_usize("LARK_METADATA_BLOCK_SIZE_KB") {
            opts.metadata_block_size = v * 1024;
        }
        if let Ok(v) = std::env::var("LARK_COMPACTION_STYLE") {
            if let Some(style) = parse_compaction_style(&v) {
                opts.compaction_style = style;
            }
        }
        if let Some(v) = env_u64("LARK_FIFO_MAX_TABLE_FILES_MB") {
            opts.fifo_max_table_files_size = v * 1024 * 1024;
        }
        if let Some(v) = env_u32("LARK_UNIVERSAL_SIZE_RATIO") {
            opts.universal_size_ratio = v;
        }
        if let Some(v) = env_u32("LARK_UNIVERSAL_MIN_MERGE_WIDTH") {
            opts.universal_min_merge_width = v;
        }
        if let Some(v) = env_u32("LARK_UNIVERSAL_MAX_MERGE_WIDTH") {
            opts.universal_max_merge_width = v;
        }
        if let Some(v) = env_u32("LARK_UNIVERSAL_MAX_SIZE_AMP_PERCENT") {
            opts.universal_max_size_amplification_percent = v;
        }

        opts
    }

    /// Convert to `lark_kv::Options`.
    pub(crate) fn to_lark_options(&self) -> lark_kv::Options {
        lark_kv::Options {
            write_buffer_size: self.write_buffer_size,
            block_size: self.block_size,
            block_cache_size: self.block_cache_size,
            block_cache_num_shard_bits: self.block_cache_num_shard_bits,
            strict_capacity_limit: self.strict_capacity_limit,
            bloom_bits_per_key: self.bloom_bits_per_key,
            compression: self.compression.into(),
            compression_per_level: self
                .compression_per_level
                .as_ref()
                .map(|levels| levels.iter().copied().map(Into::into).collect()),
            l0_compaction_trigger: self.l0_compaction_trigger,
            level0_slowdown_writes_trigger: self.level0_slowdown_writes_trigger,
            level0_stop_writes_trigger: self.level0_stop_writes_trigger,
            soft_pending_compaction_bytes_limit: self.soft_pending_compaction_bytes_limit,
            hard_pending_compaction_bytes_limit: self.hard_pending_compaction_bytes_limit,
            max_write_buffer_number: self.max_write_buffer_number,
            level_base_bytes: self.level_base_bytes,
            level_size_multiplier: self.level_size_multiplier,
            target_file_size: self.target_file_size,
            max_background_compactions: self.max_background_compactions,
            evict_compaction_data_from_page_cache: self.evict_compaction_data_from_page_cache,
            partitioned_index: self.partitioned_index,
            metadata_block_size: self.metadata_block_size,
            compaction_style: self.compaction_style.into(),
            fifo_compaction_options: lark_kv::FifoCompactionOptions {
                max_table_files_size: self.fifo_max_table_files_size,
            },
            universal_compaction_options: lark_kv::UniversalCompactionOptions {
                size_ratio: self.universal_size_ratio,
                min_merge_width: self.universal_min_merge_width,
                max_merge_width: self.universal_max_merge_width,
                max_size_amplification_percent: self.universal_max_size_amplification_percent,
            },
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

fn env_u32(key: &str) -> Option<u32> {
    std::env::var(key).ok()?.parse().ok()
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse().ok()
}

fn env_bool(key: &str) -> Option<bool> {
    std::env::var(key).ok().and_then(|v| parse_bool(&v))
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_compression(value: &str) -> Option<CompressionType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "0" | "false" | "none" | "off" => Some(CompressionType::None),
        "snappy" => Some(CompressionType::Snappy),
        "1" | "true" | "lz4" | "on" => Some(CompressionType::Lz4),
        _ => None,
    }
}

fn parse_compression_list(value: &str) -> Option<Vec<CompressionType>> {
    if value.trim().is_empty() {
        return None;
    }

    let mut out = Vec::new();
    for item in value.split(',') {
        out.push(parse_compression(item)?);
    }
    Some(out)
}

fn parse_compaction_style(value: &str) -> Option<CompactionStyle> {
    match value.trim().to_ascii_lowercase().as_str() {
        "level" | "leveled" => Some(CompactionStyle::Level),
        "fifo" => Some(CompactionStyle::Fifo),
        "universal" => Some(CompactionStyle::Universal),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_compression_builder_preserves_existing_behavior() {
        let disabled = LarkStoreOptions::new()
            .with_compression(false)
            .to_lark_options();
        assert_eq!(disabled.compression, lark_kv::CompressionType::None);

        let enabled = LarkStoreOptions::new()
            .with_compression(true)
            .to_lark_options();
        assert_eq!(enabled.compression, lark_kv::CompressionType::Lz4);
    }

    #[test]
    fn to_lark_options_maps_production_knobs() {
        let opts = LarkStoreOptions::new()
            .with_block_cache_size(11)
            .with_write_buffer_size(22)
            .with_block_size(33)
            .with_block_cache_num_shard_bits(4)
            .with_strict_capacity_limit(true)
            .with_bloom_bits_per_key(5)
            .with_compression_type(CompressionType::Snappy)
            .with_compression_per_level(vec![
                CompressionType::None,
                CompressionType::Lz4,
                CompressionType::Snappy,
            ])
            .with_l0_compaction_trigger(6)
            .with_level0_slowdown_writes_trigger(7)
            .with_level0_stop_writes_trigger(8)
            .with_soft_pending_compaction_bytes_limit(9)
            .with_hard_pending_compaction_bytes_limit(10)
            .with_max_write_buffer_number(12)
            .with_level_base_bytes(13)
            .with_level_size_multiplier(14)
            .with_target_file_size(15)
            .with_max_background_compactions(16)
            .with_evict_compaction_data_from_page_cache(true)
            .with_partitioned_index(true)
            .with_metadata_block_size(17)
            .with_compaction_style(CompactionStyle::Universal)
            .with_fifo_max_table_files_size(18)
            .with_universal_size_ratio(19)
            .with_universal_min_merge_width(20)
            .with_universal_max_merge_width(21)
            .with_universal_max_size_amplification_percent(22)
            .with_durability(DurabilityMode::Immediate);

        let lark_opts = opts.to_lark_options();

        assert_eq!(lark_opts.block_cache_size, 11);
        assert_eq!(lark_opts.write_buffer_size, 22);
        assert_eq!(lark_opts.block_size, 33);
        assert_eq!(lark_opts.block_cache_num_shard_bits, 4);
        assert!(lark_opts.strict_capacity_limit);
        assert_eq!(lark_opts.bloom_bits_per_key, 5);
        assert_eq!(lark_opts.compression, lark_kv::CompressionType::Snappy);
        assert_eq!(
            lark_opts.compression_per_level,
            Some(vec![
                lark_kv::CompressionType::None,
                lark_kv::CompressionType::Lz4,
                lark_kv::CompressionType::Snappy,
            ])
        );
        assert_eq!(lark_opts.l0_compaction_trigger, 6);
        assert_eq!(lark_opts.level0_slowdown_writes_trigger, 7);
        assert_eq!(lark_opts.level0_stop_writes_trigger, 8);
        assert_eq!(lark_opts.soft_pending_compaction_bytes_limit, 9);
        assert_eq!(lark_opts.hard_pending_compaction_bytes_limit, 10);
        assert_eq!(lark_opts.max_write_buffer_number, 12);
        assert_eq!(lark_opts.level_base_bytes, 13);
        assert_eq!(lark_opts.level_size_multiplier, 14);
        assert_eq!(lark_opts.target_file_size, 15);
        assert_eq!(lark_opts.max_background_compactions, 16);
        assert!(lark_opts.evict_compaction_data_from_page_cache);
        assert!(lark_opts.partitioned_index);
        assert_eq!(lark_opts.metadata_block_size, 17);
        assert_eq!(
            lark_opts.compaction_style,
            lark_kv::CompactionStyle::Universal
        );
        assert_eq!(lark_opts.fifo_compaction_options.max_table_files_size, 18);
        assert_eq!(lark_opts.universal_compaction_options.size_ratio, 19);
        assert_eq!(lark_opts.universal_compaction_options.min_merge_width, 20);
        assert_eq!(lark_opts.universal_compaction_options.max_merge_width, 21);
        assert_eq!(
            lark_opts
                .universal_compaction_options
                .max_size_amplification_percent,
            22
        );
        assert_eq!(lark_opts.durability, lark_kv::DurabilityMode::Immediate);
    }

    #[test]
    fn parse_bool_accepts_common_forms() {
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("YES"), Some(true));
        assert_eq!(parse_bool("off"), Some(false));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("maybe"), None);
    }

    #[test]
    fn parse_compression_accepts_bool_and_codec_names() {
        assert_eq!(parse_compression("0"), Some(CompressionType::None));
        assert_eq!(parse_compression("false"), Some(CompressionType::None));
        assert_eq!(parse_compression("none"), Some(CompressionType::None));
        assert_eq!(parse_compression("snappy"), Some(CompressionType::Snappy));
        assert_eq!(parse_compression("1"), Some(CompressionType::Lz4));
        assert_eq!(parse_compression("true"), Some(CompressionType::Lz4));
        assert_eq!(parse_compression("LZ4"), Some(CompressionType::Lz4));
        assert_eq!(parse_compression("zstd"), None);
    }

    #[test]
    fn parse_compression_list_requires_known_non_empty_entries() {
        assert_eq!(
            parse_compression_list("none,lz4,snappy"),
            Some(vec![
                CompressionType::None,
                CompressionType::Lz4,
                CompressionType::Snappy,
            ])
        );
        assert_eq!(parse_compression_list(""), None);
        assert_eq!(parse_compression_list("none,,lz4"), None);
        assert_eq!(parse_compression_list("none,zstd"), None);
    }

    #[test]
    fn parse_compaction_style_accepts_known_values() {
        assert_eq!(
            parse_compaction_style("level"),
            Some(CompactionStyle::Level)
        );
        assert_eq!(
            parse_compaction_style("leveled"),
            Some(CompactionStyle::Level)
        );
        assert_eq!(parse_compaction_style("fifo"), Some(CompactionStyle::Fifo));
        assert_eq!(
            parse_compaction_style("UNIVERSAL"),
            Some(CompactionStyle::Universal)
        );
        assert_eq!(parse_compaction_style("tiered"), None);
    }
}
