# Shinzo Benchmarking

Scripts for benchmarking defradb.rs under the Shinzo Ethereum indexer workload.

Tracking issue: [#419](https://github.com/jackzampolin/defradb.rs/issues/419)

## Quick Start

```bash
# 1. Always start clean
./scripts/shinzo-test.sh clean

# 2. Build release (required after code changes)
cargo build --release

# 3. Start defra + indexer (uses random ports, logs to /tmp/shinzo-test/)
./scripts/shinzo-test.sh

# 4. In another terminal, monitor RSS/CPU/disk every 5s
./scripts/shinzo-test.sh monitor
```

The script picks random free ports, so no conflicts. Everything lives under `/tmp/shinzo-test/`.

## Commands

| Command | Purpose |
|---------|---------|
| `./scripts/shinzo-test.sh` | Start defra + indexer |
| `./scripts/shinzo-test.sh clean` | Remove all state and logs |
| `./scripts/shinzo-test.sh stop` | Graceful shutdown |
| `./scripts/shinzo-test.sh status` | Ports, PIDs, latest block height, disk |
| `./scripts/shinzo-test.sh monitor` | Live RSS/CPU/disk/block/errors every 5s |
| `./scripts/shinzo-test.sh logs defra` | Tail defra log |
| `./scripts/shinzo-test.sh logs indexer` | Tail indexer log |
| `./scripts/shinzo-test.sh query '<graphql>'` | Run a GraphQL query against the running node |

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `STORE` | `fjall` | Storage backend: `fjall`, `rocksdb`, `redb`, `memory` |
| `CONCURRENCY` | `4` | Concurrent blocks to index |
| `RECEIPT_WORKERS` | `4` | Receipt processing workers |
| `START_HEIGHT_OVERRIDE` | `23700000` | Ethereum start block |
| `WATCHDOG_DISK_LIMIT_GB` | `200` | Kill indexer if free disk drops below this |
| `WATCHDOG_RSS_LIMIT_MB` | `12000` | Kill indexer if RSS exceeds this |

### RocksDB Tuning (`STORE=rocksdb`)

All knobs exposed via environment variables (see `crates/storage/src/backends/rocksdb/config.rs`):

| Env Var | Default | Purpose |
|---------|---------|---------|
| `ROCKS_BLOCK_CACHE_MB` | 512 | Block cache size |
| `ROCKS_WRITE_BUFFER_MB` | 64 | Write buffer (memtable) size |
| `ROCKS_MAX_WRITE_BUFFERS` | 4 | Max memtables before stall |
| `ROCKS_COMPACTIONS` | 4 | Max background compaction threads |
| `ROCKS_FLUSHES` | 2 | Max background flush threads |
| `ROCKS_L0_SLOWDOWN` | 20 | L0 file count that triggers write slowdown |
| `ROCKS_L0_STOP` | 36 | L0 file count that stops writes entirely |
| `ROCKS_TARGET_FILE_MB` | 64 | Target SST file size |
| `ROCKS_LEVEL_BASE_MB` | 256 | Max bytes for L1 |
| `ROCKS_BLOCK_SIZE_KB` | 16 | SST block size |
| `ROCKS_COMPRESSION` | `lz4` | Compression: `none`, `snappy`, `zstd`, `lz4` |
| `ROCKS_COMPACTION_STYLE` | `level` | Strategy: `level`, `universal` |
| `ROCKS_BLOB_FILES` | `false` | Enable BlobDB for large values |
| `ROCKS_MIN_BLOB_SIZE` | 256 | Min value size (bytes) for blob storage |

### Example: High-Throughput Run

```bash
STORE=rocksdb \
  ROCKS_BLOCK_CACHE_MB=2048 \
  ROCKS_WRITE_BUFFER_MB=128 \
  ROCKS_MAX_WRITE_BUFFERS=6 \
  ROCKS_COMPACTIONS=8 \
  CONCURRENCY=16 RECEIPT_WORKERS=16 \
  ./scripts/shinzo-test.sh
```

## Metrics to Capture

| Metric | Source |
|--------|--------|
| RSS start/peak/end | `ps -o rss=` or monitor output |
| Blocks indexed | Indexer log height delta |
| Wall time | Timestamps from monitor |
| Blocks/sec | blocks / wall_time |
| Disk usage | `du -sh /tmp/shinzo-test/` |
| Error count | `grep -c ERROR /tmp/shinzo-test/indexer.log` |

## After a Run

1. Stop: `./scripts/shinzo-test.sh stop`
2. Capture final metrics from monitor output
3. Post a comment on [issue #419](https://github.com/jackzampolin/defradb.rs/issues/419) with results
4. Save logs: `cp /tmp/shinzo-test/*.log /tmp/shinzo-run-N/`
