//! Isolated, bounded aggregate-work experiments. No network service is exposed.
//! CPU is process CPU (all threads, user + kernel), never inferred from latency.
use std::{path::Path, time::Instant};

use anyhow::{bail, ensure, Context, Result};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    dense,
    dense_batch::{BatchEvaluator, BatchKernel},
    finite_differences as fd, single_pass,
    snapshot::SnapshotView,
    subset_xor::SubsetXorIndex,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub candidate: String,
    pub rows: usize,
    pub row_bytes: usize,
    pub queries: usize,
    pub seed: u64,
    pub group_bits: usize,
    pub field_bits: usize,
    pub fanout: usize,
    pub payload_slots: usize,
    pub partitions: usize,
    pub batch_size: usize,
    pub kernel: String,
    pub max_resident_bytes: usize,
    pub max_download_bytes: usize,
    pub max_upload_bytes: usize,
    pub rebuild_every: usize,
    pub update_batch: usize,
    pub workers: usize,
    pub cold_cache_bytes: usize,
    pub arrival_interval_ms: u64,
    pub max_queue_dwell_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            candidate: "dense".into(),
            rows: 4096,
            row_bytes: 96,
            queries: 10,
            seed: 1,
            group_bits: 2,
            field_bits: 16,
            fanout: 4,
            payload_slots: 4,
            partitions: 4,
            batch_size: 8,
            kernel: "shared".into(),
            max_resident_bytes: 512 << 20,
            max_download_bytes: 1 << 20,
            max_upload_bytes: 1 << 20,
            rebuild_every: 0,
            update_batch: 1,
            workers: 1,
            cold_cache_bytes: 0,
            arrival_interval_ms: 0,
            max_queue_dwell_ms: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct Cost {
    cpu_ms: Option<f64>,
    wall_ms: f64,
}
fn cpu_ms() -> Option<f64> {
    #[cfg(unix)]
    {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: ts is a live writable timespec; this clock has no side effects.
        let status = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
        (status == 0).then_some(ts.tv_sec as f64 * 1000.0 + ts.tv_nsec as f64 / 1e6)
    }
    #[cfg(not(unix))]
    {
        None
    }
}
fn measure<T>(f: impl FnOnce() -> Result<T>) -> Result<(T, Cost)> {
    let cpu = cpu_ms();
    let wall = Instant::now();
    let value = f()?;
    Ok((
        value,
        Cost {
            wall_ms: wall.elapsed().as_secs_f64() * 1000.0,
            cpu_ms: cpu.zip(cpu_ms()).map(|(a, b)| (b - a).max(0.0)),
        },
    ))
}
impl Cost {
    fn add(&mut self, other: Self) {
        self.wall_ms += other.wall_ms;
        self.cpu_ms = self.cpu_ms.zip(other.cpu_ms).map(|(a, b)| a + b);
    }
    fn zero() -> Self {
        Self {
            cpu_ms: cpu_ms().map(|_| 0.0),
            wall_ms: 0.0,
        }
    }
}

#[derive(Default, Serialize)]
struct Sample {
    logical_queries: usize,
    server: Cost,
    client: Cost,
    upload_bytes: usize,
    download_bytes: usize,
    source_operand_bytes: usize,
    selector_bytes: usize,
    scratch_write_bytes: usize,
    useful_bytes: usize,
    payload_requests: usize,
    queue_delay_ms: Vec<f64>,
}
impl Sample {
    fn new(logical_queries: usize) -> Self {
        Self {
            logical_queries,
            server: Cost::zero(),
            client: Cost::zero(),
            ..Self::default()
        }
    }
}

fn validate(c: &Config) -> Result<()> {
    ensure!(
        c.rows > 0 && c.row_bytes >= 8 && c.queries > 0,
        "rows/queries must be positive and row_bytes >= 8"
    );
    ensure!(
        [
            "dense",
            "sharded",
            "public",
            "decoy",
            "witness",
            "subset",
            "single-pass",
            "batch",
            "finite-differences",
            "field-bitmap",
            "field-inline",
            "field-public",
            "field-postings"
        ]
        .contains(&c.candidate.as_str()),
        "unknown candidate"
    );
    ensure!(
        [1, 2, 4, 8].contains(&c.group_bits)
            || c.candidate == "subset" && [6, 10].contains(&c.group_bits),
        "unsupported group_bits"
    );
    ensure!(
        [16, 32, 64].contains(&c.field_bits),
        "field_bits must be 16, 32 or 64"
    );
    ensure!(
        c.fanout > 0 && c.payload_slots >= c.fanout && c.batch_size > 0,
        "invalid fanout, padding or batch size"
    );
    ensure!(c.update_batch <= c.rows, "update_batch exceeds rows");
    ensure!(
        [1, 2, 4, 8].contains(&c.workers),
        "workers must be 1/2/4/8 per operator"
    );
    ensure!(
        c.cold_cache_bytes <= c.max_resident_bytes / 4,
        "cache scrub exceeds budget"
    );
    ensure!(
        (c.rows.div_ceil(c.fanout) as u128) < (1u128 << c.field_bits),
        "field domain must leave an absent value"
    );
    let bytes = c
        .rows
        .checked_mul(c.row_bytes)
        .context("table size overflow")?;
    ensure!(
        bytes.checked_mul(4).context("resident size overflow")? <= c.max_resident_bytes,
        "base tables and working reserve exceed resident budget"
    );
    ensure!(
        c.queries <= 1_000_000 && c.payload_slots <= c.rows && c.batch_size <= 512,
        "experiment exceeds query/slot/batch bounds"
    );
    Ok(())
}

// Each group contains 2^g rows, each row an N-bit membership bitmap. These
// are field-value bits, unlike subset_xor's groups of source row selectors.
struct FieldIndex {
    groups: Vec<Vec<u8>>,
    bitmap_bytes: usize,
    bits: usize,
}
impl FieldIndex {
    fn build(c: &Config, fields: &[u64]) -> Result<Self> {
        let bitmap_bytes = c.rows.div_ceil(8);
        let group_count = c.field_bits / c.group_bits;
        let index_bytes = group_count
            .checked_mul(1 << c.group_bits)
            .and_then(|n| n.checked_mul(bitmap_bytes))
            .context("field index overflow")?;
        ensure!(
            index_bytes
                .checked_mul(2)
                .and_then(|n| n.checked_add(4 * c.rows * c.row_bytes))
                .is_some_and(|n| n <= c.max_resident_bytes),
            "field index exceeds resident budget"
        );
        let mut groups = vec![vec![0; (1 << c.group_bits) * bitmap_bytes]; group_count];
        for (row, &field) in fields.iter().enumerate() {
            for (group, table) in groups.iter_mut().enumerate() {
                let value =
                    ((field >> (group * c.group_bits)) & ((1 << c.group_bits) - 1)) as usize;
                table[value * bitmap_bytes + row / 8] |= 1 << (row % 8);
            }
        }
        Ok(Self {
            groups,
            bitmap_bytes,
            bits: c.group_bits,
        })
    }
    fn bytes(&self) -> usize {
        self.groups.iter().map(Vec::len).sum()
    }
    fn select(
        &self,
        value: u64,
        private: bool,
        rng: &mut StdRng,
        s: &mut Sample,
    ) -> Result<Vec<usize>> {
        let mut intersection = vec![255; self.bitmap_bytes];
        for (group, rows) in self.groups.iter().enumerate() {
            let selected = ((value >> (group * self.bits)) & ((1 << self.bits) - 1)) as usize;
            let view = SnapshotView::new(rows, 1 << self.bits, self.bitmap_bytes);
            let bitmap = if private {
                retrieve(view, selected, rng, s)?
            } else {
                let (result, cost) = measure(|| Ok(view.row(selected)?.to_vec()))?;
                s.server.add(cost);
                s.source_operand_bytes += self.bitmap_bytes;
                s.upload_bytes += 8;
                s.download_bytes += self.bitmap_bytes;
                result
            };
            let (_, cost) = measure(|| {
                for (a, b) in intersection.iter_mut().zip(bitmap) {
                    *a &= b;
                }
                Ok(())
            })?;
            s.client.add(cost);
        }
        let (ids, cost) = measure(|| {
            Ok(intersection
                .iter()
                .enumerate()
                .flat_map(|(byte, &bits)| {
                    (0..8)
                        .filter(move |bit| bits & (1 << bit) != 0)
                        .map(move |bit| byte * 8 + bit)
                })
                .collect())
        })?;
        s.client.add(cost);
        Ok(ids)
    }
}

fn retrieve(
    view: SnapshotView<'_>,
    target: usize,
    rng: &mut StdRng,
    s: &mut Sample,
) -> Result<Vec<u8>> {
    let (shares, cost) = measure(|| dense::query_shares(target, view.bucket_count, 2, rng))?;
    s.client.add(cost);
    let mut answers = Vec::new();
    for share in shares {
        let selected = share
            .iter()
            .enumerate()
            .map(|(byte, &bits)| {
                let valid = (view.bucket_count - byte * 8).min(8);
                (bits & ((1u16 << valid) - 1) as u8).count_ones() as usize
            })
            .sum::<usize>();
        s.upload_bytes += share.len();
        s.selector_bytes += share.len();
        s.source_operand_bytes += selected * view.row_size;
        let (answer, cost) = measure(|| dense::answer(view, &share))?;
        s.server.add(cost);
        s.download_bytes += answer.len();
        answers.push(answer);
    }
    let (answer, cost) = measure(|| dense::combine(&answers))?;
    s.client.add(cost);
    Ok(answer)
}

pub fn run_file(path: &Path) -> Result<Value> {
    let c: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    run(&c)
}

pub fn run(c: &Config) -> Result<Value> {
    validate(c)?;
    if c.candidate == "witness" {
        return run_witness(c);
    }
    let mut rng = StdRng::seed_from_u64(c.seed);
    let (mut rows, corpus_cost) = measure(|| {
        let mut data = vec![0; c.rows * c.row_bytes];
        rng.fill_bytes(&mut data);
        Ok(data)
    })?;
    // Permuted row order avoids giving an index an accidental clustered advantage.
    let mut fields: Vec<u64> = (0..c.rows).map(|i| (i / c.fanout) as u64).collect();
    use rand::seq::SliceRandom;
    fields.shuffle(&mut rng);
    let mut build = Cost::zero();
    let mut maintenance = Cost::zero();
    let mut client_setup = Cost::zero();
    let mut client_setup_max_cpu_ms: Option<f64> = cpu_ms().map(|_| 0.0);
    let mut setup_download = 0;
    let mut persistent_client_bytes = 0;
    let mut index_bytes = 0;
    let mut subset = None;
    let mut field = None;
    let mut postings = None;
    let mut inline = Vec::new();
    let inline_width = c
        .payload_slots
        .checked_mul(c.row_bytes + 8)
        .and_then(|v| v.checked_add(8))
        .context("inline width overflow")?;
    let mut state = None;
    let mut encoded = None;
    let mut cloud = Vec::new();
    let mut finite_parameters = Value::Null;
    let mut generation = [0u8; 32];
    let mut rebuild_count = 0;
    let mut samples = Vec::new();
    let (evaluator, pool_setup) = measure(|| {
        if c.candidate == "sharded" {
            Ok(Some(dense::ParallelEvaluator::new(c.workers)?))
        } else {
            Ok(None)
        }
    })?;
    build.add(pool_setup);
    let mut cache_scrub = vec![0u8; c.cold_cache_bytes];
    let arrival_start = Instant::now();
    let mut arrival_cursor = 0usize;
    // Build both logical replicas independently; they are co-located, not a
    // network deployment. One immutable copy can then serve sequential role calls.
    for iteration in 0..c.queries {
        let arrival_first = arrival_cursor;
        let mut batch_queries = c.batch_size;
        let mut queue_delay_ms = Vec::new();
        if c.candidate == "batch" && c.arrival_interval_ms > 0 {
            let first = arrival_cursor as u128 * c.arrival_interval_ms as u128;
            let last = first + (c.batch_size - 1) as u128 * c.arrival_interval_ms as u128;
            let release = last.min(first + c.max_queue_dwell_ms as u128);
            let now = arrival_start.elapsed().as_millis();
            if release > now {
                std::thread::sleep(std::time::Duration::from_millis((release - now) as u64));
            }
            let now_ms = arrival_start.elapsed().as_secs_f64() * 1000.0;
            batch_queries = (((now_ms - first as f64).max(0.0) / c.arrival_interval_ms as f64)
                .floor() as usize
                + 1)
            .min(c.batch_size);
            queue_delay_ms = (0..batch_queries)
                .map(|i| {
                    (now_ms - (first + i as u128 * c.arrival_interval_ms as u128) as f64).max(0.0)
                })
                .collect();
            arrival_cursor += batch_queries;
        }
        // A declared cache-conditioning control, outside protocol CPU. The
        // scrub allocation remains part of the host's resident experiment.
        for byte in cache_scrub.iter_mut().step_by(64) {
            *byte = byte.wrapping_add(1);
        }
        std::hint::black_box(&cache_scrub);
        let rebuilding = iteration == 0 || c.rebuild_every > 0 && iteration % c.rebuild_every == 0;
        if rebuilding {
            if iteration > 0 {
                let (_, cost) = measure(|| {
                    for i in 0..c.update_batch {
                        let row = (iteration + i) % c.rows;
                        rows[row * c.row_bytes] ^= 1;
                    }
                    Ok(())
                })?;
                maintenance.add(cost);
                generation = *blake3::hash(&iteration.to_le_bytes()).as_bytes();
                rebuild_count += 1;
            }
            // Release the previous generation before rebuilding; concurrent
            // serving during rebuild is outside this isolated experiment.
            subset = None;
            field = None;
            postings = None;
            encoded = None;
            state = None;
            inline.clear();
            let view = SnapshotView::new(&rows, c.rows, c.row_bytes);
            for _ in 0..if ["public", "decoy"].contains(&c.candidate.as_str()) {
                1
            } else {
                2
            } {
                let (_, cost) = measure(|| {
                    // Materialize the source replica even when there is no index.
                    std::hint::black_box(rows.clone());
                    match c.candidate.as_str() {
                        "field-inline" => {
                            index_bytes = (c.rows.div_ceil(c.fanout) + 1)
                                .checked_mul(inline_width)
                                .context("inline table overflow")?;
                            ensure!(
                                index_bytes <= (c.max_resident_bytes - 4 * rows.len()) / 2,
                                "inline table exceeds resident budget"
                            );
                            inline = vec![0; index_bytes];
                            for (id, &value) in fields.iter().enumerate() {
                                let start = value as usize * inline_width;
                                let count = u64::from_le_bytes(
                                    inline[start..start + 8].try_into().unwrap(),
                                ) as usize;
                                ensure!(count < c.payload_slots, "inline posting overflow");
                                let offset = start + 8 + count * (c.row_bytes + 8);
                                inline[offset..offset + 8]
                                    .copy_from_slice(&(id as u64).to_le_bytes());
                                inline[offset + 8..offset + 8 + c.row_bytes]
                                    .copy_from_slice(view.row(id)?);
                                inline[start..start + 8]
                                    .copy_from_slice(&((count + 1) as u64).to_le_bytes());
                            }
                        }
                        "subset" => {
                            let estimate = SubsetXorIndex::estimate(view, c.group_bits)?;
                            ensure!(
                                2 * estimate.index_data_bytes + 4 * rows.len()
                                    <= c.max_resident_bytes,
                                "subset index exceeds resident budget"
                            );
                            subset = Some(SubsetXorIndex::build_with_limit(
                                view,
                                c.group_bits,
                                c.max_resident_bytes / 2,
                            )?);
                            index_bytes = estimate.index_data_bytes;
                        }
                        "field-bitmap" | "field-public" => {
                            field = Some(FieldIndex::build(c, &fields)?);
                            index_bytes = field.as_ref().unwrap().bytes();
                        }
                        "field-postings" => {
                            let mut map = std::collections::BTreeMap::<u64, Vec<usize>>::new();
                            for (id, &value) in fields.iter().enumerate() {
                                map.entry(value).or_default().push(id);
                            }
                            index_bytes = c.rows * size_of::<usize>() + map.len() * 8;
                            postings = Some(map);
                        }
                        "finite-differences" => {
                            let params = fd::Parameters::pareto_variants(c.rows, c.row_bytes, 30)?
                                .into_iter()
                                .find(|p| {
                                    p.storage_bytes().is_ok_and(|b| {
                                        b <= (c.max_resident_bytes - 4 * rows.len()) / 2
                                    }) && p
                                        .answer_bytes()
                                        .is_ok_and(|b| 2 * b <= c.max_download_bytes)
                                })
                                .context(
                                    "no finite-differences variant passes storage/download gates",
                                )?;
                            index_bytes = params.storage_bytes()?;
                            finite_parameters = json!({"m":params.variables_m,"d":params.total_degree_d,"cloud_count":params.cloud_count});
                            cloud = params.cloud();
                            encoded = Some(fd::EncodedDatabase::encode(params, &rows)?);
                        }
                        _ => {}
                    }
                    Ok(())
                })?;
                if iteration == 0 {
                    build.add(cost);
                } else {
                    maintenance.add(cost);
                }
            }
            if c.candidate == "single-pass" {
                let (new_state, cost) = measure(|| {
                    single_pass::ClientState::setup(view, generation, c.partitions, &mut rng)
                })?;
                client_setup.add(cost);
                client_setup_max_cpu_ms = client_setup_max_cpu_ms
                    .zip(cost.cpu_ms)
                    .map(|(a, b)| a.max(b));
                persistent_client_bytes = new_state.payload_bytes();
                setup_download += rows.len();
                // Explicit server read/copy for setup publication, counted each generation.
                let (_, cost) = measure(|| {
                    std::hint::black_box(rows.clone());
                    Ok(())
                })?;
                if iteration == 0 {
                    build.add(cost);
                } else {
                    maintenance.add(cost);
                }
                state = Some(new_state);
            }
        }
        let view = SnapshotView::new(&rows, c.rows, c.row_bytes);
        // Same targets across adapters despite different setup/query RNG consumption.
        let target_hash =
            blake3::hash(&[c.seed.to_le_bytes(), (iteration as u64).to_le_bytes()].concat());
        let target = (u64::from_le_bytes(target_hash.as_bytes()[..8].try_into().unwrap())
            % c.rows as u64) as usize;
        let mut s = Sample::new(if c.candidate == "batch" {
            batch_queries
        } else {
            1
        });
        s.queue_delay_ms = queue_delay_ms;
        if c.candidate == "batch" && c.arrival_interval_ms > 0 {
            let now = arrival_start.elapsed().as_secs_f64() * 1000.0;
            s.queue_delay_ms = (0..batch_queries)
                .map(|i| (now - (arrival_first + i) as f64 * c.arrival_interval_ms as f64).max(0.0))
                .collect();
        }
        match c.candidate.as_str() {
            "public" | "decoy" => {
                let count = if c.candidate == "public" { 1 } else { 100 };
                let mut requests = (0..count - 1)
                    .map(|_| rng.next_u64() as usize % c.rows)
                    .collect::<Vec<_>>();
                requests.push(target);
                requests.shuffle(&mut rng);
                let target_slot = requests.iter().position(|&i| i == target).unwrap();
                let (answers, cost) = measure(|| {
                    requests
                        .iter()
                        .map(|&i| Ok(view.row(i)?.to_vec()))
                        .collect::<Result<Vec<_>>>()
                })?;
                s.server.add(cost);
                ensure!(
                    answers[target_slot] == view.row(target)?,
                    "control reconstruction failed"
                );
                s.upload_bytes = count * 8;
                s.download_bytes = count * c.row_bytes;
                s.source_operand_bytes = count * c.row_bytes;
                s.useful_bytes = c.row_bytes;
            }
            "sharded" => {
                let (shares, cost) = measure(|| dense::query_shares(target, c.rows, 2, &mut rng))?;
                s.client.add(cost);
                let mut answers = Vec::new();
                for share in shares {
                    s.upload_bytes += share.len();
                    s.selector_bytes += share.len();
                    s.source_operand_bytes += (0..c.rows)
                        .filter(|&i| share[i / 8] & (1 << (i % 8)) != 0)
                        .count()
                        * c.row_bytes;
                    let (answer, cost) =
                        measure(|| evaluator.as_ref().unwrap().answer(view, &share))?;
                    s.server.add(cost);
                    s.download_bytes += answer.len();
                    answers.push(answer);
                }
                let (answer, cost) = measure(|| dense::combine(&answers))?;
                s.client.add(cost);
                ensure!(answer == view.row(target)?, "sharded reconstruction failed");
                s.useful_bytes = c.row_bytes;
            }
            "field-inline" => {
                let value = if iteration % 4 == 3 {
                    c.rows.div_ceil(c.fanout)
                } else {
                    fields[target] as usize
                };
                let result = retrieve(
                    SnapshotView::new(&inline, c.rows.div_ceil(c.fanout) + 1, inline_width),
                    value,
                    &mut rng,
                    &mut s,
                )?;
                let expected: Vec<_> = fields
                    .iter()
                    .enumerate()
                    .filter_map(|(i, &v)| (v == value as u64).then_some(i))
                    .collect();
                ensure!(
                    u64::from_le_bytes(result[..8].try_into().unwrap()) as usize == expected.len(),
                    "inline result count mismatch"
                );
                for (slot, &id) in expected.iter().enumerate() {
                    let offset = 8 + slot * (c.row_bytes + 8);
                    ensure!(
                        u64::from_le_bytes(result[offset..offset + 8].try_into().unwrap()) as usize
                            == id,
                        "inline ID mismatch"
                    );
                    ensure!(
                        result[offset + 8..offset + 8 + c.row_bytes] == *view.row(id)?,
                        "inline payload mismatch"
                    );
                }
                s.useful_bytes = expected.len() * (c.row_bytes + 8);
            }
            "field-bitmap" | "field-public" | "field-postings" => {
                // Every fourth query is absent. Output schedule is fixed even for it.
                let value = if iteration % 4 == 3 {
                    c.rows.div_ceil(c.fanout) as u64
                } else {
                    fields[target]
                };
                let private = c.candidate == "field-bitmap";
                let ids = if let Some(index) = &field {
                    index.select(value, private, &mut rng, &mut s)?
                } else {
                    let (ids, cost) = measure(|| {
                        Ok(postings
                            .as_ref()
                            .unwrap()
                            .get(&value)
                            .cloned()
                            .unwrap_or_default())
                    })?;
                    s.server.add(cost);
                    s.upload_bytes += 8;
                    s.download_bytes += ids.len() * 8;
                    ids
                };
                let expected: Vec<_> = fields
                    .iter()
                    .enumerate()
                    .filter_map(|(i, &v)| (v == value).then_some(i))
                    .collect();
                ensure!(
                    ids == expected && ids.len() <= c.payload_slots,
                    "incorrect or overflowing complete search"
                );
                for slot in 0..c.payload_slots {
                    let id = ids.get(slot).copied().unwrap_or(0);
                    let result = if private {
                        retrieve(view, id, &mut rng, &mut s)?
                    } else {
                        let (result, cost) = measure(|| Ok(view.row(id)?.to_vec()))?;
                        s.server.add(cost);
                        s.source_operand_bytes += c.row_bytes;
                        s.upload_bytes += 8;
                        s.download_bytes += c.row_bytes;
                        result
                    };
                    ensure!(result == view.row(id)?, "payload verification failed");
                    s.payload_requests += 1;
                }
                s.useful_bytes = ids.len() * (c.row_bytes + size_of::<u64>());
            }
            "subset" => {
                let (shares, cost) = measure(|| dense::query_shares(target, c.rows, 2, &mut rng))?;
                s.client.add(cost);
                let mut answers = Vec::new();
                for share in shares {
                    s.upload_bytes += share.len();
                    s.selector_bytes += share.len();
                    let (answer, cost) =
                        measure(|| subset.as_ref().unwrap().answer_with_metrics(&share))?;
                    s.server.add(cost);
                    s.source_operand_bytes += answer.metrics.logical_data_bytes_read;
                    s.download_bytes += answer.bytes.len();
                    answers.push(answer.bytes);
                }
                let (result, cost) = measure(|| dense::combine(&answers))?;
                s.client.add(cost);
                ensure!(result == view.row(target)?, "subset reconstruction failed");
                s.useful_bytes = c.row_bytes;
            }
            "single-pass" => {
                let state = state.as_mut().unwrap();
                let (query, cost) = measure(|| state.prepare_query(generation, target, &mut rng))?;
                s.client.add(cost);
                let mut answers = Vec::new();
                for q in query.server_queries() {
                    s.upload_bytes += q.wire_bytes();
                    let (answer, cost) = measure(|| single_pass::answer(view, generation, q))?;
                    s.server.add(cost);
                    s.download_bytes += answer.wire_bytes();
                    s.source_operand_bytes += q.indices().len() * c.row_bytes;
                    answers.push(answer);
                }
                let (result, cost) = measure(|| state.complete_query(generation, query, &answers))?;
                s.client.add(cost);
                ensure!(
                    result == view.row(target)?,
                    "SinglePass reconstruction failed"
                );
                s.useful_bytes = c.row_bytes;
            }
            "finite-differences" => {
                let db = encoded.as_ref().unwrap();
                let (query, cost) =
                    measure(|| fd::prepare_query(db.parameters(), target, &mut rng))?;
                s.client.add(cost);
                let mut answers = Vec::new();
                for q in query.server_queries {
                    let (answer, cost) = measure(|| db.answer(&cloud, q))?;
                    s.server.add(cost);
                    s.upload_bytes += 8;
                    s.download_bytes += answer.len();
                    s.source_operand_bytes += answer.len();
                    answers.push(answer);
                }
                let (result, cost) = measure(|| {
                    fd::recover(db.parameters(), &cloud, query, &answers.try_into().unwrap())
                })?;
                s.client.add(cost);
                ensure!(
                    result == view.row(target)?,
                    "finite-differences reconstruction failed"
                );
                s.useful_bytes = c.row_bytes;
            }
            "batch" => {
                let kernel = match c.kernel.as_str() {
                    "independent" => BatchKernel::Independent,
                    "shared" => BatchKernel::SharedRowMajor,
                    "blocked" => BatchKernel::CacheBlocked {
                        rows_per_block: 2048,
                    },
                    "transposed" => BatchKernel::SelectorTransposed,
                    "four-russians" => BatchKernel::GroupedFourRussians {
                        group_bits: c.group_bits,
                    },
                    _ => bail!("unknown batch kernel"),
                };
                let (queries, cost) = measure(|| {
                    (0..batch_queries)
                        .map(|i| dense::query_shares((target + i) % c.rows, c.rows, 2, &mut rng))
                        .collect::<Result<Vec<_>>>()
                })?;
                s.client.add(cost);
                let evaluator = BatchEvaluator::new(batch_queries, c.max_resident_bytes / 4)?;
                let mut all_answers = Vec::new();
                for replica in 0..2 {
                    let shares: Vec<_> = queries.iter().map(|q| &q[replica]).collect();
                    let (result, cost) = measure(|| evaluator.evaluate(view, &shares, kernel))?;
                    s.server.add(cost);
                    s.source_operand_bytes += result.metrics.immutable_source_operand_bytes;
                    s.scratch_write_bytes += result.metrics.scratch_write_bytes;
                    s.selector_bytes += result.metrics.unique_selector_bytes_addressed;
                    s.upload_bytes += result.metrics.query_share_bytes;
                    s.download_bytes += result.metrics.materialized_answer_bytes;
                    all_answers.push(result.answers);
                }
                for i in 0..batch_queries {
                    let (result, cost) = measure(|| {
                        dense::combine(&[all_answers[0][i].clone(), all_answers[1][i].clone()])
                    })?;
                    s.client.add(cost);
                    ensure!(
                        result == view.row((target + i) % c.rows)?,
                        "batch reconstruction failed"
                    );
                }
                s.useful_bytes = batch_queries * c.row_bytes;
            }
            _ => {
                ensure!(
                    retrieve(view, target, &mut rng, &mut s)? == view.row(target)?,
                    "dense reconstruction failed"
                );
                s.useful_bytes = c.row_bytes;
            }
        }
        samples.push(s);
    }
    let completed: usize = samples.iter().map(|s| s.logical_queries).sum();
    let online_cpu: Option<f64> = samples.iter().map(|s| s.server.cpu_ms).sum();
    let total_cpu = online_cpu
        .zip(build.cpu_ms)
        .zip(maintenance.cpu_ms)
        .map(|((a, b), c)| a + b + c);
    let caps_pass = client_setup_max_cpu_ms.is_some_and(|ms| ms <= 10_000.0)
        && persistent_client_bytes <= 64 << 20
        && setup_download / (rebuild_count + 1) <= 64 << 20
        && samples.iter().all(|s| {
            s.upload_bytes / s.logical_queries <= c.max_upload_bytes
                && s.download_bytes / s.logical_queries <= c.max_download_bytes
                && s.client
                    .cpu_ms
                    .is_some_and(|ms| ms / s.logical_queries as f64 <= 1000.0)
        });
    Ok(
        json!({"schema":"pir-total-work-v2", "config":c, "completed_logical_queries":completed,
        "workload": if c.candidate.starts_with("field-") {"equality-all-ids-and-padded-payload"} else {"known-row"},
        "security": {"private": !["field-public","field-postings","public","decoy"].contains(&c.candidate.as_str()),
            "logical_operators":if ["public","decoy"].contains(&c.candidate.as_str()) {1} else {2},"collusion_tolerance":if ["field-public","field-postings","public","decoy"].contains(&c.candidate.as_str()) {0} else {1},"deployment":"co-located sequential role simulation",
            "randomness":"seeded StdRng for reproducible benchmark only; seeds must never be public in deployment",
            "field_group_placement":"two noncolluding replicas per group; worker assignment is not a new privacy party"},
        "corpus_generation":corpus_cost, "global_server_build":build, "server_maintenance":maintenance,
        "client_setup":client_setup,"client_setup_max_cpu_ms":client_setup_max_cpu_ms,"setup_download_bytes":setup_download,"persistent_client_bytes":persistent_client_bytes,
        "aggregate_server_storage_bytes":(if ["public","decoy"].contains(&c.candidate.as_str()) {1} else {2})*(rows.len()+index_bytes+if c.candidate.starts_with("field-") {fields.len()*8} else {0}),"index_bytes_per_replica":index_bytes,
        "storage_excludes":"allocator/tree node overhead; source retained; role copies built twice but one retained for sequential simulation",
        "rebuild_count":rebuild_count,"finite_parameters":finite_parameters,
        "aggregate_server_cpu_ms":total_cpu,"server_cpu_ms_per_completed_query":total_cpu.map(|v| v/completed as f64),
        "client_measured_caps_pass":caps_pass, "eligibility":"microbenchmark only; transport, physical traffic, peak memory and energy gates unmeasured",
        "physical_dram_bytes":null,"gpu_active_ms":null,"energy_joules":null,"network_transport_cpu_ms":null,
        "client_peak_resident_bytes":null,"samples":samples,
        "workers_per_operator":c.workers,"cache_conditioning_bytes":c.cold_cache_bytes,
        "limitations":["No network latency or transport overhead is measured; bytes describe protocol payloads, each transfer counted once.",
            "Updates mutate payload bytes and perform full generation rebuilds; field membership changes and concurrent serving are not modeled.",
            "No server-side private intersection/compaction is implemented. field-bitmap reconstructs all group bitmaps at the client.",
            "CPU phase measurements include clock overhead; corpus generation and correctness oracle are excluded from server work.",
            "SinglePass setup includes a server table copy and client preprocessing; publication framing and durable state writes are excluded."]}),
    )
}

fn run_witness(c: &Config) -> Result<Value> {
    use crate::verification::{build_benchmark_witnesses, verify_nullifier_witness};
    ensure!(
        c.rows <= 4096,
        "canonical witness pilot is bounded to 4096 leaves"
    );
    let scalar = |v: u64| {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&v.to_le_bytes());
        bytes
    };
    let mut values = (1..=c.rows)
        .map(|i| scalar(i as u64 * 4))
        .collect::<Vec<_>>();
    let mut rng = StdRng::seed_from_u64(c.seed);
    let mut build = Cost::zero();
    let mut maintenance = Cost::zero();
    let mut samples = Vec::new();
    let mut root = [0u8; 32];
    let mut rows = Vec::new();
    let mut old_answer: Option<(Vec<u8>, [u8; 32])> = None;
    let mut stale_rejections = 0;
    let mut updates = 0;
    for iteration in 0..c.queries {
        if iteration == 0 || c.rebuild_every > 0 && iteration % c.rebuild_every == 0 {
            if iteration > 0 {
                // Remove one existing value and insert a fresh value. All paths
                // are rebuilt against the new canonical Poseidon root.
                values.remove(0);
                values.push(scalar((c.rows + iteration + 1) as u64 * 4));
                updates += 1;
            }
            let ((new_root, witnesses), cost) =
                measure(|| build_benchmark_witnesses(&values, true))?;
            if iteration == 0 {
                build.add(cost);
            } else {
                maintenance.add(cost);
            }
            root = new_root;
            let (snapshot, cost) = measure(|| {
                let data = witnesses
                    .into_iter()
                    .flat_map(|(_, row)| row)
                    .collect::<Vec<_>>();
                std::hint::black_box(data.clone());
                std::hint::black_box(data.clone());
                Ok(data)
            })?;
            rows = snapshot;
            if iteration == 0 {
                build.add(cost);
            } else {
                maintenance.add(cost);
            }
            if let Some((old, target)) = &old_answer {
                ensure!(
                    verify_nullifier_witness(target, old, &root).is_err(),
                    "stale witness accepted under current root"
                );
                stale_rejections += 1;
            }
        }
        let mut sample = Sample::new(1);
        let i = iteration % values.len();
        let (ordinal, target) = match iteration % 3 {
            0 => (i + 1, values[i]),
            1 => {
                let v = u64::from_le_bytes(values[i][..8].try_into().unwrap());
                (i + 1, scalar(v + 1))
            }
            _ => (0, scalar(1)),
        };
        let answer = retrieve(
            SnapshotView::new(&rows, values.len() + 1, 2008),
            ordinal,
            &mut rng,
            &mut sample,
        )?;
        let (_, cost) = measure(|| verify_nullifier_witness(&target, &answer, &root))?;
        sample.client.add(cost);
        sample.useful_bytes = 2008;
        old_answer = Some((answer, target));
        samples.push(sample);
    }
    let online: Option<f64> = samples.iter().map(|s| s.server.cpu_ms).sum();
    let total = online
        .zip(build.cpu_ms)
        .zip(maintenance.cpu_ms)
        .map(|((a, b), c)| a + b + c);
    Ok(
        json!({"schema":"pir-total-work-v2","config":c,"workload":"canonical-current-root-witness",
        "completed_logical_queries":samples.len(),"row_bytes":2008,"leaf_count":values.len(),
        "global_server_build":build,"server_maintenance":maintenance,"samples":samples,
        "aggregate_server_cpu_ms":total,"server_cpu_ms_per_completed_query":total.map(|v|v/c.queries as f64),
        "aggregate_server_storage_bytes":2*rows.len(),"stale_root_rejections":stale_rejections,"rebuild_count":updates,
        "security":{"private":true,"logical_operators":2,"collusion_tolerance":1},
        "verification":"canonical depth-20 arity-4 Poseidon paths: membership, predecessor/terminal absence, lower-sentinel absence; current root checked",
        "client_measured_caps_pass":samples.iter().all(|s|s.upload_bytes<=c.max_upload_bytes && s.download_bytes<=c.max_download_bytes && s.client.cpu_ms.is_some_and(|ms|ms<=1000.0)),
        "eligibility":"canonical local application benchmark; transport, client peak RSS and hardware counters are not measured here",
        "client_setup":{"cpu_ms":0.0,"wall_ms":0.0},"persistent_client_bytes":32,"setup_download_bytes":32,
        "physical_dram_bytes":null,"energy_joules":null}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_membership_absence_and_generation_invalidation() {
        let c = Config {
            candidate: "witness".into(),
            rows: 4,
            queries: 6,
            rebuild_every: 3,
            ..Config::default()
        };
        let report = run(&c).unwrap();
        assert_eq!(report["stale_root_rejections"], 1);
        assert_eq!(report["row_bytes"], 2008);
    }
    #[test]
    fn logical_reads_exclude_padding_bits_in_short_selectors() {
        let data = vec![42; 8];
        let mut rng = StdRng::seed_from_u64(4);
        let mut sample = Sample::new(1);
        assert_eq!(
            retrieve(SnapshotView::new(&data, 1, 8), 0, &mut rng, &mut sample).unwrap(),
            data
        );
        assert_eq!(sample.source_operand_bytes, 8);
    }
    #[test]
    fn complete_private_search_covers_absence_padding_and_partial_byte() {
        for bits in [1, 2, 4, 8] {
            let c = Config {
                candidate: "field-bitmap".into(),
                rows: 19,
                fanout: 3,
                payload_slots: 3,
                group_bits: bits,
                queries: 4,
                ..Config::default()
            };
            let report = run(&c).unwrap();
            assert_eq!(report["completed_logical_queries"], 4);
            let samples = report["samples"].as_array().unwrap();
            assert_eq!(samples[3]["useful_bytes"], 0);
            assert!(samples.iter().all(|s| s["payload_requests"] == 3));
            assert!(samples
                .windows(2)
                .all(|p| p[0]["download_bytes"] == p[1]["download_bytes"]));
        }
    }
    #[test]
    fn gates_reject_before_large_allocation() {
        assert!(run(&Config {
            rows: usize::MAX,
            ..Config::default()
        })
        .is_err());
        assert!(run(&Config {
            candidate: "field-bitmap".into(),
            max_resident_bytes: 1,
            ..Config::default()
        })
        .is_err());
        assert!(run(&Config {
            payload_slots: 1,
            ..Config::default()
        })
        .is_err());
    }
    #[test]
    fn adapters_reconstruct_and_count_rebuilds_and_batches() {
        for candidate in [
            "dense",
            "subset",
            "single-pass",
            "batch",
            "field-public",
            "field-inline",
            "field-postings",
            "finite-differences",
        ] {
            let c = Config {
                candidate: candidate.into(),
                rows: 32,
                queries: 4,
                rebuild_every: 2,
                ..Config::default()
            };
            let r = run(&c).unwrap();
            assert_eq!(r["rebuild_count"], 1);
            assert_eq!(
                r["completed_logical_queries"],
                if candidate == "batch" { 32 } else { 4 }
            );
        }
    }
}
