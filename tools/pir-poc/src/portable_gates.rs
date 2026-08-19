//! Portable-client budgets and production admission gates.
//!
//! These helpers do not change any protocol. They make the caller prove that
//! dimensions, state, and messages fit a declared client policy before it
//! allocates or evaluates them. Deterministic algorithmic work is intentionally
//! separate from optional measured CPU time.

use anyhow::{bail, Context, Result};
use serde::Serialize;

use crate::dense;
use crate::single_pass::GENERATION_ID_BYTES;

pub const CANONICAL_DOCUMENTS: usize = 1 << 20;
pub const CANONICAL_MPHF_ROWS: usize = 1 << 18;
pub const CANONICAL_ROW_BYTES: usize = 96;
pub const CANONICAL_TABLE_BYTES: usize = CANONICAL_MPHF_ROWS * CANONICAL_ROW_BYTES;
pub const PIR_SERVER_COUNT: usize = 2;
pub const LIVE_BUCKET_COUNT: usize = 1 << 22;
pub const LIVE_NOTIFICATION_SHARE_BYTES: usize = 16 + 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct PortableClientPolicy {
    pub max_persistent_bytes: usize,
    pub max_transient_bytes: usize,
    pub max_setup_download_bytes: usize,
    pub max_online_upload_bytes: usize,
    pub max_online_download_bytes: usize,
    pub max_setup_cpu_ms: u64,
    pub max_online_cpu_ms: u64,
    pub max_dense_batch_queries: usize,
    pub max_single_pass_partition_count: usize,
    pub max_live_subscriptions_per_server: usize,
    pub max_live_event_batch: usize,
}

impl PortableClientPolicy {
    /// Compatibility envelope, not a performance target.
    ///
    /// It is deliberately broad enough for a modern phone while remaining a
    /// useful denial-of-service boundary. A deployment should tighten it from
    /// measurements on the oldest supported device.
    pub const PHONE_COMPATIBILITY: Self = Self {
        max_persistent_bytes: 64 * 1024 * 1024,
        max_transient_bytes: 128 * 1024 * 1024,
        max_setup_download_bytes: 64 * 1024 * 1024,
        max_online_upload_bytes: 1024 * 1024,
        max_online_download_bytes: 1024 * 1024,
        max_setup_cpu_ms: 10_000,
        max_online_cpu_ms: 1_000,
        max_dense_batch_queries: 16,
        max_single_pass_partition_count: 32,
        max_live_subscriptions_per_server: 100_000,
        max_live_event_batch: 1_024,
    };
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct MeasuredClientCpu {
    pub setup_ms: Option<f64>,
    pub online_ms: Option<f64>,
    pub target: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    Passed,
    Failed,
    NotMeasured,
}

#[derive(Clone, Debug, Serialize)]
pub struct GateCheck {
    pub name: &'static str,
    pub status: GateStatus,
    pub observed: Option<f64>,
    pub limit: Option<f64>,
    pub unit: &'static str,
    pub evidence: &'static str,
    pub note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClientFeasibility {
    pub path: &'static str,
    pub persistent_bytes: usize,
    pub transient_bytes_upper_bound: usize,
    pub setup_download_bytes: usize,
    pub online_upload_bytes: usize,
    pub online_download_bytes: usize,
    pub deterministic_cpu_work: DeterministicClientCpuWork,
    pub measured_cpu: MeasuredClientCpu,
    pub gates: Vec<GateCheck>,
    pub phone_compatible_under_deterministic_budgets: bool,
    pub phone_cpu_status: GateStatus,
    pub note: &'static str,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct DeterministicClientCpuWork {
    pub public_index_lookups: usize,
    pub cryptographic_tree_levels: usize,
    pub random_bytes_generated: usize,
    pub selector_bytes_xored: usize,
    pub table_bytes_consumed_during_setup: usize,
    pub response_bytes_xored: usize,
    pub state_hint_byte_xor_updates: usize,
    pub permutation_positions_initialized: usize,
    pub note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct PortableFeasibilityReport {
    pub schema: &'static str,
    pub policy: PortableClientPolicy,
    pub cold_exact_mphf_dense: ClientFeasibility,
    pub warm_single_pass: ClientFeasibility,
    pub live_compact_dpf: ClientFeasibility,
    pub build_matrix: Vec<BuildGate>,
    pub production_readiness: Vec<ReadinessItem>,
    pub interpretation: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BuildGate {
    pub target: &'static str,
    pub client_architecture: &'static str,
    pub status: &'static str,
    pub command: &'static str,
    pub evidence: &'static str,
    pub limitation: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessStatus {
    ImplementedInPoc,
    GateImplementedNotExercised,
    RequiredForProduction,
    OutOfScope,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReadinessItem {
    pub area: &'static str,
    pub status: ReadinessStatus,
    pub requirement: &'static str,
    pub evidence_or_next_action: &'static str,
}

/// Construct the deterministic portability report for the canonical corpus.
///
/// `mphf_public_metadata_bytes` comes from the authenticated generation being
/// evaluated because PtrHash's serialized representation is build-specific.
/// Optional CPU measurements must come from the named target device; absence
/// stays `not_measured` and cannot accidentally pass the CPU gate.
pub fn canonical_report(
    mphf_public_metadata_bytes: usize,
    single_pass_partition_count: usize,
    cold_cpu: MeasuredClientCpu,
    warm_cpu: MeasuredClientCpu,
    live_cpu: MeasuredClientCpu,
) -> Result<PortableFeasibilityReport> {
    let policy = PortableClientPolicy::PHONE_COMPATIBILITY;
    Ok(PortableFeasibilityReport {
        schema: "pir-portable-gates-v1",
        policy,
        cold_exact_mphf_dense: cold_dense_feasibility(
            policy,
            mphf_public_metadata_bytes,
            cold_cpu,
        )?,
        warm_single_pass: warm_single_pass_feasibility(
            policy,
            mphf_public_metadata_bytes,
            single_pass_partition_count,
            warm_cpu,
        )?,
        live_compact_dpf: live_compact_dpf_feasibility(policy, live_cpu)?,
        build_matrix: build_matrix(),
        production_readiness: production_readiness(),
        interpretation: vec![
            "A build-only target pass proves type/dependency portability, not latency, memory RSS, thermal stability, battery use, background-execution behavior, or network reliability.",
            "Deterministic CPU work reports protocol-level bytes/levels/initializations. Millisecond gates remain not_measured until supplied from the named target device.",
            "The phone policy is a compatibility ceiling, not the optimization target. Desktop/server clients may use larger batches while every request still passes admission control.",
            "The complete pir-poc crate currently combines client, server, DefraDB, HTTP, and benchmark dependencies. A production mobile SDK must split the client-only surface before Android/iOS/WASI build results are meaningful.",
        ],
    })
}

fn cold_dense_feasibility(
    policy: PortableClientPolicy,
    metadata_bytes: usize,
    measured_cpu: MeasuredClientCpu,
) -> Result<ClientFeasibility> {
    let query_bytes = dense::query_size(CANONICAL_MPHF_ROWS);
    let upload = query_bytes
        .checked_mul(PIR_SERVER_COUNT)
        .context("Dense upload overflow")?;
    let download = CANONICAL_ROW_BYTES
        .checked_mul(PIR_SERVER_COUNT)
        .context("Dense download overflow")?;
    // Current query_shares materializes both shares simultaneously. Keep the
    // small response/combine allocation in the same conservative upper bound.
    let transient = upload
        .checked_add(download)
        .and_then(|bytes| bytes.checked_add(CANONICAL_ROW_BYTES))
        .context("Dense transient memory overflow")?;
    feasibility(
        "cold exact-MPHF Dense XOR",
        policy,
        metadata_bytes,
        transient,
        metadata_bytes,
        upload,
        download,
        DeterministicClientCpuWork {
            public_index_lookups: 1,
            cryptographic_tree_levels: 0,
            random_bytes_generated: query_bytes * (PIR_SERVER_COUNT - 1),
            selector_bytes_xored: query_bytes * (PIR_SERVER_COUNT - 1),
            table_bytes_consumed_during_setup: 0,
            response_bytes_xored: CANONICAL_ROW_BYTES * (PIR_SERVER_COUNT - 1),
            state_hint_byte_xor_updates: 0,
            permutation_positions_initialized: 0,
            note: "One MPHF ordinal, one random Dense share, one final selector XOR, and one 96-byte response combine. PtrHash lookup instructions are implementation-dependent and require target measurement.",
        },
        measured_cpu,
        "Stateless after loading the authenticated public MPHF artifact. The server scans the table; the client does not download it.",
    )
}

fn warm_single_pass_feasibility(
    policy: PortableClientPolicy,
    metadata_bytes: usize,
    partition_count: usize,
    measured_cpu: MeasuredClientCpu,
) -> Result<ClientFeasibility> {
    if !(2..=policy.max_single_pass_partition_count).contains(&partition_count) {
        bail!(
            "SinglePass partition count must be between 2 and {}",
            policy.max_single_pass_partition_count
        );
    }
    let partition_len = CANONICAL_MPHF_ROWS.div_ceil(partition_count);
    let hint_bytes = partition_len
        .checked_mul(CANONICAL_ROW_BYTES)
        .context("SinglePass hint size overflow")?;
    let permutation_bytes = CANONICAL_MPHF_ROWS
        .checked_mul(2 * size_of::<u32>())
        .context("SinglePass permutation size overflow")?;
    let persistent = metadata_bytes
        .checked_add(hint_bytes)
        .and_then(|bytes| bytes.checked_add(permutation_bytes))
        .and_then(|bytes| bytes.checked_add(GENERATION_ID_BYTES))
        .context("SinglePass persistent state overflow")?;
    // During setup the downloaded table and the resulting state may coexist.
    let transient = CANONICAL_TABLE_BYTES
        .checked_add(persistent)
        .context("SinglePass transient memory overflow")?;
    let setup_download = metadata_bytes
        .checked_add(CANONICAL_TABLE_BYTES)
        .context("SinglePass setup download overflow")?;
    let upload = partition_count
        .checked_mul(size_of::<u32>())
        .and_then(|bytes| bytes.checked_add(GENERATION_ID_BYTES))
        .and_then(|bytes| bytes.checked_mul(PIR_SERVER_COUNT))
        .context("SinglePass upload overflow")?;
    let download = partition_count
        .checked_mul(CANONICAL_ROW_BYTES)
        .and_then(|bytes| bytes.checked_add(GENERATION_ID_BYTES))
        .and_then(|bytes| bytes.checked_mul(PIR_SERVER_COUNT))
        .context("SinglePass download overflow")?;
    feasibility(
        "warm generation-bound SinglePass",
        policy,
        persistent,
        transient,
        setup_download,
        upload,
        download,
        DeterministicClientCpuWork {
            public_index_lookups: 1,
            cryptographic_tree_levels: 0,
            random_bytes_generated: 0,
            selector_bytes_xored: 0,
            table_bytes_consumed_during_setup: CANONICAL_TABLE_BYTES,
            response_bytes_xored: CANONICAL_ROW_BYTES * 4 * (partition_count - 1),
            state_hint_byte_xor_updates: 2 * (partition_count - 1) * CANONICAL_ROW_BYTES,
            permutation_positions_initialized: CANONICAL_MPHF_ROWS * 2,
            note: "Setup consumes every table byte and initializes forward/inverse permutations. Online byte-XOR count is a conservative reconstruction/delta/hint-update bound; RNG rejection sampling and PtrHash instructions need target measurement.",
        },
        measured_cpu,
        "The full authorized locator table may coexist with state during setup. A streaming mobile implementation can lower peak memory, but that optimization is not credited until implemented and measured.",
    )
}

fn live_compact_dpf_feasibility(
    policy: PortableClientPolicy,
    measured_cpu: MeasuredClientCpu,
) -> Result<ClientFeasibility> {
    let depth = LIVE_BUCKET_COUNT.trailing_zeros() as usize;
    let key_bytes_per_server = 16 + 16 + depth * 17 + 16;
    let registration_bytes = key_bytes_per_server
        .checked_mul(PIR_SERVER_COUNT)
        .context("Compact DPF registration overflow")?;
    let notification_bytes = LIVE_NOTIFICATION_SHARE_BYTES
        .checked_mul(PIR_SERVER_COUNT)
        .context("Compact DPF notification overflow")?;
    feasibility(
        "live two-server Compact DPF subscription",
        policy,
        0,
        registration_bytes + notification_bytes,
        0,
        registration_bytes,
        notification_bytes,
        DeterministicClientCpuWork {
            public_index_lookups: 0,
            cryptographic_tree_levels: depth,
            random_bytes_generated: 2 * 16 + 16,
            selector_bytes_xored: 0,
            table_bytes_consumed_during_setup: 0,
            response_bytes_xored: 16,
            state_hint_byte_xor_updates: 0,
            permutation_positions_initialized: 0,
            note: "Registration key generation constructs one depth-level two-party DPF; each notification combine XORs one 16-byte pair. Exact AES/PRG instructions require target measurement.",
        },
        measured_cpu,
        "Client persistent state excludes the application subscription descriptor/target, which is deployment data rather than DPF key state; server key state is not client RAM.",
    )
}

#[allow(clippy::too_many_arguments)]
fn feasibility(
    path: &'static str,
    policy: PortableClientPolicy,
    persistent: usize,
    transient: usize,
    setup_download: usize,
    online_upload: usize,
    online_download: usize,
    cpu_work: DeterministicClientCpuWork,
    measured_cpu: MeasuredClientCpu,
    note: &'static str,
) -> Result<ClientFeasibility> {
    let mut gates = vec![
        deterministic_gate(
            "persistent client memory",
            persistent,
            policy.max_persistent_bytes,
            "bytes",
            "algorithm dimensions and retained payload",
        ),
        deterministic_gate(
            "peak transient client memory upper bound",
            transient,
            policy.max_transient_bytes,
            "bytes",
            "conservative simultaneous owned payloads; allocator/RSS excluded",
        ),
        deterministic_gate(
            "setup download",
            setup_download,
            policy.max_setup_download_bytes,
            "bytes",
            "authenticated metadata/table wire payload; framing excluded",
        ),
        deterministic_gate(
            "online upload",
            online_upload,
            policy.max_online_upload_bytes,
            "bytes/query or registration",
            "protocol payload; framing excluded",
        ),
        deterministic_gate(
            "online download",
            online_download,
            policy.max_online_download_bytes,
            "bytes/query or event",
            "protocol payload including two live subscription IDs where applicable",
        ),
        measured_cpu_gate(
            "setup client CPU",
            measured_cpu.setup_ms,
            policy.max_setup_cpu_ms,
            measured_cpu.target,
        ),
        measured_cpu_gate(
            "online client CPU",
            measured_cpu.online_ms,
            policy.max_online_cpu_ms,
            measured_cpu.target,
        ),
    ];
    let deterministic_pass = gates[..5]
        .iter()
        .all(|gate| gate.status == GateStatus::Passed);
    let cpu_status = if gates[5..]
        .iter()
        .any(|gate| gate.status == GateStatus::Failed)
    {
        GateStatus::Failed
    } else if gates[5..]
        .iter()
        .all(|gate| gate.status == GateStatus::Passed)
    {
        GateStatus::Passed
    } else {
        GateStatus::NotMeasured
    };
    // Preserve stable order after evaluating the slices above.
    gates.shrink_to_fit();
    Ok(ClientFeasibility {
        path,
        persistent_bytes: persistent,
        transient_bytes_upper_bound: transient,
        setup_download_bytes: setup_download,
        online_upload_bytes: online_upload,
        online_download_bytes: online_download,
        deterministic_cpu_work: cpu_work,
        measured_cpu,
        gates,
        phone_compatible_under_deterministic_budgets: deterministic_pass,
        phone_cpu_status: cpu_status,
        note,
    })
}

fn deterministic_gate(
    name: &'static str,
    observed: usize,
    limit: usize,
    unit: &'static str,
    note: &'static str,
) -> GateCheck {
    GateCheck {
        name,
        status: if observed <= limit {
            GateStatus::Passed
        } else {
            GateStatus::Failed
        },
        observed: Some(observed as f64),
        limit: Some(limit as f64),
        unit,
        evidence: "deterministic",
        note,
    }
}

fn measured_cpu_gate(
    name: &'static str,
    observed: Option<f64>,
    limit_ms: u64,
    target: Option<&'static str>,
) -> GateCheck {
    let valid = if target.is_some() {
        observed.filter(|value| value.is_finite() && *value >= 0.0)
    } else {
        None
    };
    GateCheck {
        name,
        status: valid.map_or(GateStatus::NotMeasured, |value| {
            if value <= limit_ms as f64 {
                GateStatus::Passed
            } else {
                GateStatus::Failed
            }
        }),
        observed: valid,
        limit: Some(limit_ms as f64),
        unit: "milliseconds",
        evidence: if valid.is_some() {
            "measured on named target"
        } else {
            "not measured"
        },
        note: target.unwrap_or(
            "No target device was named; a desktop/host timing must not be relabelled as mobile.",
        ),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColdDenseAdmission {
    pub row_count: usize,
    pub row_size: usize,
    pub server_count: usize,
    pub batch_queries: usize,
    pub query_share_bytes: usize,
}

pub fn admit_cold_dense(policy: PortableClientPolicy, request: ColdDenseAdmission) -> Result<()> {
    if request.row_count == 0 || request.row_size == 0 {
        bail!("Dense table dimensions must be non-zero");
    }
    if request.server_count < 2 {
        bail!("Dense XOR requires at least two servers");
    }
    if request.batch_queries == 0 || request.batch_queries > policy.max_dense_batch_queries {
        bail!("Dense batch exceeds the portable admission limit");
    }
    let expected_query_bytes = dense::query_size(request.row_count);
    if request.query_share_bytes != expected_query_bytes {
        bail!("Dense query share has the wrong encoded size");
    }
    let upload = expected_query_bytes
        .checked_mul(request.server_count)
        .and_then(|bytes| bytes.checked_mul(request.batch_queries))
        .context("Dense admitted upload overflow")?;
    let download = request
        .row_size
        .checked_mul(request.server_count)
        .and_then(|bytes| bytes.checked_mul(request.batch_queries))
        .context("Dense admitted download overflow")?;
    let transient = upload
        .checked_add(download)
        .context("Dense admitted transient size overflow")?;
    if upload > policy.max_online_upload_bytes || download > policy.max_online_download_bytes {
        bail!("Dense request exceeds portable online byte limits");
    }
    if transient > policy.max_transient_bytes {
        bail!("Dense request exceeds the portable transient memory limit");
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SinglePassAdmission {
    pub row_count: usize,
    pub row_size: usize,
    pub partition_count: usize,
    pub public_metadata_bytes: usize,
}

pub fn admit_single_pass(policy: PortableClientPolicy, request: SinglePassAdmission) -> Result<()> {
    if request.row_count == 0 || request.row_size == 0 {
        bail!("SinglePass table dimensions must be non-zero");
    }
    if !(2..=policy.max_single_pass_partition_count).contains(&request.partition_count) {
        bail!("SinglePass partition count exceeds the portable admission limit");
    }
    let table_bytes = request
        .row_count
        .checked_mul(request.row_size)
        .context("SinglePass admitted table size overflow")?;
    let setup = table_bytes
        .checked_add(request.public_metadata_bytes)
        .context("SinglePass admitted setup size overflow")?;
    let partition_len = request.row_count.div_ceil(request.partition_count);
    let permutation_bytes = request
        .row_count
        .checked_mul(2 * size_of::<u32>())
        .context("SinglePass admitted permutation size overflow")?;
    let metadata_and_generation = request
        .public_metadata_bytes
        .checked_add(GENERATION_ID_BYTES)
        .context("SinglePass admitted metadata size overflow")?;
    let state = partition_len
        .checked_mul(request.row_size)
        .and_then(|bytes| bytes.checked_add(permutation_bytes))
        .and_then(|bytes| bytes.checked_add(metadata_and_generation))
        .context("SinglePass admitted state size overflow")?;
    let transient = table_bytes
        .checked_add(state)
        .context("SinglePass admitted transient size overflow")?;
    let online_upload = request
        .partition_count
        .checked_mul(size_of::<u32>())
        .and_then(|bytes| bytes.checked_add(GENERATION_ID_BYTES))
        .and_then(|bytes| bytes.checked_mul(PIR_SERVER_COUNT))
        .context("SinglePass admitted online upload overflow")?;
    let response_bytes_per_server = request
        .partition_count
        .checked_mul(request.row_size)
        .and_then(|bytes| bytes.checked_add(GENERATION_ID_BYTES))
        .context("SinglePass admitted response size overflow")?;
    let online_download = response_bytes_per_server
        .checked_mul(PIR_SERVER_COUNT)
        .context("SinglePass admitted online download overflow")?;
    if setup > policy.max_setup_download_bytes
        || state > policy.max_persistent_bytes
        || transient > policy.max_transient_bytes
        || online_upload > policy.max_online_upload_bytes
        || online_download > policy.max_online_download_bytes
    {
        bail!("SinglePass setup, state, transient memory, or online payload exceeds the portable admission limit");
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveAdmission {
    pub current_subscriptions: usize,
    pub event_batch: usize,
    pub bucket_count: usize,
    pub encoded_key_bytes: usize,
}

pub fn admit_live_compact_dpf(policy: PortableClientPolicy, request: LiveAdmission) -> Result<()> {
    if !request.bucket_count.is_power_of_two() || request.bucket_count < 4 {
        bail!("Compact DPF bucket domain is invalid");
    }
    if request.current_subscriptions >= policy.max_live_subscriptions_per_server {
        bail!("Compact DPF subscription admission limit reached");
    }
    if request.event_batch == 0 || request.event_batch > policy.max_live_event_batch {
        bail!("Compact DPF event batch exceeds the admission limit");
    }
    let depth = request.bucket_count.trailing_zeros() as usize;
    let expected_key_bytes = 16 + 16 + depth * 17 + 16;
    if request.encoded_key_bytes != expected_key_bytes {
        bail!("Compact DPF key has the wrong size for its domain");
    }
    let outputs = request
        .current_subscriptions
        .checked_add(1)
        .and_then(|subscriptions| subscriptions.checked_mul(request.event_batch))
        .and_then(|outputs| outputs.checked_mul(LIVE_NOTIFICATION_SHARE_BYTES))
        .context("Compact DPF admitted output size overflow")?;
    if outputs > policy.max_transient_bytes {
        bail!("Compact DPF output batch exceeds the transient memory limit");
    }
    Ok(())
}

fn build_matrix() -> Vec<BuildGate> {
    vec![
        BuildGate {
            target: "x86_64-unknown-linux-gnu",
            client_architecture: "Linux desktop/server host",
            status: "exercised by normal WSL checks; rerun portable script for the current commit",
            command: "cargo check -p pir-poc --lib --target x86_64-unknown-linux-gnu",
            evidence: "build-only",
            limitation: "Does not exercise ARM, a mobile OS, client-only dependency separation, or performance.",
        },
        BuildGate {
            target: "x86_64-pc-windows-msvc",
            client_architecture: "Windows desktop host",
            status: "failed with pinned Rust 1.91: sha2-asm 0.6.4 passes sha512_x64.S to MSVC cl, which ignores it; link fails LNK1181 for the missing object",
            command: "cargo check -p pir-poc --lib --target x86_64-pc-windows-msvc",
            evidence: "exercised build-only failure",
            limitation: "Must be green before claiming Windows support; still not a phone result.",
        },
        BuildGate {
            target: "wasm32-wasip1",
            client_architecture: "portable WASI build proxy",
            status: "failed on this commit: transitive sucds 0.8.3 requires target_pointer_width=64, while wasm32-wasip1 is 32-bit",
            command: "cargo check -p pir-poc --lib --target wasm32-wasip1",
            evidence: "exercised build-only failure",
            limitation: "The monolithic crate pulls this server/index dependency into the client check. Split a client-only crate; the failure is not evidence that client-side PIR algebra cannot run on WASI.",
        },
        BuildGate {
            target: "aarch64-linux-android",
            client_architecture: "64-bit Android",
            status: "target/NDK not installed in the current environment",
            command: "cargo check -p pir-poc-client --target aarch64-linux-android",
            evidence: "not exercised",
            limitation: "Requires a client-only crate, pinned Android NDK, linker configuration, device tests, and an ARM phone benchmark.",
        },
        BuildGate {
            target: "aarch64-apple-ios",
            client_architecture: "64-bit iOS",
            status: "not available from the current Windows/WSL host",
            command: "cargo check -p pir-poc-client --target aarch64-apple-ios",
            evidence: "not exercised",
            limitation: "Requires a macOS/Xcode runner, client-only crate, device tests, and an iPhone benchmark.",
        },
    ]
}

fn production_readiness() -> Vec<ReadinessItem> {
    use ReadinessStatus::{
        GateImplementedNotExercised, ImplementedInPoc, OutOfScope, RequiredForProduction,
    };
    vec![
        ReadinessItem {
            area: "client/server package boundary",
            status: RequiredForProduction,
            requirement: "Publish a client-only Rust crate with no DefraDB node, Axum, server evaluator, Rayon server pool, or native storage dependency.",
            evidence_or_next_action: "The current monolithic pir-poc crate makes cross-target failures ambiguous; split the authenticated manifest/index, query generation, state, and combine APIs first.",
        },
        ReadinessItem {
            area: "authenticated immutable generation",
            status: ImplementedInPoc,
            requirement: "Bind MPHF metadata, rows, and mutable SinglePass state to one authenticated generation and reject stale state before mutation.",
            evidence_or_next_action: "MPHF artifacts carry digests/generation; SinglePass core state, queries, and answers are generation-bound and reject mismatches before state mutation.",
        },
        ReadinessItem {
            area: "malformed input rejection",
            status: ImplementedInPoc,
            requirement: "Reject wrong Dense share lengths, corrupt/truncated MPHF artifacts, invalid SinglePass dimensions/answers, and malformed/wrong-party/wrong-domain DPF keys.",
            evidence_or_next_action: "Protocol tests plus portable gate tests cover these shapes; add coverage-guided fuzzing and structured error metrics.",
        },
        ReadinessItem {
            area: "admission control",
            status: GateImplementedNotExercised,
            requirement: "Bound batch size, transient output, query/key shape, SinglePass Q/state/table size, live subscriptions, event batches, queue dwell, and per-principal rate.",
            evidence_or_next_action: "Pure deterministic gates are implemented; enforce them before allocation/queueing in HTTP and event sidecars, then load-test rejection paths.",
        },
        ReadinessItem {
            area: "Android/iOS/WASI builds",
            status: GateImplementedNotExercised,
            requirement: "Run pinned cross-builds for every supported target and archive compiler/dependency evidence in CI.",
            evidence_or_next_action: "Portable scripts enumerate installed targets and fail on build errors. Android/iOS require dedicated toolchains/runners and the client-only split.",
        },
        ReadinessItem {
            area: "real mobile resource measurement",
            status: RequiredForProduction,
            requirement: "Measure cold/warm/live client CPU, peak RSS, allocations, battery/energy, thermal throttling, and network on the oldest supported ARM phone.",
            evidence_or_next_action: "Inject named-device setup/online milliseconds into canonical_report; missing values remain not_measured and cannot pass.",
        },
        ReadinessItem {
            area: "cryptographic review and constant time",
            status: RequiredForProduction,
            requirement: "Use audited implementations, stable safe MPHF serialization, reviewed randomness, secret erasure where relevant, and constant-time DPF primitives.",
            evidence_or_next_action: "Current PtrHash epserde and fss-rs are research POC dependencies; pin/review or replace before production.",
        },
        ReadinessItem {
            area: "SinglePass crash consistency",
            status: RequiredForProduction,
            requirement: "Persist state atomically, serialize one in-flight query, and discard/recover state after ambiguous failures without rollback reuse.",
            evidence_or_next_action: "The core rejects concurrent in-flight queries; durable journal/recovery and failure-injection tests are still required.",
        },
        ReadinessItem {
            area: "live delivery privacy",
            status: RequiredForProduction,
            requirement: "Pad notification identifiers/shares and release on a fixed schedule, or prove a private aggregation replacement.",
            evidence_or_next_action: "Compact DPF hides the subscribed point from either server but unpadded delivery leaks match count/timing.",
        },
        ReadinessItem {
            area: "malicious servers and Byzantine availability",
            status: OutOfScope,
            requirement: "Add verifiable/committed PIR or authenticated result proofs and a proven threshold construction before claiming malicious robustness.",
            evidence_or_next_action: "Current selected paths are semi-honest and require both answers; extra replicas alone do not change the proof.",
        },
        ReadinessItem {
            area: "authorization cohort",
            status: RequiredForProduction,
            requirement: "Ensure a SinglePass client is authorized to download every locator/projection byte in its table and prevent cross-cohort generation reuse.",
            evidence_or_next_action: "If non-result locators are confidential to the client, use Dense/another data-private path instead of SinglePass full-table setup.",
        },
        ReadinessItem {
            area: "observability without query leakage",
            status: RequiredForProduction,
            requirement: "Record aggregate work, queue/admission failures, generation skew, drops, and resource saturation without logging selectors, tags, keys, or results.",
            evidence_or_next_action: "Adopt pir-aggregate-work-v1 counters and privacy-reviewed structured logging.",
        },
    ]
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;
    use crate::single_pass::ClientState;
    use crate::snapshot::Snapshot;
    use crate::subscription::{compact_registration, CompactSubscriptionServer};

    const TEST_METADATA_BYTES: usize = 98_534;

    #[test]
    fn canonical_paths_fit_deterministic_phone_envelope_without_faking_cpu() {
        let report = canonical_report(
            TEST_METADATA_BYTES,
            2,
            MeasuredClientCpu::default(),
            MeasuredClientCpu::default(),
            MeasuredClientCpu::default(),
        )
        .unwrap();
        for path in [
            &report.cold_exact_mphf_dense,
            &report.warm_single_pass,
            &report.live_compact_dpf,
        ] {
            assert!(path.phone_compatible_under_deterministic_budgets);
            assert_eq!(path.phone_cpu_status, GateStatus::NotMeasured);
        }
        assert_eq!(report.cold_exact_mphf_dense.online_upload_bytes, 65_536);
        assert_eq!(report.cold_exact_mphf_dense.online_download_bytes, 192);
        assert_eq!(
            report.warm_single_pass.setup_download_bytes,
            CANONICAL_TABLE_BYTES + TEST_METADATA_BYTES
        );
        assert_eq!(report.warm_single_pass.online_upload_bytes, 80);
        assert_eq!(report.warm_single_pass.online_download_bytes, 448);
        assert_eq!(report.live_compact_dpf.online_upload_bytes, 844);
        assert_eq!(report.live_compact_dpf.online_download_bytes, 64);
    }

    #[test]
    fn cpu_gate_requires_a_named_valid_measurement() {
        let report = canonical_report(
            TEST_METADATA_BYTES,
            2,
            MeasuredClientCpu {
                setup_ms: Some(1.0),
                online_ms: Some(50.0),
                target: Some("test-arm64-phone"),
            },
            MeasuredClientCpu::default(),
            MeasuredClientCpu::default(),
        )
        .unwrap();
        assert_eq!(
            report.cold_exact_mphf_dense.phone_cpu_status,
            GateStatus::Passed
        );
        assert_eq!(
            report.warm_single_pass.phone_cpu_status,
            GateStatus::NotMeasured
        );
    }

    #[test]
    fn dense_and_single_pass_admission_reject_malformed_or_oversized_requests() {
        let policy = PortableClientPolicy::PHONE_COMPATIBILITY;
        assert!(admit_cold_dense(
            policy,
            ColdDenseAdmission {
                row_count: CANONICAL_MPHF_ROWS,
                row_size: CANONICAL_ROW_BYTES,
                server_count: 2,
                batch_queries: 1,
                query_share_bytes: dense::query_size(CANONICAL_MPHF_ROWS),
            }
        )
        .is_ok());
        assert!(admit_cold_dense(
            policy,
            ColdDenseAdmission {
                row_count: CANONICAL_MPHF_ROWS,
                row_size: CANONICAL_ROW_BYTES,
                server_count: 2,
                batch_queries: policy.max_dense_batch_queries + 1,
                query_share_bytes: dense::query_size(CANONICAL_MPHF_ROWS),
            }
        )
        .is_err());
        assert!(admit_cold_dense(
            policy,
            ColdDenseAdmission {
                row_count: CANONICAL_MPHF_ROWS,
                row_size: CANONICAL_ROW_BYTES,
                server_count: 2,
                batch_queries: 1,
                query_share_bytes: 7,
            }
        )
        .is_err());
        assert!(admit_single_pass(
            policy,
            SinglePassAdmission {
                row_count: CANONICAL_MPHF_ROWS,
                row_size: CANONICAL_ROW_BYTES,
                partition_count: 2,
                public_metadata_bytes: TEST_METADATA_BYTES,
            }
        )
        .is_ok());
        assert!(admit_single_pass(
            policy,
            SinglePassAdmission {
                row_count: CANONICAL_MPHF_ROWS,
                row_size: CANONICAL_ROW_BYTES,
                partition_count: 1,
                public_metadata_bytes: TEST_METADATA_BYTES,
            }
        )
        .is_err());
    }

    #[test]
    fn core_protocols_reject_malformed_dense_and_single_pass_shapes() {
        let snapshot = Snapshot::benchmark(64, 96, 3).unwrap();
        assert!(crate::dense::answer(snapshot.view(), &[0u8; 7]).is_err());
        assert!(ClientState::setup(
            snapshot.view(),
            snapshot.manifest.generation_id().unwrap(),
            1,
            &mut StdRng::seed_from_u64(4),
        )
        .is_err());
    }

    #[test]
    fn compact_dpf_rejects_malformed_keys_and_admission_overflow() {
        let bucket_count = LIVE_BUCKET_COUNT;
        let registration =
            compact_registration(123, bucket_count, &mut StdRng::seed_from_u64(5)).unwrap();
        let policy = PortableClientPolicy::PHONE_COMPATIBILITY;
        assert!(admit_live_compact_dpf(
            policy,
            LiveAdmission {
                current_subscriptions: 0,
                event_batch: 1,
                bucket_count,
                encoded_key_bytes: registration.server_keys[0].len(),
            }
        )
        .is_ok());
        assert!(admit_live_compact_dpf(
            policy,
            LiveAdmission {
                current_subscriptions: policy.max_live_subscriptions_per_server,
                event_batch: 1,
                bucket_count,
                encoded_key_bytes: registration.server_keys[0].len(),
            }
        )
        .is_err());
        assert!(admit_live_compact_dpf(
            policy,
            LiveAdmission {
                current_subscriptions: 0,
                event_batch: policy.max_live_event_batch + 1,
                bucket_count,
                encoded_key_bytes: registration.server_keys[0].len(),
            }
        )
        .is_err());

        let mut server = CompactSubscriptionServer::new(0, bucket_count).unwrap();
        let mut bad_magic = registration.server_keys[0].clone();
        bad_magic[0] ^= 0xff;
        assert!(server.register(registration.id, &bad_magic).is_err());
        let mut bad_party = registration.server_keys[0].clone();
        bad_party[4] = 1;
        assert!(server.register(registration.id, &bad_party).is_err());
        let truncated = &registration.server_keys[0][..registration.server_keys[0].len() - 1];
        assert!(server.register(registration.id, truncated).is_err());
        let mut bad_flags = registration.server_keys[0].clone();
        bad_flags[6] = 1;
        assert!(server.register(registration.id, &bad_flags).is_err());
        let mut bad_domain = registration.server_keys[0].clone();
        bad_domain[15] ^= 1;
        assert!(server.register(registration.id, &bad_domain).is_err());
    }
}
