mod active_nullifier;
mod billion_tag;
pub mod cross_language;
mod ohttp;
mod use_cases;

pub use crate::profile::Profile;
pub use active_nullifier::run as run_active_nullifier;
pub use billion_tag::run as run_billion_tag;
pub use use_cases::{run as run_use_cases, SelectedUseCaseBenchmarkReport};

/// The default benchmark is the one authoritative three-use-case report.
pub fn run(profile: Profile) -> anyhow::Result<SelectedUseCaseBenchmarkReport> {
    run_use_cases(profile)
}

// Historical experiments remain opt-in. They are intentionally absent from
// the default binary and documentation so the POC does not look like twenty
// competing product choices.
#[cfg(feature = "research")]
pub mod accounting;
#[cfg(feature = "research")]
mod cold;
#[cfg(feature = "research")]
mod config;
#[cfg(feature = "research")]
mod cpu_snapshot;
#[cfg(feature = "research")]
mod dense_batch;
#[cfg(feature = "research")]
mod end_to_end;
#[cfg(feature = "research")]
mod endpoints;
#[cfg(feature = "research")]
mod fuse;
#[cfg(feature = "research")]
mod gpu_reference_decoy;
#[cfg(feature = "research")]
mod kernels;
#[cfg(feature = "research")]
mod local;
#[cfg(feature = "research")]
mod mphf;
#[cfg(feature = "research")]
mod mphf_subset_xor;
#[cfg(feature = "research")]
mod optimization;
#[cfg(feature = "research")]
mod perf_gate;
#[cfg(feature = "research")]
mod production_scale;
#[cfg(feature = "research")]
pub mod report;
#[cfg(feature = "research")]
mod ribbon;
#[cfg(feature = "research")]
mod single_pass;
#[cfg(feature = "research")]
mod subset_xor;
#[cfg(feature = "research")]
mod warm_stateful;

#[cfg(feature = "research")]
pub use cold::run as run_cold;
#[cfg(feature = "research")]
pub use cpu_snapshot::run as run_cpu_snapshot;
#[cfg(feature = "research")]
pub use dense_batch::run as run_dense_batch;
#[cfg(feature = "research")]
pub use end_to_end::run as run_end_to_end;
#[cfg(feature = "research")]
pub use endpoints::run as run_endpoints;
#[cfg(feature = "research")]
pub use fuse::run as run_fuse;
#[cfg(feature = "research")]
pub use gpu_reference_decoy::run as run_gpu_reference_decoy;
#[cfg(feature = "research")]
pub use mphf::run as run_mphf;
#[cfg(feature = "research")]
pub use mphf_subset_xor::run as run_mphf_subset_xor;
#[cfg(feature = "research")]
pub use optimization::run as run_optimizations;
#[cfg(feature = "research")]
pub use production_scale::{run_cli as run_production_scale, ProductionScaleReport};
#[cfg(feature = "research")]
pub use ribbon::run as run_ribbon;
#[cfg(feature = "research")]
pub use single_pass::run as run_single_pass;
#[cfg(feature = "research")]
pub use subset_xor::run as run_subset_xor;
#[cfg(feature = "research")]
pub use warm_stateful::run as run_warm_stateful;

#[cfg(feature = "research")]
pub(super) const SERVER_WORKER_THREADS: usize = 2;

#[cfg(feature = "research")]
pub(super) fn percentile(values: &[std::time::Duration], percentile: usize) -> std::time::Duration {
    let index = ((values.len() - 1) * percentile).div_ceil(100);
    values[index]
}

#[cfg(feature = "research")]
pub(super) fn millis(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(feature = "research")]
pub(super) fn micros(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}
