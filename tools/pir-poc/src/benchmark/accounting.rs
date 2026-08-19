use std::fmt;

use serde::Serialize;

pub const SCHEMA_VERSION: &str = "pir-aggregate-work-v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Evidence {
    Measured,
    Deterministic,
    Estimated,
    NotMeasured,
    NotApplicable,
}

#[derive(Clone, Debug, Serialize)]
pub struct Metric<T> {
    pub value: Option<T>,
    pub evidence: Evidence,
    pub note: &'static str,
}

impl<T> Metric<T> {
    pub fn measured(value: T, note: &'static str) -> Self {
        Self {
            value: Some(value),
            evidence: Evidence::Measured,
            note,
        }
    }

    pub fn deterministic(value: T, note: &'static str) -> Self {
        Self {
            value: Some(value),
            evidence: Evidence::Deterministic,
            note,
        }
    }

    pub fn estimated(value: T, note: &'static str) -> Self {
        Self {
            value: Some(value),
            evidence: Evidence::Estimated,
            note,
        }
    }

    pub fn not_measured(note: &'static str) -> Self {
        Self {
            value: None,
            evidence: Evidence::NotMeasured,
            note,
        }
    }

    pub fn not_applicable(note: &'static str) -> Self {
        Self {
            value: None,
            evidence: Evidence::NotApplicable,
            note,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "class")]
pub enum LeakageScope {
    ExactQueryPrivacy,
    CandidateSet { candidates: usize },
    PublicQuery,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ComparisonScope {
    pub workload: &'static str,
    pub result: &'static str,
    pub public_partition: &'static str,
    pub leakage: LeakageScope,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SecurityLabels {
    pub privacy: &'static str,
    pub server_count: usize,
    pub collusion_tolerance: usize,
    pub required_answers: usize,
    pub assumptions: &'static str,
    pub availability: &'static str,
    pub integrity: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct PhaseWork {
    pub unit: &'static str,
    pub aggregate_server_time_ms: Metric<f64>,
    pub client_time_ms: Metric<f64>,
    pub logical_selected_bytes: Metric<usize>,
    pub physical_or_scanned_bytes: Metric<usize>,
    pub peak_server_ram_bytes: Metric<usize>,
    pub peak_client_ram_bytes: Metric<usize>,
    pub client_upload_bytes: Metric<usize>,
    pub client_download_bytes: Metric<usize>,
    pub server_scans: Metric<usize>,
    pub network_rounds: Metric<usize>,
}

impl PhaseWork {
    pub fn not_applicable(unit: &'static str, note: &'static str) -> Self {
        Self {
            unit,
            aggregate_server_time_ms: Metric::not_applicable(note),
            client_time_ms: Metric::not_applicable(note),
            logical_selected_bytes: Metric::not_applicable(note),
            physical_or_scanned_bytes: Metric::not_applicable(note),
            peak_server_ram_bytes: Metric::not_applicable(note),
            peak_client_ram_bytes: Metric::not_applicable(note),
            client_upload_bytes: Metric::not_applicable(note),
            client_download_bytes: Metric::not_applicable(note),
            server_scans: Metric::not_applicable(note),
            network_rounds: Metric::not_applicable(note),
        }
    }

    pub fn unmeasured(unit: &'static str, note: &'static str) -> Self {
        Self {
            unit,
            aggregate_server_time_ms: Metric::not_measured(note),
            client_time_ms: Metric::not_measured(note),
            logical_selected_bytes: Metric::not_measured(note),
            physical_or_scanned_bytes: Metric::not_measured(note),
            peak_server_ram_bytes: Metric::not_measured(note),
            peak_client_ram_bytes: Metric::not_measured(note),
            client_upload_bytes: Metric::not_measured(note),
            client_download_bytes: Metric::not_measured(note),
            server_scans: Metric::not_measured(note),
            network_rounds: Metric::not_measured(note),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct PerServerOnlineWork {
    pub server_index: usize,
    pub server_time_p50_ms: Metric<f64>,
    pub logical_selected_bytes: Metric<usize>,
    pub physical_or_scanned_bytes: Metric<usize>,
    pub scans: Metric<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OnlineWork {
    pub unit: &'static str,
    pub per_server: Vec<PerServerOnlineWork>,
    pub aggregate_server_time_p50_ms: Metric<f64>,
    pub max_server_time_p50_ms: Metric<f64>,
    pub aggregate_logical_selected_bytes: Metric<usize>,
    pub aggregate_physical_or_scanned_bytes: Metric<usize>,
    pub server_scans: Metric<usize>,
    pub network_rounds: Metric<usize>,
    pub useful_result_bytes: Metric<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClientWork {
    pub online_cpu_p50_ms: Metric<f64>,
    pub peak_transient_ram_bytes: Metric<usize>,
    pub persistent_state_bytes: Metric<usize>,
    pub upload_bytes: Metric<usize>,
    pub download_bytes: Metric<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PersistedStorage {
    pub server_bytes_per_server: Metric<usize>,
    pub aggregate_server_bytes: Metric<usize>,
    pub client_bytes: Metric<usize>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AmortizationHorizon {
    pub global_build: &'static str,
    pub per_client_setup: &'static str,
    pub maintenance: &'static str,
    pub assumed_global_queries: Option<usize>,
    pub assumed_queries_per_client_setup: Option<usize>,
    pub assumed_online_events_per_maintenance: Option<usize>,
    pub note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct HardwareCounterStatus {
    pub adapter: &'static str,
    pub physical_bytes: &'static str,
    pub cpu_energy: &'static str,
    pub dram_energy: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct AggregateWorkReport {
    pub schema: &'static str,
    pub protocol: &'static str,
    pub comparison_scope: ComparisonScope,
    pub security: SecurityLabels,
    pub global_build: PhaseWork,
    pub per_client_setup: PhaseWork,
    pub online: OnlineWork,
    pub maintenance: PhaseWork,
    pub client: ClientWork,
    pub persisted_storage: PersistedStorage,
    pub amortization: AmortizationHorizon,
    pub hardware_counters: HardwareCounterStatus,
}

impl AggregateWorkReport {
    pub fn new(
        protocol: &'static str,
        comparison_scope: ComparisonScope,
        security: SecurityLabels,
    ) -> Self {
        let per_server = (0..security.server_count)
            .map(|server_index| PerServerOnlineWork {
                server_index,
                server_time_p50_ms: Metric::not_measured(
                    "per-server time was not recorded for this benchmark",
                ),
                logical_selected_bytes: Metric::not_measured(
                    "logical selected bytes were not recorded",
                ),
                physical_or_scanned_bytes: Metric::not_measured(
                    "hardware byte counters were not collected",
                ),
                scans: Metric::not_measured("server scan count was not recorded"),
            })
            .collect();
        Self {
            schema: SCHEMA_VERSION,
            protocol,
            comparison_scope,
            security,
            global_build: PhaseWork::unmeasured(
                "snapshot build",
                "global build cost was not recorded",
            ),
            per_client_setup: PhaseWork::not_applicable(
                "client setup",
                "protocol has no per-client setup phase",
            ),
            online: OnlineWork {
                unit: "logical operation",
                per_server,
                aggregate_server_time_p50_ms: Metric::not_measured(
                    "aggregate server time was not recorded",
                ),
                max_server_time_p50_ms: Metric::not_measured(
                    "maximum server time was not recorded",
                ),
                aggregate_logical_selected_bytes: Metric::not_measured(
                    "logical selected bytes were not recorded",
                ),
                aggregate_physical_or_scanned_bytes: Metric::not_measured(
                    "hardware byte counters were not collected",
                ),
                server_scans: Metric::not_measured("server scan count was not recorded"),
                network_rounds: Metric::not_measured("network rounds were not recorded"),
                useful_result_bytes: Metric::not_measured("useful result size was not recorded"),
            },
            maintenance: PhaseWork::unmeasured(
                "maintenance operation",
                "maintenance cost was not recorded",
            ),
            client: ClientWork {
                online_cpu_p50_ms: Metric::not_measured("client online CPU was not recorded"),
                peak_transient_ram_bytes: Metric::not_measured(
                    "client peak transient RAM was not measured",
                ),
                persistent_state_bytes: Metric::not_measured(
                    "client persistent state was not recorded",
                ),
                upload_bytes: Metric::not_measured("client upload was not recorded"),
                download_bytes: Metric::not_measured("client download was not recorded"),
            },
            persisted_storage: PersistedStorage {
                server_bytes_per_server: Metric::not_measured(
                    "per-server persisted storage was not recorded",
                ),
                aggregate_server_bytes: Metric::not_measured(
                    "aggregate server persisted storage was not recorded",
                ),
                client_bytes: Metric::not_measured("client persisted storage was not recorded"),
            },
            amortization: AmortizationHorizon {
                global_build: "all operations served by one immutable snapshot",
                per_client_setup: "all operations by one client before state refresh",
                maintenance: "operations between snapshot or state updates",
                assumed_global_queries: None,
                assumed_queries_per_client_setup: None,
                assumed_online_events_per_maintenance: None,
                note: "No amortization denominator is assumed; reports expose phase costs separately so deployments can apply their own horizons.",
            },
            hardware_counters: unavailable_hardware_counters(),
        }
    }

    pub fn validate(&self) -> Result<(), AccountingError> {
        if self.security.server_count == 0 {
            return Err(AccountingError::NoServers);
        }
        if self.online.per_server.len() != self.security.server_count {
            return Err(AccountingError::ServerCount {
                declared: self.security.server_count,
                recorded: self.online.per_server.len(),
            });
        }
        for (position, server) in self.online.per_server.iter().enumerate() {
            if server.server_index != position {
                return Err(AccountingError::ServerIndex {
                    position,
                    recorded: server.server_index,
                });
            }
        }
        if self.security.collusion_tolerance >= self.security.server_count {
            return Err(AccountingError::CollusionTolerance {
                tolerance: self.security.collusion_tolerance,
                server_count: self.security.server_count,
            });
        }
        if self.security.required_answers == 0
            || self.security.required_answers > self.security.server_count
        {
            return Err(AccountingError::RequiredAnswers {
                required: self.security.required_answers,
                server_count: self.security.server_count,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonMismatch {
    Workload,
    Result,
    PublicPartition,
    Leakage,
    SecurityModel,
    InvalidValues,
}

#[derive(Clone, Debug, Serialize)]
pub struct DirectComparison {
    pub label: &'static str,
    pub directly_comparable: bool,
    pub candidate_over_baseline: Option<f64>,
    pub blocked_by: Vec<ComparisonMismatch>,
}

pub fn direct_ratio(
    label: &'static str,
    baseline: &AggregateWorkReport,
    candidate: &AggregateWorkReport,
    baseline_value: f64,
    candidate_value: f64,
) -> DirectComparison {
    let mut blocked_by = Vec::new();
    if baseline.comparison_scope.workload != candidate.comparison_scope.workload {
        blocked_by.push(ComparisonMismatch::Workload);
    }
    if baseline.comparison_scope.result != candidate.comparison_scope.result {
        blocked_by.push(ComparisonMismatch::Result);
    }
    if baseline.comparison_scope.public_partition != candidate.comparison_scope.public_partition {
        blocked_by.push(ComparisonMismatch::PublicPartition);
    }
    if baseline.comparison_scope.leakage != candidate.comparison_scope.leakage {
        blocked_by.push(ComparisonMismatch::Leakage);
    }
    if baseline.security != candidate.security {
        blocked_by.push(ComparisonMismatch::SecurityModel);
    }
    if !baseline_value.is_finite()
        || baseline_value <= 0.0
        || !candidate_value.is_finite()
        || candidate_value < 0.0
    {
        blocked_by.push(ComparisonMismatch::InvalidValues);
    }
    let directly_comparable = blocked_by.is_empty();
    DirectComparison {
        label,
        directly_comparable,
        candidate_over_baseline: directly_comparable.then_some(candidate_value / baseline_value),
        blocked_by,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AccountingError {
    NoServers,
    ServerCount {
        declared: usize,
        recorded: usize,
    },
    ServerIndex {
        position: usize,
        recorded: usize,
    },
    CollusionTolerance {
        tolerance: usize,
        server_count: usize,
    },
    RequiredAnswers {
        required: usize,
        server_count: usize,
    },
}

impl fmt::Display for AccountingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoServers => write!(formatter, "accounting must declare at least one server"),
            Self::ServerCount { declared, recorded } => write!(
                formatter,
                "accounting declares {declared} servers but records {recorded}"
            ),
            Self::ServerIndex { position, recorded } => write!(
                formatter,
                "accounting server at position {position} records index {recorded}"
            ),
            Self::CollusionTolerance {
                tolerance,
                server_count,
            } => write!(
                formatter,
                "accounting declares collusion tolerance {tolerance} for {server_count} servers"
            ),
            Self::RequiredAnswers {
                required,
                server_count,
            } => write!(
                formatter,
                "accounting requires {required} answers from {server_count} servers"
            ),
        }
    }
}

impl std::error::Error for AccountingError {}

pub trait HardwareCounterAdapter {
    fn name(&self) -> &'static str;
    fn begin(&self) -> std::io::Result<Box<dyn HardwareCounterSample>>;
}

pub trait HardwareCounterSample {
    fn finish(self: Box<Self>) -> std::io::Result<HardwareCounterReading>;
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct HardwareCounterReading {
    pub physical_bytes: Option<u64>,
    pub cpu_energy_microjoules: Option<u64>,
    pub dram_energy_microjoules: Option<u64>,
}

/// Cross-platform extension point for an out-of-process Linux perf/RAPL collector.
/// The POC does not silently substitute estimated payload bytes for these counters.
pub struct DisabledHardwareCounters;

impl HardwareCounterAdapter for DisabledHardwareCounters {
    fn name(&self) -> &'static str {
        "disabled"
    }

    fn begin(&self) -> std::io::Result<Box<dyn HardwareCounterSample>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "hardware counters are disabled; supply a Linux perf/RAPL adapter",
        ))
    }
}

pub fn unavailable_hardware_counters() -> HardwareCounterStatus {
    HardwareCounterStatus {
        adapter: "none (HardwareCounterAdapter accepts an optional Linux perf/RAPL collector)",
        physical_bytes: "not measured; physical_or_scanned_bytes values are explicitly labelled estimates or deterministic logical accesses",
        cpu_energy: "not measured",
        dram_energy: "not measured",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(scope: ComparisonScope) -> AggregateWorkReport {
        let phase = PhaseWork::not_applicable("none", "test fixture");
        AggregateWorkReport {
            schema: SCHEMA_VERSION,
            protocol: "test",
            comparison_scope: scope,
            security: SecurityLabels {
                privacy: "test",
                server_count: 1,
                collusion_tolerance: 0,
                required_answers: 1,
                assumptions: "test",
                availability: "test",
                integrity: "test",
            },
            global_build: phase.clone(),
            per_client_setup: phase.clone(),
            online: OnlineWork {
                unit: "query",
                per_server: vec![PerServerOnlineWork {
                    server_index: 0,
                    server_time_p50_ms: Metric::measured(1.0, "test"),
                    logical_selected_bytes: Metric::deterministic(1, "test"),
                    physical_or_scanned_bytes: Metric::estimated(1, "test"),
                    scans: Metric::deterministic(1, "test"),
                }],
                aggregate_server_time_p50_ms: Metric::measured(1.0, "test"),
                max_server_time_p50_ms: Metric::measured(1.0, "test"),
                aggregate_logical_selected_bytes: Metric::deterministic(1, "test"),
                aggregate_physical_or_scanned_bytes: Metric::estimated(1, "test"),
                server_scans: Metric::deterministic(1, "test"),
                network_rounds: Metric::deterministic(1, "test"),
                useful_result_bytes: Metric::deterministic(1, "test"),
            },
            maintenance: phase,
            client: ClientWork {
                online_cpu_p50_ms: Metric::measured(1.0, "test"),
                peak_transient_ram_bytes: Metric::not_measured("test"),
                persistent_state_bytes: Metric::deterministic(0, "test"),
                upload_bytes: Metric::deterministic(1, "test"),
                download_bytes: Metric::deterministic(1, "test"),
            },
            persisted_storage: PersistedStorage {
                server_bytes_per_server: Metric::deterministic(1, "test"),
                aggregate_server_bytes: Metric::deterministic(1, "test"),
                client_bytes: Metric::deterministic(0, "test"),
            },
            amortization: AmortizationHorizon {
                global_build: "test",
                per_client_setup: "test",
                maintenance: "test",
                assumed_global_queries: None,
                assumed_queries_per_client_setup: None,
                assumed_online_events_per_maintenance: None,
                note: "test",
            },
            hardware_counters: unavailable_hardware_counters(),
        }
    }

    fn exact_scope(result: &'static str) -> ComparisonScope {
        ComparisonScope {
            workload: "same populated table",
            result,
            public_partition: "global snapshot",
            leakage: LeakageScope::ExactQueryPrivacy,
        }
    }

    #[test]
    fn direct_comparison_rejects_different_leakage() {
        let exact = report(exact_scope("one tag page"));
        let mut candidate_scope = exact_scope("one tag page");
        candidate_scope.leakage = LeakageScope::CandidateSet { candidates: 100 };
        let decoys = report(candidate_scope);

        let comparison = direct_ratio("server work", &exact, &decoys, 10.0, 1.0);

        assert!(!comparison.directly_comparable);
        assert_eq!(comparison.candidate_over_baseline, None);
        assert_eq!(comparison.blocked_by, vec![ComparisonMismatch::Leakage]);
    }

    #[test]
    fn direct_comparison_rejects_different_result_scope() {
        let page = report(exact_scope("one tag page"));
        let notification = report(exact_scope("one matching subscription bit"));

        let comparison = direct_ratio("server work", &page, &notification, 10.0, 1.0);

        assert!(!comparison.directly_comparable);
        assert_eq!(comparison.candidate_over_baseline, None);
        assert_eq!(comparison.blocked_by, vec![ComparisonMismatch::Result]);
    }

    #[test]
    fn direct_comparison_accepts_identical_scope() {
        let baseline = report(exact_scope("one tag page"));
        let candidate = report(exact_scope("one tag page"));

        let comparison = direct_ratio("server work", &baseline, &candidate, 10.0, 2.5);

        assert!(comparison.directly_comparable);
        assert_eq!(comparison.candidate_over_baseline, Some(0.25));
        assert!(comparison.blocked_by.is_empty());
    }

    #[test]
    fn direct_comparison_rejects_different_security_models() {
        let baseline = report(exact_scope("one tag page"));
        let mut candidate = report(exact_scope("one tag page"));
        candidate.security.privacy = "computational privacy";

        let comparison = direct_ratio("server work", &baseline, &candidate, 10.0, 2.5);

        assert!(!comparison.directly_comparable);
        assert_eq!(comparison.candidate_over_baseline, None);
        assert_eq!(
            comparison.blocked_by,
            vec![ComparisonMismatch::SecurityModel]
        );
    }

    #[test]
    fn direct_comparison_rejects_every_different_security_assumption() {
        for mutate in [
            |security: &mut SecurityLabels| security.assumptions = "different assumptions",
            |security: &mut SecurityLabels| security.availability = "different availability",
            |security: &mut SecurityLabels| security.integrity = "different integrity",
        ] {
            let baseline = report(exact_scope("one tag page"));
            let mut candidate = report(exact_scope("one tag page"));
            mutate(&mut candidate.security);

            let comparison = direct_ratio("server work", &baseline, &candidate, 10.0, 2.5);

            assert!(!comparison.directly_comparable);
            assert_eq!(comparison.candidate_over_baseline, None);
            assert_eq!(
                comparison.blocked_by,
                vec![ComparisonMismatch::SecurityModel]
            );
        }
    }

    #[test]
    fn report_validation_checks_server_shape() {
        let mut invalid = report(exact_scope("one tag page"));
        invalid.security.server_count = 2;

        assert_eq!(
            invalid.validate(),
            Err(AccountingError::ServerCount {
                declared: 2,
                recorded: 1,
            })
        );
    }

    #[test]
    fn report_validation_rejects_impossible_security_shapes() {
        let mut no_servers = report(exact_scope("one tag page"));
        no_servers.security.server_count = 0;
        no_servers.online.per_server.clear();
        assert_eq!(no_servers.validate(), Err(AccountingError::NoServers));

        let mut collusion = report(exact_scope("one tag page"));
        collusion.security.collusion_tolerance = 1;
        assert_eq!(
            collusion.validate(),
            Err(AccountingError::CollusionTolerance {
                tolerance: 1,
                server_count: 1,
            })
        );

        let mut no_answers = report(exact_scope("one tag page"));
        no_answers.security.required_answers = 0;
        assert_eq!(
            no_answers.validate(),
            Err(AccountingError::RequiredAnswers {
                required: 0,
                server_count: 1,
            })
        );
    }

    #[test]
    fn direct_comparison_rejects_invalid_numeric_inputs() {
        let baseline = report(exact_scope("one tag page"));
        let candidate = report(exact_scope("one tag page"));

        let comparison = direct_ratio("server work", &baseline, &candidate, 0.0, f64::NAN);

        assert!(!comparison.directly_comparable);
        assert_eq!(comparison.candidate_over_baseline, None);
        assert_eq!(
            comparison.blocked_by,
            vec![ComparisonMismatch::InvalidValues]
        );
    }

    #[test]
    fn estimates_are_distinct_from_hardware_measurements_in_json() {
        let json = serde_json::to_value(report(exact_scope("one tag page"))).unwrap();

        assert_eq!(
            json["online"]["per_server"][0]["physical_or_scanned_bytes"]["evidence"],
            "estimated"
        );
        assert_eq!(
            &json["hardware_counters"]["adapter"].as_str().unwrap()[..4],
            "none"
        );
    }
}
