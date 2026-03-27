//! Benchmark statistics and summary reporting.

use std::time::Duration;

use crate::benchmark_queries::format_duration;

#[derive(Debug, Clone)]
pub struct BenchmarkSummary {
    pub case_name: String,
    pub sample_count: usize,
    pub hit_count: usize,
    pub average: Duration,
    pub minimum: Duration,
    pub maximum: Duration,
    pub p50: Duration,
    pub p95: Duration,
}

impl BenchmarkSummary {
    pub fn render(&self) -> String {
        format!(
            "{}: samples={} hits={} avg={} p50={} p95={} min={} max={}",
            self.case_name,
            self.sample_count,
            self.hit_count,
            format_duration(self.average),
            format_duration(self.p50),
            format_duration(self.p95),
            format_duration(self.minimum),
            format_duration(self.maximum),
        )
    }
}

pub(crate) fn summarize(
    case_name: String,
    hit_count: usize,
    mut samples: Vec<Duration>,
) -> BenchmarkSummary {
    samples.sort_unstable();

    let total = samples
        .iter()
        .copied()
        .fold(Duration::ZERO, |acc, value| acc + value);
    let average = total / (samples.len() as u32);
    let minimum = *samples.first().unwrap_or(&Duration::ZERO);
    let maximum = *samples.last().unwrap_or(&Duration::ZERO);
    let p50 = percentile(&samples, 0.50);
    let p95 = percentile(&samples, 0.95);

    BenchmarkSummary {
        case_name,
        sample_count: samples.len(),
        hit_count,
        average,
        minimum,
        maximum,
        p50,
        p95,
    }
}

fn percentile(samples: &[Duration], percentile: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }

    let last_index = samples.len() - 1;
    let index = ((last_index as f64) * percentile).round() as usize;
    samples[index.min(last_index)]
}
