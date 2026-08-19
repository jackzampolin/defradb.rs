use anyhow::{Context, Result};
use pir_poc::portable_gates::{canonical_report, MeasuredClientCpu};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let metadata_bytes = args
        .next()
        .context("usage: portable-gates MPHF_PUBLIC_METADATA_BYTES [SINGLEPASS_Q]")?
        .parse::<usize>()
        .context("MPHF_PUBLIC_METADATA_BYTES must be a positive integer")?;
    let partition_count = args
        .next()
        .map(|value| value.parse::<usize>())
        .transpose()
        .context("SINGLEPASS_Q must be a positive integer")?
        .unwrap_or(2);
    if args.next().is_some() {
        anyhow::bail!("usage: portable-gates MPHF_PUBLIC_METADATA_BYTES [SINGLEPASS_Q]");
    }

    let report = canonical_report(
        metadata_bytes,
        partition_count,
        MeasuredClientCpu::default(),
        MeasuredClientCpu::default(),
        MeasuredClientCpu::default(),
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
