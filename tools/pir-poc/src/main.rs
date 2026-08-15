use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use pir_poc::benchmark::Profile;
use pir_poc::snapshot::{records_from_json, Snapshot, SnapshotConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("demo") => print_json(&pir_poc::demo::run().await?),
        Some("bench") => {
            let profile = profile(args.get(1));
            print_json(&pir_poc::benchmark::run(profile)?)
        }
        Some("bench-opt") => {
            let profile = profile(args.get(1));
            print_json(&pir_poc::benchmark::run_optimizations(profile)?)
        }
        Some("subscription-demo") => print_json(&pir_poc::subscription::demo().await?),
        Some("bench-subscriptions") => {
            let profile = profile(args.get(1));
            print_json(&pir_poc::subscription::benchmark(profile)?)
        }
        Some("build") => build(&args[1..]),
        Some("serve") => serve(&args[1..]).await,
        Some("query") => query(&args[1..]).await,
        _ => {
            usage();
            Ok(())
        }
    }
}

fn build(args: &[String]) -> Result<()> {
    if args.len() != 5 {
        bail!("build requires INPUT OUTPUT COLLECTION KEY_FIELD VALUE_FIELD");
    }
    let input: serde_json::Value = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let root = input.get("data").unwrap_or(&input);
    let records = records_from_json(root, Some(&args[2]), &args[3], &args[4])?;
    let bucket_count = (records.len().saturating_mul(2))
        .next_power_of_two()
        .max(16);
    let snapshot = Snapshot::build_paged(
        records,
        SnapshotConfig {
            bucket_count,
            bucket_capacity: 8,
            values_per_page: 4,
            max_key_bytes: 128,
            max_value_bytes: 1024,
            source: format!("{}.{}->{}", args[2], args[3], args[4]),
            source_cutoff: "manual-export".into(),
        },
    )?;
    snapshot.save(Path::new(&args[1]))?;
    print_json(&snapshot.manifest)
}

async fn serve(args: &[String]) -> Result<()> {
    if args.len() != 2 {
        bail!("serve requires SNAPSHOT_DIR BIND_ADDRESS");
    }
    let snapshot = Arc::new(Snapshot::load(&PathBuf::from(&args[0]))?);
    pir_poc::http::serve(snapshot, &args[1]).await
}

async fn query(args: &[String]) -> Result<()> {
    if args.len() < 3 {
        bail!("query requires KEY SERVER [SERVER ...]");
    }
    let client = pir_poc::http::PirClient::connect(&args[1..]).await?;
    let values = client.private_lookup(args[0].as_bytes()).await?;
    let rendered = values
        .into_iter()
        .map(|value| String::from_utf8(value.clone()).unwrap_or_else(|_| hex::encode(value)))
        .collect::<Vec<_>>();
    print_json(&rendered)
}

fn profile(value: Option<&String>) -> Profile {
    match value.map(String::as_str) {
        Some("full") => Profile::Full,
        _ => Profile::Quick,
    }
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).context("serialize report")?
    );
    Ok(())
}

fn usage() {
    eprintln!(
        "pir-poc commands:\n  demo\n  subscription-demo\n  bench [quick|full]\n  bench-opt [quick|full]\n  bench-subscriptions [quick|full]\n  build INPUT OUTPUT COLLECTION KEY_FIELD VALUE_FIELD\n  serve SNAPSHOT_DIR BIND_ADDRESS\n  query KEY SERVER [SERVER ...]"
    );
}
